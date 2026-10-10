//! OTLP-HOST：最小真实SDK／官方gRPC运输宿主，不依赖被测运行时或采样逻辑。
//!
//! 显式创建一个Tokio运输worker和一个官方PeriodicReader线程，只服务本夹具。
//! 接收端仅保存官方已解码请求并确认接收，不构造指标、不调用Gauge.record。
//! 资源／截止合同见docs/RUNTIME_METRICS_OTLP_ACCEPTANCE_FIXTURE_DESIGN.md第五节。

use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    metrics_service_server::{MetricsService, MetricsServiceServer},
};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use std::{net::TcpListener, sync::mpsc::{self, Receiver, SyncSender, TryRecvError}, time::Duration};
use tokio::{runtime::{Builder, Runtime}, sync::oneshot, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Server};

const IO_LIMIT: Duration = Duration::from_secs(3);
const MESSAGE_LIMIT: usize = 64 * 1024;

/// 框架在真实TCP接收并解码后调用；满队列／接收端关闭必须返回gRPC错误，不能假确认。
#[derive(Debug)]
struct CaptureService {
    requests: SyncSender<ExportMetricsServiceRequest>,
}

#[tonic::async_trait]
impl MetricsService for CaptureService {
    async fn export(&self, request: Request<ExportMetricsServiceRequest>)
        -> Result<Response<ExportMetricsServiceResponse>, Status>
    {
        self.requests.try_send(request.into_inner())
            .map_err(|_| Status::resource_exhausted("验收接收队列已满或关闭"))?;
        Ok(Response::new(ExportMetricsServiceResponse { partial_success: None }))
    }
}

/// 拥有实际宿主设施；不向场景暴露Provider、Sender、listener或运输runtime可变权限。
///
/// new／flush／shutdown均非纯、非幂等，会使用线程和网络。只许在隔离子进程调用，
/// 外部父进程60秒截止兜底SDK无界force_flush；不保证OS硬实时或SDK线程已join。
/// 全部为安全Rust，无自制unsafe或FFI；不作为生产指标功能的实现或性能证据。
pub(super) struct OtlpHost {
    runtime: Option<Runtime>,
    provider: Option<SdkMeterProvider>,
    stop: Option<oneshot::Sender<()>>,
    stopped: Receiver<Result<(), tonic::transport::Error>>,
    server: Option<JoinHandle<()>>,
    requests: Receiver<ExportMetricsServiceRequest>,
}

impl OtlpHost {
    /// 绑定127.0.0.1随机端口并直接转交原listener，然后安装一次真实全局Provider。
    ///
    /// 无入参；返回独占清理句柄。网络／SDK／线程创建错误直接使测试失败。必须早于
    /// 被测仪器初始化调用；不在多线程宿主修改环境。恒定设施空间，SDK成本另计。
    pub(super) fn new() -> Self {
        let runtime = Builder::new_multi_thread().worker_threads(1)
            .thread_name("Metrics-OTLP-Transport").enable_all().build().unwrap();
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (requests_tx, requests) = mpsc::sync_channel(8);
        let (stop, stop_rx) = oneshot::channel();
        let (done_tx, stopped) = mpsc::sync_channel(1);
        let server = {
            let _entered = runtime.enter();
            let incoming = TcpListenerStream::new(tokio::net::TcpListener::from_std(listener).unwrap());
            runtime.spawn(async move {
                // 只有一个官方服务，直接交给Server；不引入多服务Router及额外axum依赖。
                let service = MetricsServiceServer::new(CaptureService { requests: requests_tx })
                    .max_decoding_message_size(MESSAGE_LIMIT);
                let result = Server::builder().serve_with_incoming_shutdown(
                    service, incoming, async { let _ = stop_rx.await; }
                ).await;
                let _ = done_tx.try_send(result);
            })
        };
        // 先建立清理所有者，再构建SDK，避免SDK构建panic后丢失已启动的服务端。
        let mut host = Self { runtime: Some(runtime), provider: None, stop: Some(stop),
            stopped, server: Some(server), requests };
        let provider = {
            let _entered = host.runtime.as_ref().unwrap().enter();
            let exporter = MetricExporter::builder().with_tonic().with_endpoint(endpoint)
                .with_timeout(IO_LIMIT).with_temporality(Temporality::Cumulative).build().unwrap();
            let reader = PeriodicReader::builder(exporter)
                .with_interval(Duration::from_secs(3600)).build();
            SdkMeterProvider::builder().with_reader(reader).build()
        };
        opentelemetry::global::set_meter_provider(provider.clone());
        host.provider = Some(provider);
        host
    }

    /// 场景必须先完成真实采集，随后宿主刷新并取回恰好一份实际解码请求。
    ///
    /// 返回拥有的官方报文，不克隆／重写其内容。SDK自身force_flush没有完整硬截止，
    /// 由父进程限制整例；返回成功不是对生产采集函数增加网络确认语义。
    pub(super) fn flush(&self) -> ExportMetricsServiceRequest {
        self.provider.as_ref().expect("SDK只在关闭前刷新").force_flush().unwrap();
        let request = self.receive_request();
        self.assert_no_extra_request();
        request
    }

    /// 消费宿主，先执行官方SDK最终导出，再停服务并关闭运输runtime，返回末次报文。
    ///
    /// 调用前须停止生产采集并注销全部实例。失败不吞错；异常Drop只尽力清理，不能
    /// 使失败测试转绿。关闭不等于库自动采样，不改报文中最后一次Gauge记录。
    pub(super) fn shutdown(mut self) -> ExportMetricsServiceRequest {
        self.provider.take().expect("SDK只关闭一次").shutdown().unwrap();
        let request = self.receive_request();
        self.stop_transport().unwrap();
        self.assert_no_extra_request();
        request
    }

    fn receive_request(&self) -> ExportMetricsServiceRequest {
        self.requests.recv_timeout(IO_LIMIT).expect("官方导出必须到达真实接收端")
    }

    fn assert_no_extra_request(&self) {
        match self.requests.try_recv() {
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => (),
            Ok(_) => panic!("一次宿主刷新／关闭不应出现额外的OTLP请求"),
        }
    }

    /// 仅回收宿主设施，不读取或清理生产队列；完成通道只观测原服务Future退出。
    /// 即使服务停止失败仍终止所持任务并消费Runtime，调用方决定失败是否传播。
    fn stop_transport(&mut self) -> Result<(), String> {
        if self.runtime.is_none() { return Ok(()); }
        if let Some(stop) = self.stop.take() { let _ = stop.send(()); }
        let result = self.stopped.recv_timeout(IO_LIMIT)
            .map_err(|error| format!("验收服务端未按时结束：{}", error))
            .and_then(|result| result.map_err(|error| format!("验收服务端失败：{}", error)));
        if let Some(server) = self.server.take() {
            if result.is_err() { server.abort(); }
        }
        if let Some(runtime) = self.runtime.take() { runtime.shutdown_timeout(Duration::from_secs(2)); }
        result
    }
}

impl Drop for OtlpHost {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take() {
            if let Err(error) = provider.shutdown() { eprintln!("异常清理SDK失败：{}", error); }
        }
        if let Err(error) = self.stop_transport() { eprintln!("{}", error); }
    }
}
