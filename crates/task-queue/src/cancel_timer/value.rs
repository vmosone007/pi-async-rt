//! 共享请求中的受限可转交原值（transferable original value）。

// 本模块（module）不依赖本地状态或原生队列（native queue）。
// 只有共享请求模块（module）使用此封装，不向本地采用者透出可写借用（mutable borrow）。
// @trace 1
pub(super) struct SendValue<Item: 'static> {
    item: Item,
}

impl<Item: 'static> SendValue<Item> {
    // 唯一安全构造入口。不能添加未受限的Default／替换／反序列化（deserialization）入口。
    pub(super) fn new(item: Item) -> Self
    where
        Item: Send,
    {
        send_value_new_inner(item)
    }

    // 消耗独立拥有的封装一次；用于未接收拒绝，或安全出队（dequeue）后的采用/清理。
    pub(super) fn into_inner(self) -> Item {
        send_value_into_inner(self)
    }
}

// 安全（safety）：字段私有且唯一构造要求Item:Send+'static；每个真实实例中的值
// 因此可以跨线程转移，即使外围来源的Item静态参数不满足Send。封装不提供共享借用，
// 不声明Sync，不为TimerShared或本地非Send原值补造跨线程权限。
unsafe impl<Item: 'static> Send for SendValue<Item> {}

include!("value_inner.rs");
