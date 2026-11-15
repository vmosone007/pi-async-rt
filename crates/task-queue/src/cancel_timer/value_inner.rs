fn send_value_new_inner<Item: Send + 'static>(item: Item) -> SendValue<Item> {
    todo!(r#"目标：将受Send约束的同一item按值装入私有字段。
边界：不得分配第二份载荷、克隆、借用外部存储或增加不受Send约束的构造入口。
判据：一次移动、同一原值；此函数不发布请求、不增加来源数量、不调用用户代码。"#)
}

fn send_value_into_inner<Item: 'static>(value: SendValue<Item>) -> Item {
    todo!(r#"目标：消耗封装并移出同一原值一次。
前提：调用者独立拥有此封装；可来自接收前拒绝、安全出队后的正常采用或受守卫保护的退出清理。
本函数不要求节点已经入队/出队，也不要求本地登记空间；正常采用的空间准备由其调用者另行保证。
边界：不克隆、不析构返回值、不访问来源或队列；不要求Item:Send/Sync/Debug。
判据：调用后旧封装不再拥有原值，错误分支不得丢失或重复析构原值。"#)
}
