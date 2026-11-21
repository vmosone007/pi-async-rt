fn send_value_new_inner<Item: Send + 'static>(item: Item) -> SendValue<Item> {
    SendValue { item }
}

fn send_value_into_inner<Item: 'static>(value: SendValue<Item>) -> Item {
    value.item
}
