// 当前文件落实配对声明中的局部操作。

#[allow(unused_variables)]
fn batch_from_inner(tasks: VecDeque<Runnable>) -> TaskBatch {
    TaskBatch { tasks }
}

#[allow(unused_variables)]
fn batch_len_inner(this: &TaskBatch) -> usize {
    this.tasks.len()
}

#[allow(unused_variables)]
fn batch_empty_inner(this: &TaskBatch) -> bool {
    this.tasks.is_empty()
}

#[allow(unused_variables)]
fn batch_into_iter_inner(this: TaskBatch) -> std::collections::vec_deque::IntoIter<Runnable> {
    this.tasks.into_iter()
}
