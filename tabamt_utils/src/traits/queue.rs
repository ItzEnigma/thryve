pub trait Queue<T>: Send + Sync + 'static {
    fn push(&self, value: T);
    fn pop(&self) -> T;
    fn finalize(&self);
    fn pause(&self);
    fn resume(&self);
}