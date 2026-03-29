use std::sync::Arc;

use crate::bases::observer::Observer;

pub trait Queue<T>: Send + Sync + 'static {
    fn push(&self, value: T);
    fn pop(&self) -> T;
    fn finalize(&self , logger: Option<Arc<Observer>>);
    fn pause(&self);
    fn resume(&self);
}