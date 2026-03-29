#[macro_export]
macro_rules! impl_as_any {
    ($t:ty) => {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    };
}