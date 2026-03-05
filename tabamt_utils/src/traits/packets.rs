use crate::res;

pub trait Packet: Send + Sync + 'static{
    fn process(self) -> Option<(u64 , res::Result)>
    where 
        Self: Sized
    {
        None
    }
}