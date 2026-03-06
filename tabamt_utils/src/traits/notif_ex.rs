use crate::traits::{notif::Notif, packets::Packet};

pub trait NotifEX: Notif + Packet{}

impl Packet for Box<dyn NotifEX> {}
impl <T> NotifEX for T 
where 
    T: Notif + Packet
{}