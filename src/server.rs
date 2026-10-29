use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum ServerStatus {
    Available,
    HeartbeatError,
    Unavailable,
}

pub struct Server {
    pub status: Arc<Mutex<ServerStatus>>,
    pub addr: String
}

impl Server {
    pub fn new(addr: &str) -> Server {
        Server {
            status: Arc::new(Mutex::new(ServerStatus::Unavailable)),
            addr: addr.to_owned()
        }
    }
}