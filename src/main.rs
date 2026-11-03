mod tcp;
pub mod server;

use std::sync::Arc;
use tokio::net::TcpListener;
use log;
use crate::server::{Server, ServerStatus};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let server_list = vec![Arc::new(Server::new("127.0.0.1:3000")), Arc::new(Server::new("127.0.0.1:3001"))];
    let listener = TcpListener::bind("127.0.0.1:2222").await?;
    log::info!("Server listening on 127.0.0.1:2222");

    let mut handled_port: usize = 0;

    for server in &server_list {
        let server = Arc::clone(server);
        tokio::spawn(async move {
            match tcp::send_regular_heartbeat(server).await {
                Ok(()) => {
                    log::info!("Heartbeat ended");
                }
                Err(e) => {
                    log::error!("Heartbeat failed");
                    log::error!("{}", e.to_string());
                }
            }
        });
    }

    loop {
        let (stream, addr) = listener.accept().await?;

        log::info!("Got connection from {}", addr);
        let mut server_found = false;
        while !server_found {
            let server_status = server_list[handled_port].status.lock().await;
            if *server_status == ServerStatus::Available {
                server_found = true;
            }
            else {
                // Keep round-robining for a port
                handled_port = (handled_port + 1) % server_list.len();
            }
        }
        match tcp::handle_client(stream, &*server_list[handled_port].addr).await {
            Err(e) => {
                log::error!("Client error: {}", e);
            }
            _ => {}
        }
        handled_port = (handled_port + 1) % server_list.len();
    }
}
