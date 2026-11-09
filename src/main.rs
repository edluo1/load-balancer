mod tcp;
pub mod server;

use std::sync::Arc;
use aws_config::BehaviorVersion;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_servicediscovery::Client;
use aws_sdk_servicediscovery::types::{HealthStatusFilter};
use tokio::net::TcpListener;
use log;
use crate::server::{Server, ServerStatus};

#[tokio::main]
async fn main() -> std::io::Result<()> {

    let region_provider = RegionProviderChain::default_provider().or_else("us-east-2");
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    let client = Client::new(&config);

    // Find instances from client
    let server_instances = client.discover_instances()
        .namespace_name("add-two-numbers.com")
        .service_name("add-two-numbers-service")
        .health_status(HealthStatusFilter::Healthy)
        .send()
        .await;

    let server_list = server_instances.unwrap().instances()
        .iter().map(|instance| {
        let attributes = instance.attributes().unwrap();
        let ip   = attributes.get("AWS_INSTANCE_IPV4").map(|s| s.as_str()).unwrap_or("?");
        let port = attributes.get("AWS_INSTANCE_PORT").map(|s| s.as_str()).unwrap_or("3000");
        Arc::new(Server::new(format!("https://{}:{}", ip, port).as_str()))
        }).collect::<Vec<_>>();

    let listener = TcpListener::bind("0.0.0.0:2222").await?;
    log::info!("Connected to {} servers", server_list.len());
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
