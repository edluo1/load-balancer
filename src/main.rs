mod tcp;
use tokio::net::TcpListener;
use log;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let port_list = vec!["127.0.0.1:3000", "127.0.0.1:3001"];
    let listener = TcpListener::bind("127.0.0.1:2222").await?;
    log::info!("Server listening on 127.0.0.1:2222");

    let mut handled_port: usize = 0;

    for addr in &port_list {
        // Copy addresses
        let addr = *addr;

        tokio::spawn(async move {
            match tcp::send_regular_heartbeat(addr).await {
                Ok(()) => {
                    println!("Heartbeat ended");
                }
                Err(e) => {
                    eprintln!("Heartbeat failed for {addr}: {e:#?}");

                    eprintln!("{}", e.to_string());
                }
            }
        });
    }

    loop {
        let (stream, addr) = listener.accept().await?;

        println!("Got connection from {}", addr);

        tcp::handle_client(stream, port_list[handled_port]).await;
        handled_port += 1;
        if handled_port >= port_list.len() {
            handled_port = 0;
        }
    }
}
