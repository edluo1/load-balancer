mod tcp;
use tokio::net::TcpListener;
use tokio::io::AsyncWriteExt;
use log;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let _port_list = vec!["127.0.0.1:3000"];
    let listener = TcpListener::bind("127.0.0.1:2222").await?;
    log::info!("Server listening on 127.0.0.1:2222");

    loop {
        let (stream, addr) = listener.accept().await?;

        println!("Got connection from {}", addr);

        tcp::handle_client(stream).await;
    }
}
