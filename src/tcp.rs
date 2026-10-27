// Importing Rust libs
use tokio::net::TcpStream;
use log;

pub async fn handle_client(mut stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    // Declare buffer to read data from client
    let mut buffer = [0; 1024];
    // Read data from stream into buffer and throw fail if not working
    stream.try_read(&mut buffer).expect("Failed to read from client");
    log::info!("Received request");

    // Buffer -> UTF-8 encoded string.
    let request = String::from_utf8_lossy(&buffer[..]);

    println!("Received request: {request}");

    let params = [
        ("arg1", "2"),
        ("arg2", "3")
    ];

    let url = reqwest::Url::parse_with_params("http://localhost:3000/add?", &params).expect("Failed to parse params");
    println!("{}", &url.to_string());

    let body = reqwest::get(url)
        .await?
        .text()
        .await?;
    
    let response = body.as_bytes();
    stream.try_write(response).expect("Failed to write response to client");
    Ok(())
}