// Importing Rust libs
use tokio::net::TcpStream;
use tokio::io::AsyncReadExt;
use log;
use std::str::FromStr;
use std::thread;
use chrono::Utc;
use cron::Schedule;

pub async fn handle_client(mut stream: TcpStream, destination: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Declare buffer to read data from client
    let mut buffer = [0; 1024];
    // Read data from stream into buffer and throw fail if not working
    stream.read(&mut buffer).await?;
    log::info!("Received request");

    // Buffer -> UTF-8 encoded string.
    let request = String::from_utf8_lossy(&buffer[..]);

    println!("Received request: {request}");

    let params = [
        ("arg1", "2"),
        ("arg2", "3")
    ];

    let url = reqwest::Url::parse_with_params(format!("http://{destination}/add?").as_str(), &params).expect("Failed to parse params");
    println!("{}", &url.to_string());

    let body = reqwest::get(url)
        .await?
        .text()
        .await?;
    
    let response = body.as_bytes();
    stream.try_write(response).expect("Failed to write response to client");
    Ok(())
}

// Send heartbeats
pub async fn send_regular_heartbeat(destination: &str) -> Result<(), reqwest::Error> {
    let expression = "0/10 * * * * *";
    let schedule = Schedule::from_str(expression).expect("Failed to parse CRON expression");
    let client = reqwest::Client::new();

    for datetime in schedule.upcoming(Utc) {
        let now = Utc::now();
        let until = datetime - now;
        // Skip if the "datetime"'s been passed.
        if until < chrono::Duration::zero() {
            continue;
        }
        println!("now = {}", &now);
        println!("until = {}", &until);
        tokio::time::sleep(until.to_std().unwrap()).await;

        let heartbeat_url = format!("http://{destination}/heartbeat");

        println!("Sending heartbeat to {}", &destination);
        // Send heartbeats
        match client.get(&heartbeat_url)
            .send()
            .await {
                Ok(res) => {
                    println!("Status: {}", res.status());
                    println!("Headers:\n{:#?}", res.headers());

                    let body = res.text().await?;
                    println!("Body:\n{}", body);
                } 
                Err(e) => {
                    eprintln!("Failed");
                }
            }
    }

    Ok(())
}