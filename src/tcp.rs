use std::io::{Error, ErrorKind};
// Importing Rust libs
use tokio::net::TcpStream;
use tokio::io::AsyncReadExt;
use log;
use std::str::FromStr;
use std::sync::Arc;
use chrono::Utc;
use cron::Schedule;
use reqwest::Client;
use tokio::sync::MutexGuard;
use crate::server::{Server, ServerStatus};

pub async fn handle_client(mut stream: TcpStream, destination: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Declare buffer to read data from client
    let mut buffer = [0; 1024];
    // Read data from stream into buffer and throw fail if not working
    stream.read(&mut buffer).await?;

    // Buffer -> UTF-8 encoded string.
    let request = String::from_utf8_lossy(&buffer[..]);

    let request_start = match request.split('\n').nth(0) {
        Some(v) => {v}
        None => {""}
    };

    if request_start.is_empty() {
        return Err(Box::from(Error::new(ErrorKind::Other, "Invalid HTTP request made")));
    }

    let params = parse_request_line(request_start)?.1;

    let endpoint = parse_endpoint(&params);
    log::info!("endpoint: {:?}", endpoint);
    if endpoint != "/add" {
        return Err(Box::from(Error::new(ErrorKind::Other, "Invalid endpoint provided")));
    }

    let url = format!("http://{destination}{params}");

    let body = reqwest::get(url)
        .await?
        .text()
        .await?;
    
    let response = body.as_bytes();
    stream.try_write(response).expect("Failed to write response to client");
    Ok(())
}

fn parse_endpoint(path: &str) -> String {
    if let Some(endpoint) = path.split('?').nth(0) {
        endpoint.to_string()
    }
    else {
        path.to_string()
    }
}

// Parses a request string like GET /add?arg1=2&arg2=99 HTTP/1.1 into constituent parts
fn parse_request_line(line: &str) -> Result<(String, String, String), String> {
    let parts: Vec<&str> = line.trim().split_whitespace().collect();
    if parts.len() < 2 {
        return Err("Invalid request line".to_string());
    }
    Ok((parts[0].to_string(), parts[1].to_string(), parts.get(2).unwrap_or(&"").to_string()))
}

// Send heartbeats
pub async fn send_regular_heartbeat(server: Arc<Server>) -> Result<(), reqwest::Error> {
    let expression = "0/1 * * * * *";
    let schedule = Schedule::from_str(expression).expect("Failed to parse CRON expression");
    let client = reqwest::Client::new();
    let destination = &server.addr;

    for datetime in schedule.upcoming(Utc) {
        let now = Utc::now();
        let until = datetime - now;
        // Skip if the "datetime"'s been passed.
        if until < chrono::Duration::zero() {
            continue;
        }
        tokio::time::sleep(until.to_std().unwrap()).await;

        let heartbeat_url = format!("http://{destination}/heartbeat");

        log::info!("Sending heartbeat to {}", &destination);
        // Send heartbeats
        let status = Arc::clone(&server.status);
        let mut stat = status.lock().await;
        *stat = determine_response(&client, heartbeat_url).await;
    }
    Ok(())
}

async fn determine_response(client: &Client, heartbeat_url: String) -> ServerStatus {
    match client.get(heartbeat_url)
        .send()
        .await {
        Ok(res) => {
            match res.text().await {
                Ok(body) => {
                    if body.is_empty() {
                        log::error!("No Response");
                        ServerStatus::HeartbeatError
                    } else {
                        log::info!("Service available!");
                        ServerStatus::Available
                    }
                }
                Err(_) => {
                    log::error!("Response Error");
                    ServerStatus::HeartbeatError
                }
            }
        }
        Err(e) => {
            log::error!("{}", e.to_string());
            if e.is_connect() {
                log::error!("unavailable");
                ServerStatus::Unavailable
            } else {
                log::error!("other error");
                ServerStatus::HeartbeatError
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_request_line() {
        let result = parse_request_line("GET /add?arg1=2&arg2=99 HTTP/1.1").unwrap();
        assert_eq!(result, ("GET".to_string(), "/add?arg1=2&arg2=99".to_string(), "HTTP/1.1".to_string()),
                   "Not splitting properly");

        let result = parse_request_line("GET /add?arg1=9.3&arg2=1.22 HTTP/1.1").unwrap();
        assert_eq!(result, ("GET".to_string(), "/add?arg1=9.3&arg2=1.22".to_string(), "HTTP/1.1".to_string()),
                   "Not splitting properly with decimals");

        // Failed parsing
        let result = parse_request_line("fake_request_text")
            .unwrap_or_else(|_| ("".to_string(), "".to_string(), "".to_string()));
        assert_eq!(result, ("".to_string(), "".to_string(), "".to_string()),
                   "Not properly returning error");
    }

    #[test]
    fn test_parse_endpoint() {
        let result = parse_endpoint("/add?arg1=2&arg2=99");
        assert_eq!(result, "/add".to_string(), "Not returning the right endpoint");

        let result = parse_endpoint("/ADD?ARG1=2&ARG2=99");
        assert_eq!(result, "/ADD".to_string(), "Not case-sensitive");

        // Failed parsing
        let result = parse_endpoint("Cheesecake");
        assert_eq!(result, "Cheesecake".to_string(), "Not handling weird input");
    }
}

