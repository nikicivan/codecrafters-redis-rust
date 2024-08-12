use anyhow::{Context, Result};
use resp::{RespHandler, Value};
use std::env::args;
use storage::Storage;
use tokio::net::{TcpListener, TcpStream};

mod resp;
pub mod storage;

#[tokio::main]
async fn main() -> Result<()> {
    let port = args().nth(2).unwrap_or("6379".to_string());
    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .context("Failed to bind TCP listeners to address")?;

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok((stream, _)) => {
                println!("accepted new connection");

                tokio::spawn(async move { handle_conn(stream).await });
            }
            Err(e) => {
                println!("error {}", e)
            }
        };
    }
}

// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n

async fn handle_conn(stream: TcpStream) {
    let mut handler = RespHandler::new(stream);
    let mut storage = Storage::new();

    println!("Starting read loop");

    loop {
        let value = handler.read_value().await.unwrap();

        println!("Got value: {:?}", value);

        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            match command.as_str() {
                "PING" => Value::SimpleString("PONG".to_string()),
                "ECHO" => args.first().unwrap().clone(),
                "SET" => {
                    let key = unpack_bulk_str(args[0].clone()).unwrap();
                    let value = unpack_bulk_str(args[1].clone()).unwrap();
                    if args.len() > 3 {
                        let subcommand = unpack_bulk_str(args[2].clone()).unwrap();
                        match subcommand.as_str() {
                            "px" => {
                                let expires =
                                    unpack_bulk_str(args[3].clone()).unwrap().parse().unwrap();
                                storage.set(&key, &value, expires);
                            }
                            _ => panic!("Cannot handle subcommand {}", subcommand),
                        }
                    } else {
                        storage.set(&key, &value, 0);
                    }
                    Value::SimpleString("OK".to_string())
                }
                "GET" => {
                    let key = unpack_bulk_str(args.first().unwrap().clone()).unwrap();
                    match storage.get(&key) {
                        Some(item) => Value::BulkString(item.value.clone()),
                        None => Value::Null,
                    }
                }
                "INFO" => Value::BulkString("role:replicaof".to_string()),
                _ => panic!("Cannot handle command {}", command),
            }
        } else {
            break;
        };

        println!("Sending value {:?}", response);

        handler.write_value(response).await.unwrap();
    }
}

fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
    match value {
        Value::Array(a) => Ok((
            unpack_bulk_str(a.first().unwrap().clone())?,
            a.into_iter().skip(1).collect(),
        )),
        _ => Err(anyhow::anyhow!("Unexpected command format")),
    }
}

fn unpack_bulk_str(value: Value) -> Result<String> {
    match value {
        Value::BulkString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string")),
    }
}

// fn set(storage: &mut HashMap<String, String>, key: String, value: String) -> Value {
//     storage.insert(key, value);
//     Value::SimpleString("OK".to_string())
// }

// fn get(storage: &HashMap<String, String>, key: String) -> Value {
//     match storage.get(&key) {
//         Some(v) => Value::BulkString(v.to_string()),
//         None => Value::Null,
//     }
// }
