use anyhow::Result;
use clap::Parser;
use command_handling::CommandHandler;
use server::{ServerInfo, Session};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

mod command_handling;
mod commands;
mod protocol_parser;
mod server;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long)]
    port: Option<i16>,
    #[arg(short, long)]
    replicaof: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Logs from your program will appear here!");

    let cli = Cli::parse();

    let port: i16 = cli.port.unwrap_or(6379);
    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&addr).await?;

    let server_info = match cli.replicaof {
        Some(master_addr) => {
            let master_addr = master_addr.replace(' ', ":");
            master_handshake(&master_addr).await;
            ServerInfo::new_slave(&addr, &master_addr)
        }
        None => ServerInfo::new_master(&addr),
    };

    loop {
        let (stream, addr) = listener.accept().await?;
        let mut session = Session::new(server_info.clone(), stream);
        println!("Accepted connection from: {}", addr);

        tokio::spawn(async move {
            handle_connect(&mut session).await;
        });
    }
}

async fn master_handshake(master_addr: &str) {
    let mut conn = TcpStream::connect(master_addr)
        .await
        .expect("Can not connect to master server");

    conn.write_all("*1\r\n$4\r\nping\r\n".as_bytes())
        .await
        .expect("Can not return response from master server");
}

async fn handle_connect(session: &mut Session) {
    handle_request(session)
        .await
        .expect("Failed to connect to server");
}

async fn handle_request(session: &mut Session) -> Result<(), String> {
    let mut read_buff = [0; 1024];
    let mut command_handler = CommandHandler;

    loop {
        let bytes_read = session
            .stream
            .read(&mut read_buff)
            .await
            .expect("Failed to read from stream");

        if bytes_read == 0 {
            break;
        }

        if let Ok(request) = String::from_utf8(read_buff[..bytes_read].to_vec()) {
            let command = protocol_parser::parse_protocol(&request);

            match command {
                Ok(cmd) => command_handler.handle(session, cmd).await,
                Err(_) => println!("Failed to parse command"),
            }
        } else {
            return Err(String::from("Received non-UTF8 data."));
        }
    }

    Ok(())
}
