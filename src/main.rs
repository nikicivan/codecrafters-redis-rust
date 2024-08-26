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
mod helpers;
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

    let server_info = if let Some(master_addr) = cli.replicaof {
        let master_addr = master_addr.replace(' ', ":");
        master_handshake(&master_addr, port).await;
        ServerInfo::new_slave(&addr, &master_addr)
    } else {
        ServerInfo::new_master(&addr)
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

async fn master_handshake(master_addr: &str, slave_port: i16) {
    let mut stream = TcpStream::connect(master_addr)
        .await
        .expect("Can not connect to master server");

    // SEND the PING command to master
    send_ping_command(&mut stream).await;

    // Send the first REPLCONF command to master
    send_replconf_command(vec!["listening-port", &slave_port.to_string()], &mut stream).await;

    // Send the second REPLCONF command to master.
    send_replconf_command(vec!["capa", "psync2"], &mut stream).await;

    // stream
    //     .write_all("*1\r\n$4\r\nping\r\n".as_bytes())
    //     .await
    //     .expect("Can not return response from master server");

    send_psync_command(vec!["PSYNC", "?", "-1"], &mut stream).await;
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

async fn send_ping_command(stream: &mut TcpStream) {
    let ping = helpers::RespHandler::to_resp_array(vec!["PING"]);
    stream
        .write_all(ping.as_bytes())
        .await
        .expect("Couldn't send ping");

    stream.flush().await.expect("Couldn't flush response");

    let read_buff = &mut [0; 128];
    let bytes_read = stream
        .read(read_buff)
        .await
        .expect("couldn't read response");

    if bytes_read == 0 {
        panic!("Received non-UTF8 data.")
    }

    match String::from_utf8(read_buff[..bytes_read].to_vec()) {
        Ok(r) => {
            if r != "+PONG\r\n" {
                panic!("Unexpected PING response from master: {}", r);
            }
        }
        Err(_) => panic!("Received non-UTF8 data."),
    }
}

async fn send_replconf_command(mut values: Vec<&str>, stream: &mut TcpStream) {
    values.insert(0, "REPLCONF");
    let replconf = helpers::RespHandler::to_resp_array(values);
    stream
        .write_all(replconf.as_bytes())
        .await
        .expect("Failed to send replconf response");
    stream.flush().await.expect("Couldn't flush response");

    let read_buff = &mut [0; 128];
    let bytes_read = stream
        .read(read_buff)
        .await
        .expect("couldn't read response");

    if bytes_read == 0 {
        panic!("Received non-UTF8 data.")
    }

    match String::from_utf8(read_buff[..bytes_read].to_vec()) {
        Ok(r) => {
            if r != "+OK\r\n" {
                panic!("Unexpected REPLCONF response from master: {}", r);
            }
        }
        Err(_) => panic!("Received non-UTF8 data."),
    }
}

async fn send_psync_command(values: Vec<&str>, stream: &mut TcpStream) {
    let replconf = helpers::RespHandler::to_resp_array(values);
    println!("{:?}", replconf);
    stream
        .write_all(replconf.as_bytes())
        .await
        .expect("Failed to send replconf response");
    stream.flush().await.expect("Couldn't flush response");

    // let read_buff = &mut [0; 128];
    // let bytes_read = stream
    //     .read(read_buff)
    //     .await
    //     .expect("couldn't read response");

    // if bytes_read == 0 {
    //     panic!("Received non-UTF8 data.")
    // }

    // match String::from_utf8(read_buff[..bytes_read].to_vec()) {
    //     Ok(r) => {
    //         if r != "+OK\r\n" {
    //             panic!("Unexpected REPLCONF response from master: {}", r);
    //         }
    //     }
    //     Err(_) => panic!("Received non-UTF8 data."),
    // }
}
