mod cli;
mod client_handler;
mod cmds;
mod connection;
mod db;
mod global;
mod parse;
mod resp;
mod token;

pub use cli::Cli;
use client_handler::handle_client;
pub use db::{load_from_rdb, ExpiringHashMap};
pub use global::CONFIG_LIST;

use rand::{distributions::Alphanumeric, Rng};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

pub async fn start_server(
    bind_address: Option<String>,
    listening_port: Option<u16>,
    dir_name: Option<String>,
    dbfilename: Option<String>,
    replicaof: Option<String>,
) {
    // Start logging.
    femme::start();
    if bind_address.is_some() {
        CONFIG_LIST.push(("bind_address".to_string(), bind_address.clone().unwrap()));
    }

    if listening_port.is_some() {
        CONFIG_LIST.push((
            "listening_port".to_string(),
            listening_port.unwrap().to_string(),
        ));
    }

    if dir_name.is_some() {
        CONFIG_LIST.push(("dir".to_string(), dir_name.clone().unwrap()));
    }

    if dbfilename.is_some() {
        CONFIG_LIST.push(("dbfilename".to_string(), dbfilename.clone().unwrap()));
    }

    if replicaof.is_some() {
        CONFIG_LIST.push(("replicaof".to_string(), replicaof.clone().unwrap()));
    }

    if bind_address.is_some() && listening_port.is_some() {
        // initialise the DB
        //let db: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
        log::info!("initialising database files...");
        let db: ExpiringHashMap<String, String> = ExpiringHashMap::new();

        if CONFIG_LIST.get_val(&"dir_name".to_string()).is_some()
            && CONFIG_LIST.get_val(&"dbfilename".to_string()).is_some()
        {
            load_from_rdb(db.clone())
                .await
                .expect("RDB file read failed");
        }

        // Create TCP Listener
        let bind_address = CONFIG_LIST.get_val(&"bind_address".to_string()).unwrap();
        let listening_port = CONFIG_LIST.get_val(&"listening_port".to_string()).unwrap();
        let listener_addr = format!("{}:{}", bind_address, listening_port);
        let listener = TcpListener::bind(listener_addr.to_owned())
            .await
            .expect("Binding to listener address failed!");
        log::info!("Redis running on {}...", listener_addr);

        // Handle Multiple Clients in a loop
        loop {
            if replicaof.is_some() {
                dbg!(replicaof.clone());
                tokio::spawn(handle_follower());
            } else {
                dbg!(replicaof.clone());
                let master_replid: String = rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(40) // 40 character long
                    .map(char::from) // `u8` values to `char`
                    .collect();

                CONFIG_LIST.push(("master_replid".into(), master_replid));

                let master_repl_offset: u64 = 0;
                CONFIG_LIST.push(("master_repl_offset".into(), master_repl_offset.to_string()));
            }
            let (tcp_stream, socket_addr) = listener
                .accept()
                .await
                .expect("Accepting connection failed");
            log::info!("Accepted connection from {}", socket_addr.ip().to_string());
            let db = db.clone();

            tokio::spawn(handle_client(tcp_stream, socket_addr, db));
        }
    } else {
        panic!("Bind address and port cannot be empty!");
    }
}

async fn handle_follower() {
    let replicaof = CONFIG_LIST.get_val(&"replicaof".to_string());
    let leader_addr = if let Some(val) = replicaof {
        let ip_and_port: Vec<&str> = val.split_whitespace().collect();
        if ip_and_port.len() > 2 {
            panic!("Wrong number of arguments in leader connection string");
        }
        format!("{}:{}", ip_and_port[0], ip_and_port[1])
    } else {
        panic!("Leader address is not valid");
    };

    tokio::spawn(async move {
        let mut stream = tokio::net::TcpStream::connect(leader_addr).await.unwrap();
        // Hashshake
        let message = b"*1\r\n$4\r\nPING\r\n";
        stream.write_all(message).await;

        let mut buffer = [0; 512];
        let n = stream.read(&mut buffer).await.unwrap();
        println!("{}", String::from_utf8_lossy(&buffer[..n]).to_string())
    });
}
