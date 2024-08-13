use bytes::{Buf, BytesMut};
use core::str;
use std::{collections::HashMap, sync::Arc, sync::Mutex, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::Instant,
};
#[derive(Debug)]
struct ServerSettings {
    pub port: String,
    pub role: Role,
    pub replica_of_host: Option<String>,
    pub replica_of_port: Option<String>,
}
impl ServerSettings {
    fn new() -> ServerSettings {
        ServerSettings {
            role: Role::Master,
            port: String::from("6379"),
            replica_of_host: None,
            replica_of_port: None,
        }
    }
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let mut settings = ServerSettings::new();
    let mut iter = std::env::args();
    while let Some(argument) = iter.next() {
        match argument.to_ascii_lowercase().as_str() {
            "--port" => settings.port = iter.next().expect("Expected --port <port>"),
            "--replicaof" => {
                let replicaof = iter
                    .next()
                    .expect("Expected --replicaof \"<master_host> <master_port>\"");
                let mut replicaof_tokens = replicaof.split(' ');
                let master_host = replicaof_tokens
                    .next()
                    .expect("Expected provided <master-host>");
                let master_port = replicaof_tokens
                    .next()
                    .expect("Expected provided <master-port>");
                settings.replica_of_host = Some(String::from(master_host));
                settings.replica_of_port = Some(String::from(master_port));
                settings.role = Role::Slave
            }
            _ => (),
        }
    }
    println!("Starting server with settings: {:?}", settings);
    let listener = TcpListener::bind(format!("127.0.0.1:{0}", settings.port)).await?;
    let server: Server = Arc::new(Mutex::new(State::with_settings(settings)));
    loop {
        let (stream, addr) = listener.accept().await?;
        println!("Accepted connection from: {}", addr);
        let new_server_handler = server.clone();
        tokio::spawn(async move {
            handle_connection(new_server_handler, stream).await;
        });
    }
}
#[derive(Debug)]
enum Role {
    Master,
    Slave,
}
struct State {
    pub settings: ServerSettings,
    storage: HashMap<String, Record>,
}

impl State {
    fn new() -> State {
        State {
            settings: ServerSettings::new(),
            storage: HashMap::<String, Record>::new(),
        }
    }
    fn with_settings(settings: ServerSettings) -> State {
        State {
            settings: settings,
            storage: HashMap::<String, Record>::new(),
        }
    }
    fn set(&mut self, key: &String, value: &String) {
        println!("Setting {key} to {value}");
        let record = Record {
            data: value.clone(),
            expiration_timestamp: None,
        };
        self.storage.insert(key.clone(), record);
    }
    fn set_with_ttl(&mut self, key: &String, value: &String, ttl: u64) {
        let expiration_timestamp: Instant = Instant::now() + Duration::from_millis(ttl);
        println!("Setting {key} to {value} with expiration {expiration_timestamp:?}");
        let record = Record {
            data: value.clone(),
            expiration_timestamp: Some(expiration_timestamp),
        };
        self.storage.insert(key.clone(), record);
    }
    fn get(&self, key: &String) -> Option<String> {
        let record = self.storage.get(key);
        println!("Found record for key {key}: {record:?}");
        match record {
            Some(record) => match record.expiration_timestamp {
                Some(expiration_timestamp) => {
                    if expiration_timestamp < Instant::now() {
                        None
                    } else {
                        Some(record.data.clone())
                    }
                }
                None => Some(record.data.clone()),
            },
            None => None,
        }
    }
}
type Server = Arc<Mutex<State>>;
#[derive(Debug, Clone, PartialEq)]
struct Record {
    data: String,
    expiration_timestamp: Option<Instant>,
}

async fn handle_connection(server: Server, mut stream: TcpStream) {
    loop {
        let mut buffer = BytesMut::with_capacity(1024);
        let read_size = stream.read_buf(&mut buffer).await.expect("Failed read");
        if read_size == 0 {
            break;
        }
        println!("Request: {:?}", buffer);
        let request = parse_message(&mut buffer).expect("Expect valid RESP message");
        let command = parse_command(&request).expect("Expect valid RESP command");
        let response = execute_command(&server, &command).unwrap();
        let response = serialize(&response).unwrap();
        println!("Response: {:?}", response);
        stream
            .write(response.as_bytes())
            .await
            .expect("Failed write");
    }
}
fn parse_message(mut buffer: &mut BytesMut) -> anyhow::Result<Value> {
    let prefix = buffer[0];
    buffer.advance(1);
    match prefix as char {
        '+' => {
            let data = advance_until_next_crlf(&mut buffer);
            let data = String::from_utf8(data.to_vec()).expect("Expected UTF-8 data");
            Ok(Value::from(SimpleStringValue { data }))
        }
        '$' => {
            let length = advance_until_next_crlf(&mut buffer);
            let length = str::from_utf8(&length).expect("Expected UTF-8 data");
            let length = length.parse::<i64>().unwrap();
            let data = advance_until_next_crlf(&mut buffer);
            let data = String::from_utf8(data.to_vec()).expect("Expected UTF-8 data");
            Ok(Value::from(BulkStringValue { length, data }))
        }
        '*' => {
            let length = advance_until_next_crlf(&mut buffer);
            let length = str::from_utf8(&length).expect("Expected UTF-8 data");
            let length = length.parse::<i64>().unwrap();
            let mut elements = Vec::new();
            for _ in 0..length {
                let value = parse_message(&mut buffer).unwrap();
                elements.push(value);
            }
            Ok(Value::from(ArrayValue { length, elements }))
        }
        _ => panic!(),
    }
}
fn advance_until_next_crlf(buffer: &mut BytesMut) -> BytesMut {
    let len = bytes_until_next_crlf(buffer);
    let consumed = buffer.split_to(len);
    buffer.advance(2);
    consumed
}
fn bytes_until_next_crlf(buffer: &BytesMut) -> usize {
    if buffer.is_empty() {
        panic!()
    }
    let mut num_bytes = 0;
    while (buffer[num_bytes] != b'\r' || buffer[num_bytes + 1] != b'\n')
        && num_bytes < buffer.len() - 1
    {
        num_bytes += 1;
    }
    num_bytes
}
fn parse_command<'a>(value: &'a Value) -> anyhow::Result<Command> {
    let array =
        <&ArrayValue>::try_from(value).expect("Provided value is not an ARRAY of BULK STRINGS");
    let ArrayValue { elements, .. } = array;
    let elements: Vec<_> = elements
        .iter()
        .map(|val| {
            <&BulkStringValue>::try_from(val)
                .expect("Provided value is not an ARRAY of BULK STRINGS")
        })
        .collect();
    // TODO: Handle two-word commands
    let mut iter = elements.iter();
    let BulkStringValue { data: command, .. } = iter
        .next()
        .expect("Expected non-empty ARRAY of BULK STRINGS");
    let command = match command.to_ascii_uppercase().as_str() {
        "PING" => Command::Ping,
        "ECHO" => {
            let BulkStringValue { data, .. } =
                iter.next().expect("Expected 1 argument for ECHO command");
            Command::from(EchoCommand {
                value: data.clone(),
            })
        }
        "SET" => {
            let BulkStringValue { data: key, .. } =
                iter.next().expect("Expected 2 arguments for SET command");
            let BulkStringValue { data: value, .. } =
                iter.next().expect("Expected 2 arguments for SET command");
            let mut set_command = SetCommand {
                key: key.clone(),
                value: value.clone(),
                ttl: None,
            };
            while let Some(BulkStringValue { data: argument, .. }) = iter.next() {
                match argument.to_ascii_uppercase().as_str() {
                    "PX" => {
                        let BulkStringValue {
                            data: argument_value,
                            ..
                        } = iter.next().expect("Expected integer after PX argument");
                        set_command.ttl = Some(
                            argument_value
                                .parse::<u64>()
                                .expect("Expected interger value after PX argument"),
                        );
                    }
                    _ => unimplemented!(),
                }
            }
            Command::from(set_command)
        }
        "GET" => {
            let BulkStringValue { data: key, .. } =
                iter.next().expect("Expected 1 argument for GET command");
            Command::from(GetCommand { key: key.clone() })
        }
        "INFO" => {
            let info_section = match iter.next() {
                Some(BulkStringValue {
                    data: info_section, ..
                }) => match info_section.to_ascii_lowercase().as_str() {
                    "replication" => InfoSection::REPLICATION,
                    _ => unimplemented!(),
                },
                None => InfoSection::ALL,
            };
            Command::from(InfoCommand { info_section })
        }
        _ => unimplemented!(),
    };
    Ok(command)
}

fn execute_command(server: &Server, command: &Command) -> anyhow::Result<Value> {
    println!("Executing command {command:?}");
    match command {
        Command::Ping => {
            let result = Value::from(SimpleStringValue {
                data: "PONG".to_owned(),
            });
            return Ok(result);
        }
        Command::Echo(echo_command) => Ok(Value::from(BulkStringValue {
            length: echo_command.value.len() as i64,
            data: echo_command.value.clone(),
        })),
        Command::Set(set_command) => {
            let SetCommand { key, value, ttl } = set_command;
            let mut mut_state = server.lock().unwrap();
            if let Some(ttl) = ttl {
                mut_state.set_with_ttl(key, value, *ttl);
            } else {
                mut_state.set(key, value);
            }
            return Ok(Value::from(SimpleStringValue {
                data: "OK".to_owned(),
            }));
        }
        Command::Get(get_command) => {
            let mut_state = server.lock().unwrap();
            let GetCommand { key } = get_command;
            let value = match mut_state.get(key) {
                Some(value) => BulkStringValue {
                    length: value.len() as i64,
                    data: value.clone(),
                },
                None => BulkStringValue {
                    length: -1,
                    data: "".to_owned(),
                },
            };
            return Ok(Value::from(value));
        }
        Command::Info(info_command) => {
            let InfoCommand { info_section } = info_command;
            let info = match info_section {
                InfoSection::ALL | InfoSection::REPLICATION => {
                    let mut_state = server.lock().unwrap();
                    match mut_state.settings.role {
                        Role::Master => "role:master",
                        Role::Slave => "role:slave",
                    }
                }
            };

            Ok(Value::from(BulkStringValue {
                data: info.to_string(),
                length: info.len() as i64,
            }))
        }
    }
}

fn serialize(value: &Value) -> anyhow::Result<String> {
    match value {
        Value::SimpleString(simple_string) => {
            let SimpleStringValue { data } = simple_string;
            let mut string = "+".to_owned();
            string.push_str(data);
            string.push_str("\r\n");
            Ok(string)
        }
        Value::BulkString(bulk_string) => {
            let BulkStringValue { length, data } = bulk_string;
            let mut string = "$".to_owned();
            string.push_str(&length.to_string());
            string.push_str("\r\n");
            if *length != -1 {
                string.push_str(data);
                string.push_str("\r\n");
            }
            Ok(string)
        }
        Value::Array(array) => {
            let ArrayValue { length, elements } = array;
            let mut string = "*".to_owned();
            string.push_str(&length.to_string());
            if *length > 0 {
                string.push_str("\r\n");
            }
            for value in elements {
                string.push_str(&serialize(&value).unwrap());
            }
            Ok(string)
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
struct SimpleStringValue {
    data: String,
}
#[derive(Debug, Clone, PartialEq)]
struct BulkStringValue {
    length: i64,
    data: String,
}
#[derive(Debug, Clone, PartialEq)]
struct ArrayValue {
    length: i64,
    elements: Vec<Value>,
}
#[derive(Debug, Clone, PartialEq)]
enum Value {
    SimpleString(SimpleStringValue),
    BulkString(BulkStringValue),
    Array(ArrayValue),
}
impl From<SimpleStringValue> for Value {
    fn from(value: SimpleStringValue) -> Self {
        Value::SimpleString(value)
    }
}
impl TryFrom<Value> for SimpleStringValue {
    type Error = &'static str;
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if let Value::SimpleString(simple_string) = value {
            Ok(simple_string)
        } else {
            Err("Value is not a SIMPLE STRING")
        }
    }
}
impl<'a> TryFrom<&'a Value> for &'a SimpleStringValue {
    type Error = &'static str;
    fn try_from(value: &'a Value) -> Result<Self, Self::Error> {
        if let Value::SimpleString(simple_string) = value {
            Ok(simple_string)
        } else {
            Err("Value provided is not a SIMPLE STRING")
        }
    }
}
impl From<BulkStringValue> for Value {
    fn from(value: BulkStringValue) -> Self {
        Value::BulkString(value)
    }
}
impl TryFrom<Value> for BulkStringValue {
    type Error = &'static str;
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if let Value::BulkString(bulk_string) = value {
            Ok(bulk_string)
        } else {
            Err("Value provided is not a BULK STRING")
        }
    }
}
impl<'a> TryFrom<&'a Value> for &'a BulkStringValue {
    type Error = &'static str;
    fn try_from(value: &'a Value) -> Result<Self, Self::Error> {
        if let Value::BulkString(bulk_string) = value {
            Ok(bulk_string)
        } else {
            Err("Value provided is not a BULK STRING")
        }
    }
}
impl From<ArrayValue> for Value {
    fn from(value: ArrayValue) -> Self {
        Value::Array(value)
    }
}
impl TryFrom<Value> for ArrayValue {
    type Error = &'static str;
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if let Value::Array(array) = value {
            Ok(array)
        } else {
            Err("Value is not an ARRAY")
        }
    }
}
impl<'a> TryFrom<&'a Value> for &'a ArrayValue {
    type Error = &'static str;
    fn try_from(value: &'a Value) -> Result<Self, Self::Error> {
        if let Value::Array(array) = value {
            Ok(array)
        } else {
            Err("Value is not an ARRAY")
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
struct EchoCommand {
    value: String,
}
#[derive(Debug, Clone, PartialEq)]
struct SetCommand {
    key: String,
    value: String,
    ttl: Option<u64>,
}
#[derive(Debug, Clone, PartialEq)]
struct GetCommand {
    key: String,
}
#[derive(Debug, Clone, PartialEq)]
enum InfoSection {
    REPLICATION,
    ALL,
}
#[derive(Debug, Clone, PartialEq)]
struct InfoCommand {
    info_section: InfoSection,
}
#[derive(Debug, Clone, PartialEq)]
enum Command {
    Ping,
    Echo(EchoCommand),
    Set(SetCommand),
    Get(GetCommand),
    Info(InfoCommand),
}
impl From<EchoCommand> for Command {
    fn from(value: EchoCommand) -> Self {
        Command::Echo(value)
    }
}
impl TryFrom<Command> for EchoCommand {
    type Error = &'static str;
    fn try_from(value: Command) -> Result<Self, Self::Error> {
        if let Command::Echo(echo_command) = value {
            Ok(echo_command)
        } else {
            Err("Command is not an ECHO command")
        }
    }
}
impl<'a> TryFrom<&'a Command> for &'a EchoCommand {
    type Error = &'static str;
    fn try_from(value: &'a Command) -> Result<Self, Self::Error> {
        if let Command::Echo(echo_command) = value {
            Ok(echo_command)
        } else {
            Err("Command is not an ECHO command")
        }
    }
}
impl From<SetCommand> for Command {
    fn from(value: SetCommand) -> Self {
        Command::Set(value)
    }
}
impl TryFrom<Command> for SetCommand {
    type Error = &'static str;
    fn try_from(value: Command) -> Result<Self, Self::Error> {
        if let Command::Set(set_command) = value {
            Ok(set_command)
        } else {
            Err("Command is not a SET command")
        }
    }
}
impl<'a> TryFrom<&'a Command> for &'a SetCommand {
    type Error = &'static str;
    fn try_from(value: &'a Command) -> Result<Self, Self::Error> {
        if let Command::Set(set_command) = value {
            Ok(set_command)
        } else {
            Err("Command is not a SET command")
        }
    }
}
impl From<GetCommand> for Command {
    fn from(value: GetCommand) -> Self {
        Command::Get(value)
    }
}
impl TryFrom<Command> for GetCommand {
    type Error = &'static str;
    fn try_from(value: Command) -> Result<Self, Self::Error> {
        if let Command::Get(get_command) = value {
            Ok(get_command)
        } else {
            Err("Command is not a GET command")
        }
    }
}
impl<'a> TryFrom<&'a Command> for &'a GetCommand {
    type Error = &'static str;
    fn try_from(value: &'a Command) -> Result<Self, Self::Error> {
        if let Command::Get(get_command) = value {
            Ok(get_command)
        } else {
            Err("Command is not a GET command")
        }
    }
}
impl From<InfoCommand> for Command {
    fn from(command: InfoCommand) -> Self {
        Command::Info(command)
    }
}
impl TryFrom<Command> for InfoCommand {
    type Error = &'static str;
    fn try_from(value: Command) -> Result<Self, Self::Error> {
        if let Command::Info(info_command) = value {
            Ok(info_command)
        } else {
            Err("Command is not an INFO command")
        }
    }
}
impl<'a> TryFrom<&'a Command> for &'a InfoCommand {
    type Error = &'static str;
    fn try_from(value: &'a Command) -> Result<Self, Self::Error> {
        if let Command::Info(info_command) = value {
            Ok(info_command)
        } else {
            Err("Command is not an INFO command")
        }
    }
}
