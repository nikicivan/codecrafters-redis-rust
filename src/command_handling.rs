use crate::{
    commands::{Command, EchoCommand, GetCommand, InfoCommand, Section, SetCommand},
    server::{CacheValue, Session},
};
use tokio::{io::AsyncWriteExt, net::TcpStream};

pub struct CommandHandler;

impl CommandHandler {
    pub async fn handle(&mut self, session: &mut Session, command: Command) {
        match command {
            Command::Ping => ping(session).await,
            Command::ECHO(cmd) => echo(session, cmd).await,
            Command::INFO(cmd) => info(session, cmd).await,
            Command::SET(cmd) => set_command(session, cmd).await,
            Command::GET(cmd) => get_command(session, cmd).await,
        }
    }
}

async fn write_response(stream: &mut TcpStream, response_str: &str) {
    println!("RESPONSE: {:#?}", response_str);
    stream
        .write_all(response_str.as_bytes())
        .await
        .expect("Can not write response");
}

async fn ping(session: &mut Session) {
    write_response(&mut session.stream, "+PONG\r\n").await;
}

async fn echo(session: &mut Session, cmd: EchoCommand) {
    let res = format!("${}\r\n{}\r\n", cmd.message.len(), cmd.message);
    write_response(&mut session.stream, &res).await;
}

async fn info(session: &mut Session, cmd: InfoCommand) {
    let mut infos: Vec<String> = Vec::new();

    match cmd.section {
        Section::Custom(section) => {
            if section == "replication" {
                infos.extend(session.server_info.replication_info())
            }
        }
        Section::All => infos.extend(session.server_info.replication_info()),
    }

    let res = infos.join("\r\n");
    let res = format!("${}\r\n{}\r\n", res.len(), res);

    write_response(&mut session.stream, &res).await;
}

async fn set_command(session: &mut Session, cmd: SetCommand) {
    session
        .storage
        .insert(cmd.key, CacheValue::new(cmd.value.as_str(), cmd.px));

    write_response(&mut session.stream, "+OK\r\n").await;
}

async fn get_command(session: &mut Session, cmd: GetCommand) {
    let cv = session.storage.get(&cmd.key);

    if cv.is_some_and(|x| !x.is_expired()) {
        let cvu = cv.unwrap();
        write_response(
            &mut session.stream,
            format!("${}\r\n{}\r\n", cvu.value.len(), cvu.value).as_str(),
        )
        .await;
    }
}
