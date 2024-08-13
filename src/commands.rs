#[derive(Debug)]
pub enum Section {
    Custom(String),
    All,
}

pub struct EchoCommand {
    pub message: String,
}

pub struct InfoCommand {
    pub section: Section,
}

impl InfoCommand {
    pub fn new(section: Section) -> Self {
        InfoCommand { section }
    }
}

pub struct SetCommand {
    pub key: String,
    pub value: String,
    pub px: Option<i64>,
}

pub struct GetCommand {
    pub key: String,
}

pub enum Command {
    Ping,
    ECHO(EchoCommand),
    INFO(InfoCommand),
    SET(SetCommand),
    GET(GetCommand),
}

impl EchoCommand {
    pub fn new(message: &str) -> Self {
        EchoCommand {
            message: message.to_string(),
        }
    }
}

impl SetCommand {
    pub fn new(key: &str, value: &str, px: Option<i64>) -> Self {
        SetCommand {
            key: key.to_string(),
            value: value.to_string(),
            px,
        }
    }
}

impl GetCommand {
    pub fn new(key: &str) -> Self {
        GetCommand {
            key: key.to_string(),
        }
    }
}
