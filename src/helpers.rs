use itertools::Itertools;

pub struct RespHandler;

impl RespHandler {
    pub fn to_resp_array(vec: Vec<&str>) -> String {
        let resp_string = vec.iter().map(|x| RespHandler::to_resp_string(x)).join("");
        let resp_string = format!("*{}\r\n{}", vec.len(), resp_string);
        resp_string
    }

    pub fn to_resp_string(str_input: &str) -> String {
        let x = format!("${}\r\n{}\r\n", str_input.len(), str_input);
        x
    }
}
