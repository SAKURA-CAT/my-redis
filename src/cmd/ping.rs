use crate::connection::Connection;

pub struct Ping {}

impl Ping {
    pub fn new() -> Self {
        Ping {}
    }

    pub async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        "+PONG\r\n".to_string();
        Ok(())
    }
}
