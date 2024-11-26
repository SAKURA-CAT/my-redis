use crate::connection::Connection;
use crate::frame::Frame;

pub struct Ping {}

impl Ping {
    pub fn from_parse() -> Self {
        Ping {}
    }

    pub async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        dst.write_frame(&Frame::Simple("PONG".to_string())).await?;
        Ok(())
    }
}
