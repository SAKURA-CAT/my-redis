use crate::connection::Connection;
use crate::frame::Frame;

pub struct Unknown {
    command_name: String,
}

impl Unknown {
    pub fn new(key: impl ToString) -> crate::Result<Unknown> {
        Ok(Unknown {
            command_name: key.to_string(),
        })
    }

    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = Frame::Error(format!("ERR unknown command '{}'", self.command_name));
        dst.write_frame(&response).await?;
        Ok(())
    }
}
