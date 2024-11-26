use crate::connection::Connection;
use crate::db::Db;
use crate::parse::{Parse, ParseError};
use anyhow::anyhow;
use bytes::Bytes;
use std::time::Duration;

pub struct Set {
    key: String,
    value: String,
    expire: Option<Duration>,
}

impl Set {
    pub fn from_parse(parse: &mut Parse) -> crate::Result<Self> {
        let key = parse.next_string()?;
        let value = parse.next_string()?;
        let mut expire: Option<Duration> = None;
        match parse.next_string() {
            // An expiration is specified in seconds. The next value is an integer
            Ok(s) if s.to_uppercase() == "EX" => {
                let secs = parse.next_int()?;
                expire = Some(Duration::from_secs(secs));
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                let ms = parse.next_int()?;
                expire = Some(Duration::from_millis(ms));
            }
            Err(ParseError::EndOfStream) => {}
            _ => return Err(anyhow!("Invalid set command")),
        }
        Ok(Set { key, value, expire })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        db.set(self.key, Bytes::from(self.value), self.expire);
        dst.write_frame(&crate::frame::Frame::Simple("OK".to_string())).await?;
        Ok(())
    }
}
