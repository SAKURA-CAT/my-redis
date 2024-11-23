//! Redis protocol (RESP - Redis Serialization Protocol) defines five basic data types.
//! We need to define a set of parsers for RESP to parse RESP-formatted data into Rust data structures.
//!
//! In this set of parsers, the data types are called [Value], and the processors are called [RespParser].

use anyhow::anyhow;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
// These five types are:
//
// 1. Simple Strings: Start with +, followed by the string content, and end with \r\n.
//    for example: +OK\r\n
// 2. Errors: Start with -, followed by the error message, and end with \r\n.
//    for example: -ERR unknown command 'foobar'\r\n
// 3. Integers: Start with :, followed by the integer, and end with \r\n.
//    for example: :1000\r\n
// 4. Bulk Strings: Start with $, followed by the string length, then \r\n, followed by the string content, and end with \r\n.
//    for example: $6\r\nfoo\r\n
// 5. Arrays: Start with *, followed by the number of array elements, and then the serialized representation of each element.
//    for example: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
use tokio::net::TcpStream;

/// The RESP data types.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Value>),
    // Errors(String),
    // Integers(i64),
    Null,
}

impl Value {
    pub fn serialize(self) -> String {
        match self {
            Value::SimpleString(s) => format!("+{}\r\n", s),
            Value::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            // TODO implement serialize for other types
            _ => panic!("Not implemented"),
        }
    }
}

#[cfg(test)]
mod test_value {
    use super::*;

    #[test]
    fn test_serialize_simple_string() {
        let value = Value::SimpleString("OK".to_string());
        assert_eq!(value.serialize(), "+OK\r\n");
    }

    #[test]
    fn test_serialize_bulk_string() {
        let value = Value::BulkString("foobar".to_string());
        assert_eq!(value.serialize(), "$6\r\nfoobar\r\n");
    }
    #[test]
    #[should_panic(expected = "Not implemented")]
    fn test_serialize_array() {
        let value = Value::Array(vec![
            Value::SimpleString("foo".to_string()),
            Value::SimpleString("bar".to_string()),
        ]);
        // panic
        value.serialize();
    }
}

pub struct RespParser {
    stream: TcpStream,
    buf: BytesMut,
}

impl RespParser {
    pub fn new(stream: TcpStream) -> Self {
        RespParser {
            stream,
            // TODO buf的长度可能并不够
            buf: BytesMut::with_capacity(512),
        }
    }

    /// Read a RESP value from the stream.
    pub async fn read(&mut self) -> Result<Option<Value>, anyhow::Error> {
        let bytes_read = self.stream.read_buf(&mut self.buf).await?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let (v, _) = parse_msg(self.buf.split())?;
        Ok(Some(v))
    }

    /// Write a RESP value to the stream.
    pub async fn write(&mut self, value: Value) -> Result<(), anyhow::Error> {
        self.stream.write(value.serialize().as_bytes()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod test_resp_parser {
    use crate::resp::RespParser;
    use nanoid::nanoid;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    const ADDR: &str = "127.0.0.1:12345";
    /// mock a tcp stream to test
    async fn mock_stream(len: usize) -> TcpStream {
        // Create a listener
        let listener = TcpListener::bind(ADDR).await.unwrap();
        // Create a handle
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let s = nanoid!(len);
            stream.write_all(s.as_bytes()).await.unwrap();
        });
        // create a stream
        TcpStream::connect(ADDR).await.unwrap()
    }

    #[tokio::test]
    async fn test_new_resp_parser() {
        let stream = mock_stream(512).await;
        let parser = RespParser::new(stream);
        assert_eq!(parser.buf.len(), 0);
    }
}

/// parse [BytesMut] to [Value], it will also return the next position
fn parse_msg(buf: BytesMut) -> Result<(Value, usize), anyhow::Error> {
    match buf[0] as char {
        '+' => parse_simple_string(buf),
        '$' => parse_bulk_string(buf),
        '*' => parse_array(buf),
        _ => Err(anyhow!("Not a known value type {:?}", buf)),
    }
}

#[cfg(test)]
mod test_parse_msg {
    use super::*;
    #[test]
    fn test_parse_msg() {
        let buf = BytesMut::from("Unknown\r\n");
        let result = parse_msg(buf);
        assert!(result.is_err());
    }
}

/// Parse a simple string, which starts with '+', followed by the string content, and ends with \r\n.
fn parse_simple_string(buf: BytesMut) -> Result<(Value, usize), anyhow::Error> {
    if let Some((line, next)) = read_until_crlf(&buf[1..]) {
        let s = String::from_utf8(line.to_vec())?;
        return Ok((Value::SimpleString(s), next));
    }
    Err(anyhow!("Not a simple string {:?}", buf))
}

#[cfg(test)]
mod test_parse_simple_string {
    use crate::resp::{parse_simple_string, Value};
    use bytes::BytesMut;

    #[test]
    fn test_parse_simple_string() {
        let buf = BytesMut::from("+OK\r\n");
        let result = parse_simple_string(buf).unwrap();
        assert_eq!(result.0, Value::SimpleString("OK".to_string()));
        assert_eq!(result.1, 4);
    }
    #[test]
    fn test_parse_simple_string_error() {
        let mut buf = BytesMut::from("+OK");
        let mut result = parse_simple_string(buf);
        assert!(result.is_err());
        buf = BytesMut::from("+OK\n");
        result = parse_simple_string(buf);
        assert!(result.is_err());
    }
}

/// Parse a bulk string, which starts with '$', followed by the string length, then \r\n, followed by the string content, and ends with \r\n.
fn parse_bulk_string(buf: BytesMut) -> Result<(Value, usize), anyhow::Error> {
    let (bulk_str_len, buf_next) = if let Some((line, next)) = read_until_crlf(&buf[1..]) {
        let bulk_str_len = parse_int(line)?;
        // +1 for the $, which is not included in the bulk string length
        (bulk_str_len, next + 1)
    } else {
        return Err(anyhow!("Not a bulk string {:?}", buf));
    };

    // +2 for \r\n，because '\r\n' is needed in bulk string
    let end_of_bulk_str = buf_next + bulk_str_len as usize;
    if buf.len() < end_of_bulk_str {
        return Err(anyhow!("Not a bulk string {:?}", buf));
    }
    Ok((
        Value::BulkString(String::from_utf8(buf[buf_next..end_of_bulk_str].to_vec())?),
        end_of_bulk_str + 2,
    ))
}

#[cfg(test)]
mod test_parse_bulk_string {
    use crate::resp::{parse_bulk_string, Value};
    use bytes::BytesMut;

    #[test]
    fn test_parse_bulk_string() {
        let buf = BytesMut::from("$6\r\nfoobar\r\n");
        let result = parse_bulk_string(buf).unwrap();
        assert_eq!(result.0, Value::BulkString("foobar".to_string()));
        assert_eq!(result.1, 12);
    }
    #[test]
    fn test_parse_bulk_string_error() {
        let mut buf = BytesMut::from("$6\r\nfoobar");
        let mut result = parse_bulk_string(buf);
        assert!(result.is_err());
        buf = BytesMut::from("$6\r\nfoobar\n");
        result = parse_bulk_string(buf);
        assert!(result.is_err());
    }
}

fn parse_array(buf: BytesMut) -> Result<(Value, usize), anyhow::Error> {
    let (array_length, mut buf_next) = if let Some((line, next)) = read_until_crlf(&buf[1..]) {
        let array_length = parse_int(line)?;
        (array_length, next)
    } else {
        return Err(anyhow!("Not an array {:?}", buf));
    };
    let mut items = vec![];
    for _ in 0..array_length {
        let (item, len) = parse_msg(BytesMut::from(&buf[buf_next + 1..]))?;
        items.push(item);
        buf_next += len;
    }
    Ok((Value::Array(items), buf_next))
}

#[cfg(test)]
mod test_parse_array {
    use bytes::BytesMut;

    #[test]
    fn test_parse_array() {
        let buf = BytesMut::from("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let result = super::parse_array(buf).unwrap();
        assert_eq!(
            result.0,
            super::Value::Array(vec![
                super::Value::BulkString("foo".to_string()),
                super::Value::BulkString("bar".to_string())
            ])
        );
        assert_eq!(result.1, 21);
    }
}

/// Read until \r\n, return the line and the next position
fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[0..i - 1], i + 1));
        }
    }
    None
}

#[cfg(test)]
mod test_read_until_crlf {
    use super::*;
    #[test]
    fn test_read_until_crlf() {
        let buffer = b"Hello\r\nWorld";
        let (line, next) = read_until_crlf(buffer).unwrap();
        assert_eq!(line, b"Hello");
        assert_eq!(next, 7);
    }
}

/// Parse an integer
fn parse_int(buffer: &[u8]) -> Result<i64, anyhow::Error> {
    Ok(String::from_utf8(buffer.to_vec())?.parse::<i64>()?)
}

#[cfg(test)]
mod test_parse_int {
    use super::*;
    #[test]
    fn test_parse_int() {
        let buffer = b"1234";
        let result = parse_int(buffer).unwrap();
        assert_eq!(result, 1234);
    }
    #[test]
    fn test_parse_int_error() {
        let buffer = b"1234a";
        let result = parse_int(buffer);
        assert!(result.is_err());
    }
}
