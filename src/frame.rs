use anyhow::anyhow;
use bytes::{Buf, Bytes};
use std::io::Cursor;
use std::string::FromUtf8Error;
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

/// A frame in the Redis protocol.
#[derive(Clone, Debug, PartialEq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    // Null is a special case of Bulk, which represents a null value.
    Null,
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum Error {
    /// Not enough data is available to parse a message
    Incomplete,

    /// Invalid message encoding
    Other(crate::Error),
}

impl Frame {
    /// Serialize the frame to a string
    pub fn serialize(self) -> String {
        match self {
            Frame::Simple(s) => format!("+{}\r\n", s),
            Frame::Bulk(b) => format!("${}\r\n{}\r\n", b.len(), String::from_utf8(b.to_vec()).unwrap()),
            Frame::Error(s) => format!("-{}\r\n", s),
            Frame::Null => "$-1\r\n".to_string(),
            Frame::Integer(i) => format!(":{}\r\n", i),
            // TODO implement serialize for other types
            _ => panic!("Not implemented"),
        }
    }

    /// check if the frame is valid
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            b'+' => {
                get_line(src)?;
            }
            b'-' => {
                get_line(src)?;
            }
            b':' => {
                get_decimal(src)?;
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    // skip the '-1\r\n'
                    skip(src, 4)?;
                } else {
                    // read the length of the bulk string
                    let len = get_decimal(src)?;
                    skip(src, len as usize + 2)?;
                }
            }
            b'*' => {
                let len = get_decimal(src)?;
                for _ in 0..len {
                    Frame::check(src)?;
                }
            }
            _ => return Err(Error::Other(anyhow!("Not a known value type"))),
        }
        Ok(())
    }

    /// parse the frame from the buffer
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            b'+' => {
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Frame::Simple(string))
            }
            b'-' => {
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Frame::Error(string))
            }
            b':' => {
                let len = get_decimal(src)?;
                Ok(Frame::Integer(len))
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;
                    if line != b"-1" {
                        return Err(Error::Other(anyhow!("protocol error; invalid frame format")));
                    }
                    Ok(Frame::Null)
                } else {
                    let len = get_decimal(src)?;
                    let n = len as usize;
                    let mut buf = vec![0; n];
                    src.copy_to_slice(&mut buf);
                    skip(src, 2)?;
                    Ok(Frame::Bulk(Bytes::from(buf)))
                }
            }
            b'*' => {
                let len = get_decimal(src)?;
                let mut frames = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    frames.push(Frame::parse(src)?);
                }
                Ok(Frame::Array(frames))
            }
            _ => Err(Error::Other(anyhow!("Not a known value type"))),
        }
    }
}

#[cfg(test)]
mod test_frame {
    use super::*;
    #[test]
    fn test_serialize_simple_string() {
        let frame = Frame::Simple("OK".to_string());
        assert_eq!(frame.serialize(), "+OK\r\n");
    }

    #[test]
    fn test_serialize_bulk_string() {
        let frame = Frame::Bulk(Bytes::from("foo".as_bytes()));
        assert_eq!(frame.serialize(), "$3\r\nfoo\r\n");
    }

    #[test]
    fn test_serialize_error() {
        let frame = Frame::Error("ERR unknown command 'foobar'".to_string());
        assert_eq!(frame.serialize(), "-ERR unknown command 'foobar'\r\n");
    }

    #[test]
    fn test_serialize_null() {
        let frame = Frame::Null;
        assert_eq!(frame.serialize(), "$-1\r\n");
    }

    #[test]
    fn test_serialize_integer() {
        let frame = Frame::Integer(1000);
        assert_eq!(frame.serialize(), ":1000\r\n");
    }

    #[test]
    fn test_check_simple_string() {
        let mut buf = Cursor::new(&b"+OK\r\n"[..]);
        Frame::check(&mut buf).unwrap();
    }

    #[test]
    fn test_check_bulk_string() {
        let mut buf = Cursor::new(&b"$6\r\nfoobar\r\n"[..]);
        Frame::check(&mut buf).unwrap();
    }

    #[test]
    fn test_check_error() {
        let mut buf = Cursor::new(&b"-ERR unknown command 'foobar'\r\n"[..]);
        Frame::check(&mut buf).unwrap();
    }

    #[test]
    fn test_check_null() {
        let mut buf = Cursor::new(&b"$-1\r\n"[..]);
        Frame::check(&mut buf).unwrap();
    }

    #[test]
    fn test_check_integer() {
        let mut buf = Cursor::new(&b":1000\r\n"[..]);
        Frame::check(&mut buf).unwrap();
    }

    #[test]
    fn test_parse_simple_string() {
        let mut buf = Cursor::new(&b"+OK\r\n"[..]);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Simple("OK".to_string()));
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut buf = Cursor::new(&b"$6\r\nfoobar\r\n"[..]);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Bulk(Bytes::from("foobar".as_bytes())));
    }

    #[test]
    fn test_parse_error() {
        let mut buf = Cursor::new(&b"-ERR unknown command 'foobar'\r\n"[..]);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Error("ERR unknown command 'foobar'".to_string()));
    }

    #[test]
    fn test_parse_null() {
        let mut buf = Cursor::new(&b"$-1\r\n"[..]);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Null);
    }

    #[test]
    fn test_parse_integer() {
        let mut buf = Cursor::new(&b":1000\r\n"[..]);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Integer(1000));
    }

    #[test]
    fn test_parse_array() {
        let mut buf = Cursor::new(&b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..]);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::Bulk(Bytes::from("foo".as_bytes())),
                Frame::Bulk(Bytes::from("bar".as_bytes()))
            ])
        );
    }
}

/// skip n bytes from the buffer, the current position is advanced by n.
fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    };
    src.advance(n);
    Ok(())
}

#[cfg(test)]
mod test_skip {
    use super::*;
    #[test]
    fn test_skip() {
        let mut buf = Cursor::new(&b"Hello"[..]);
        skip(&mut buf, 2).unwrap();
        assert_eq!(buf.position(), 2);
    }
}

/// peek an u8 from the buffer, but the current position is not advanced.
fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(src.chunk()[0])
}

#[cfg(test)]
mod test_peek_u8 {
    use super::*;
    #[test]
    fn test_peek_u8() {
        let mut buf = Cursor::new(&b"Hello"[..]);
        let u8 = peek_u8(&mut buf).unwrap();
        assert_eq!(u8, b'H');
        assert_eq!(buf.position(), 0);
    }
}

/// get an u8 from the buffer, The current position is advanced by 1.
fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(src.get_u8())
}

#[cfg(test)]
mod test_get_u8 {
    use super::*;
    #[test]
    fn test_get_u8() {
        let mut buf = Cursor::new(&b"Hello"[..]);
        let u8 = get_u8(&mut buf).unwrap();
        assert_eq!(u8, b'H');
    }
}

/// get a line from the buffer, for example, OK\r\n will return OK
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let start = src.position() as usize;
    let mut end = src.get_ref().len();
    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            end = i;
            break;
        }
    }
    let line = &src.get_ref()[start..end];
    src.set_position(end as u64 + 2);
    Ok(line)
}

#[cfg(test)]
mod test_get_line {
    use super::*;
    #[test]
    fn test_get_line() {
        let mut buf = Cursor::new(&b"Hello\r\nWorld"[..]);
        let line = get_line(&mut buf).unwrap();
        assert_eq!(line, b"Hello");
    }
}

/// Read a new-line terminated decimal
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    let maybe_num = if let Ok(line) = get_line(src) {
        match String::from_utf8(line.to_vec())?.parse() {
            Ok(num) => Ok(num),
            Err(_) => Err(Error::Other(anyhow!("protocol error; invalid number"))),
        }
    } else {
        Err(Error::Incomplete)
    };
    match maybe_num {
        Ok(num) => Ok(num),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod test_get_decimal {
    use super::*;
    #[test]
    fn test_get_decimal() {
        let mut buf = Cursor::new(&b"1000\r\n"[..]);
        let num = get_decimal(&mut buf).unwrap();
        assert_eq!(num, 1000);
    }
}

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(anyhow!(src))
    }
}

impl From<FromUtf8Error> for Error {
    fn from(src: FromUtf8Error) -> Error {
        Error::Other(anyhow!(src))
    }
}
