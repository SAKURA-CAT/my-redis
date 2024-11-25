//! Parse a array, which means a command sent by the client

use crate::frame::Frame;
use anyhow::anyhow;
use bytes::Bytes;
use std::{str, vec};

#[derive(Debug)]
pub(crate) struct Parse {
    blocks: vec::IntoIter<Frame>,
}
#[derive(Debug)]
pub(crate) enum ParseError {
    /// Attempting to extract a value failed due to the frame being fully
    /// consumed.
    EndOfStream,

    /// All other errors
    Other(crate::Error),
}

impl Parse {
    pub(crate) fn new(frame: Frame) -> Result<Parse, ParseError> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(format!("protocol error; expected array, got {:?}", frame).into()),
        };
        Ok(Parse {
            blocks: array.into_iter(),
        })
    }

    /// Return the next block
    fn next(&mut self) -> Result<Frame, ParseError> {
        self.blocks.next().ok_or(ParseError::EndOfStream)
    }

    /// Return the next block as a string
    pub(crate) fn next_string(&mut self) -> Result<String, ParseError> {
        match self.next()? {
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(b) => str::from_utf8(&b[..])
                .map(|s| s.to_string())
                .map_err(|_| "protocol error; invalid string".into()),
            frame => Err(format!("protocol error; expected simple or bulk, got {:?}", frame).into()),
        }
    }

    /// Return the next block as raw bytes
    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, ParseError> {
        match self.next()? {
            Frame::Bulk(b) => Ok(b),
            frame => Err(format!("protocol error; expected bulk, got {:?}", frame).into()),
        }
    }

    /// Return the next block as an integer
    pub(crate) fn next_int(&mut self) -> Result<i64, ParseError> {
        let s = self.next_string()?;
        s.parse().map_err(|_| "protocol error; invalid number".into())
    }
}

#[cfg(test)]
mod test_parse {
    use super::*;
    use crate::frame::Frame;

    #[test]
    fn test_new() {
        let frame = Frame::Array(vec![Frame::Simple("GET".to_string()), Frame::Simple("foo".to_string())]);
        let parse = Parse::new(frame).unwrap();
        assert_eq!(parse.blocks.len(), 2);
    }

    #[test]
    fn test_new_error() {
        let frame = Frame::Simple("GET".to_string());
        let parse = Parse::new(frame);
        assert!(parse.is_err());
    }

    #[test]
    fn test_next() {
        let frame = Frame::Array(vec![Frame::Simple("GET".to_string()), Frame::Simple("foo".to_string())]);
        let mut parse = Parse::new(frame).unwrap();
        let block = parse.next().unwrap();
        assert_eq!(block, Frame::Simple("GET".to_string()));
    }

    #[test]
    fn test_next_error() {
        let frame = Frame::Array(vec![Frame::Simple("GET".to_string()), Frame::Simple("foo".to_string())]);
        let mut parse = Parse::new(frame).unwrap();
        let _ = parse.next().unwrap();
        let block = parse.next();
        assert!(block.is_ok());
        // err can not impl PartialEq
        match parse.next() {
            Ok(_) => assert!(false),
            Err(ParseError::EndOfStream) => assert!(true),
            Err(_) => assert!(false),
        }
    }
}

impl From<String> for ParseError {
    fn from(value: String) -> Self {
        ParseError::Other(anyhow!(value))
    }
}

#[cfg(test)]
mod test_from_string_trait {
    use super::*;

    fn create_parse_error() -> ParseError {
        "test".to_string().into()
    }

    #[test]
    fn test_from_string() {
        let error = create_parse_error();
        match error {
            ParseError::Other(_) => assert!(true),
            _ => assert!(false),
        }
    }
}

impl From<&str> for ParseError {
    fn from(value: &str) -> Self {
        ParseError::Other(anyhow!(value.to_string()))
    }
}

#[cfg(test)]
mod test_from_str_trait {
    use super::*;

    fn create_parse_error() -> ParseError {
        "test".into()
    }

    #[test]
    fn test_from_str() {
        let error = create_parse_error();
        match error {
            ParseError::Other(_) => assert!(true),
            _ => assert!(false),
        }
    }
}
