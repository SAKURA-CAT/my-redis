mod resp;

use crate::resp::Value;
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    // unwrap: If the Result is Ok, this method returns the value. If it's Err, it panics.
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                println!("accepted new connection");
                tokio::spawn(async move { handle_conn(stream).await });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
async fn handle_conn(stream: TcpStream) {
    // Every connection will be created a new RespParser
    let mut parser = resp::RespParser::new(stream);
    println!("Handle a conn");
    loop {
        let value = parser.read().await.unwrap();
        println!("read value: {:?}", value);
        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).await.unwrap();
            println!("command: {}, args: {:?}", command, args);
            match command.to_lowercase().as_str() {
                "ping" => Value::SimpleString("PONG".to_string()),
                "echo" => args.first().unwrap().clone(),
                "set" => Value::SimpleString("OK".to_string()),
                "get" => Value::BulkString("bar".to_string()),
                c => panic!("unrecognized command: {}", c),
            }
        } else {
            break;
        };
        println!("response: {:?}", response);
        parser.write(response).await.unwrap();
    }
}

async fn extract_command(value: Value) -> Result<(String, Vec<Value>), anyhow::Error> {
    match value {
        Value::Array(a) => Ok((
            unpack_bulk_str(a.first().unwrap().clone())?,
            a.into_iter().skip(1).collect(),
        )),
        _ => Err(anyhow::anyhow!("expecting  command format")),
    }
}

fn unpack_bulk_str(value: Value) -> Result<String, anyhow::Error> {
    match value {
        Value::BulkString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string")),
    }
}
