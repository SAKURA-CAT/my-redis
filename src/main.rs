mod db;
mod resp;

use crate::db::Storage;
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
    let mut storage = db::Storage::new();
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
                "set" => set(&mut storage, args),
                "get" => get(&mut storage, args),
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

/// SET key value, and optional px seconds
/// SET key value 'px' seconds
fn set(storage: &mut Storage, args: Vec<Value>) -> Value {
    // At least 2 arguments
    if args.len() < 2 {
        return Value::Err("SET requires at least 2 arguments".to_string());
    };
    // Up to four parameters
    if args.len() > 4 {
        return Value::Err("SET requires at most 4 arguments".to_string());
    };
    // if args.len() >= 3, the third argument must be 'px'
    if args.len() >= 3 && unpack_bulk_str(args[2].clone()).unwrap() != "px" {
        return Value::Err("SET requires 'px' as the third argument".to_string());
    };
    let mut expire = None;
    if args.len() == 4 {
        let px = unpack_bulk_str(args[3].clone()).unwrap().parse::<i64>();
        // must be a number
        expire = Some(if px.is_err() {
            return Value::Err("Expire time must be a number".to_string());
        } else {
            px.unwrap()
        });
    };
    storage.set(
        unpack_bulk_str(args[0].clone()).unwrap(),
        unpack_bulk_str(args[1].clone()).unwrap(),
        expire,
    )
}

fn get(storage: &mut Storage, args: Vec<Value>) -> Value {
    storage.get(unpack_bulk_str(args[0].clone()).unwrap())
}
