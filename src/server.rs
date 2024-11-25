use crate::db::{create, DB};
use crate::resp::{RespParser, Value};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};

pub async fn run(listener: TcpListener) {
    let db = create();
    loop {
        let stream = listener.accept().await;
        let db = db.clone();
        match stream {
            Ok((stream, _)) => {
                println!("accepted new connection");
                tokio::spawn(async move { handle_conn(stream, db).await });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_conn(stream: TcpStream, db: Arc<DB>) {
    // Every connection will be created a new RespParser
    let mut parser = RespParser::new(stream);
    println!("Handle a conn");
    loop {
        let db = db.clone();
        let value = parser.read().await.unwrap();
        println!("read value: {:?}", value);
        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).await.unwrap();
            println!("command: {}, args: {:?}", command, args);
            match command.to_lowercase().as_str() {
                "ping" => Value::SimpleString("PONG".to_string()),
                "echo" => args.first().unwrap().clone(),
                "set" => set(db, args).await,
                "get" => get(db, args).await,
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
async fn set(db: Arc<DB>, args: Vec<Value>) -> Value {
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
    let mut guard = db.string.write().await;
    guard.set(
        unpack_bulk_str(args[0].clone()).unwrap(),
        unpack_bulk_str(args[1].clone()).unwrap(),
        expire,
    )
}

async fn get(db: Arc<DB>, args: Vec<Value>) -> Value {
    let guard = db.string.read().await;
    guard.get(unpack_bulk_str(args[0].clone()).unwrap())
}
