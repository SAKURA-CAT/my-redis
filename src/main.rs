#![allow(unused_imports)]

use std::io::{Read, Write};
use std::net::TcpListener;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    // unwrap: If the Result is Ok, this method returns the value. If it's Err, it panics.
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming(){
        match stream {
            Ok(_stream) => {
                // 多线程处理
                std::thread::spawn(move || {
                    handle_client(_stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}


fn handle_client(mut stream: std::net::TcpStream) {
    let buffer = &mut [0; 1024];
    loop {
        let read_count = stream.read(buffer).unwrap();
        if read_count == 0{
            break;
        }
        stream.write_all(b"+PONG\r\n").unwrap();
    }
}