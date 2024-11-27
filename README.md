[![progress-banner](https://backend.codecrafters.io/progress/redis/f1569a3b-e828-419d-8fb5-423162449f99)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

`my-redis` is a simple implementation of a Redis server, with a subset of the commands supported by Redis.
It is an incomplete, idiomatic implementation of a [Redis](https://redis.io/) server built with [Tokio](https://tokio.rs/).

This is a starting point for Rust solutions to the
["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis). Just for fun, and to learn Rust. 

While I'm working on this, I also drew inspiration from the [mini-redis](https://github.com/tokio-rs/mini-redis).

## TODO List

- [x] Basic server structure
- [x] Parse RESP protocol
- [x] Support the basic command parsing, like `SET`, `GET`, `PING`
- [x] Support expiration
- [x] Background tasks
- [ ] OpenTelemetry
- [ ] RDB Persistence
- [ ] Replication
- [ ] Pub/Sub
- [ ] Docker image support

## Running

The repository provides a RESP-compatible [server](/src/bin/server.rs), which can use `redis-cli` to interact with it.

Start the server:

```shell
cargo run --bin redis-server
```

You can use `redis-cli` to interact with the server like this:

```shell
redis-cli set foo bar ex 10
redis-cli get foo  # 'bar' in 10 seconds
```