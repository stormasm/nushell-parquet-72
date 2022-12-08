
This is [nushell-0.72.0](https://github.com/nushell/nushell/releases/tag/0.72.0) with a swapped out nu-command...

So we start out with the file nushell-0.72.0.tar.gz

And then build our own custom nu-command crate...

With the following changes...

##### nu-command/Cargo.toml

```rust
parquet = "28.0.0"
arrow = "28.0.0"
arrow-schema = { version = "28.0.0", features = ["serde"] }
serde_json = "1.0.89"
```

##### nu-command/src/default_context.rs

```rust
ToParquet,
```

##### nu-command/src/formats/to/mod.rs
```rust
mod parquet;
pub use self::parquet::ToParquet;
```

##### nu-command/src/formats/to/parquet.rs

This is a completely new file where everything happens
