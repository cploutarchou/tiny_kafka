# Tiny Kafka - Lightweight Kafka Client in Rust

[![Crates.io](https://img.shields.io/crates/v/tiny_kafka.svg)](https://crates.io/crates/tiny_kafka)
[![Documentation](https://docs.rs/tiny_kafka/badge.svg)](https://docs.rs/tiny_kafka)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A lightweight, async Rust implementation of a Kafka producer and consumer. This library provides a simple, reliable interface for interacting with Apache Kafka, with built-in timeout handling and connection retries.

## Features

- **Async/Await Support**: Built on tokio for high-performance asynchronous operations
- **Timeout Handling**: Configurable timeouts for all operations with sensible defaults
- **Connection Retries**: Automatic retry logic for failed connections with exponential backoff
- **Error Handling**: Comprehensive error handling with detailed error types
- **Simple API**: Easy-to-use interface for both producer and consumer
- **Zero-Copy**: Efficient message handling with minimal memory overhead
- **Type Safety**: Strong Rust type system ensuring runtime safety
- **Logging**: Integrated tracing support for debugging and monitoring

## Prerequisites

- **Rust**: Rust 1.70.0 or higher
- **Kafka**: A running Kafka broker (default: localhost:9092)

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
tiny_kafka = "1.0.5"
tokio = { version = "1.0", features = ["full"] }
```

## Quick Start

### Consumer Example

```rust
use tiny_kafka::consumer::KafkaConsumer;
use tokio;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Create and configure consumer
    let mut consumer = KafkaConsumer::new(
        "127.0.0.1:9092".to_string(),
        "my-group".to_string(),
        "my-topic".to_string(),
    ).await?;
    
    // Connect with automatic retries
    consumer.connect().await?;
    
    // Consume messages
    let messages = consumer.consume().await?;
    for msg in messages {
        println!("Received message: {:?}", msg);
    }
    
    // Clean up
    consumer.close().await?;
    Ok(())
}
```

### Producer Example

```rust
use tiny_kafka::producer::KafkaProducer;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Create producer
    let producer = KafkaProducer::new(
        "127.0.0.1:9092".to_string(),
        None, // Optional configurations
    );
    
    // Send a message
    producer.send_message(
        "my-topic",
        Message::new("key", "value"),
    ).await?;
    
    Ok(())
}
```

## Configuration

### Consumer Configuration

```rust
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);    // Operation timeout
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);  // Initial connection timeout
```

### Producer Configuration

The producer supports various configuration options including:
- Batch size
- Compression
- Acknowledgment level
- Retry settings

## Error Handling

The library provides detailed error types for different scenarios:

- `ConnectionError`: Failed to establish connection
- `TimeoutError`: Operation exceeded configured timeout
- `ProtocolError`: Kafka protocol-related errors
- `SerializationError`: Message serialization failures

## Performance Considerations

- Uses zero-copy operations where possible
- Efficient buffer management with `BytesMut`
- Configurable batch sizes for optimal throughput
- Connection pooling for better resource utilization

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## Testing

Run the test suite:

```bash
cargo test
```

For integration tests with a running Kafka instance:

```bash
cargo test --features integration-tests
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with [tokio](https://tokio.rs/) for async runtime
- Uses [bytes](https://docs.rs/bytes/) for efficient buffer management
- Logging provided by [tracing](https://docs.rs/tracing/)
