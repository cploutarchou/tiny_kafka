# Tiny Kafka - Lightweight Kafka Client in Rust

A lightweight, async Rust implementation of a Kafka producer and consumer. This library provides a simple, reliable interface for interacting with Apache Kafka, with built-in timeout handling and connection retries.

## Features

- **Async/Await Support**: Built on tokio for asynchronous operations
- **Timeout Handling**: Configurable timeouts for all operations
- **Connection Retries**: Automatic retry logic for failed connections
- **Error Handling**: Comprehensive error handling and logging
- **Simple API**: Easy-to-use interface for both producer and consumer

## Prerequisites

- **Rust**: Ensure you have Rust and Cargo installed
- **Kafka**: A running Kafka broker (default: localhost:9092)

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
tiny_kafka = "1.0.5"
tokio = { version = "1.0", features = ["full"] }
```

## Usage

### Consumer

The Kafka consumer provides a robust way to consume messages from Kafka topics with automatic timeout handling and connection retries.

```rust
use tiny_kafka::consumer::KafkaConsumer;
use tokio;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Create a new consumer
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

#### Consumer Configuration

The consumer supports various configuration options:

```rust
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);    // Timeout for operations
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);  // Initial connection timeout
```

### Producer

The Kafka producer provides a reliable way to send messages to Kafka topics:

```rust
use tiny_kafka::producer::KafkaProducer;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Create a new producer
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

## Error Handling

The library provides detailed error handling with specific error types for different failure scenarios:

- Connection timeouts
- Operation timeouts
- Network errors
- Protocol errors

Example error handling:

```rust
match consumer.connect().await {
    Ok(_) => println!("Connected successfully"),
    Err(e) => match e.kind() {
        io::ErrorKind::TimedOut => println!("Connection timed out"),
        io::ErrorKind::ConnectionRefused => println!("Connection refused"),
        _ => println!("Other error: {}", e),
    }
}
```

## Testing

The library includes comprehensive test coverage. Run the tests with:

```bash
cargo test --lib
```

## Logging

The library uses the `tracing` crate for logging. Enable debug logging to see detailed operation information:

```rust
use tracing_subscriber;

fn main() {
    tracing_subscriber::fmt::init();
    // Your code here
}
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
