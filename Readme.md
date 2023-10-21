# Kafka Producer and Consumer in Rust

This repository contains Rust-based implementations of a Kafka producer and a Kafka consumer. It leverages the `rdkafka` library to communicate with Kafka and manage the production and consumption of messages.

## Prerequisites

- **Rust**: Ensure you have Rust and Cargo installed on your machine.
- **Kafka**: A running Kafka cluster that you can connect to.

## Setup

1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   ```

2. **Navigate to the Repository**:
   ```bash
   cd path-to-repository
   ```

3. **Build the Code**:
   ```bash
   cargo build
   ```

## Producer

The Kafka producer is a higher-level interface for producing messages to Kafka. It encapsulates the process of initializing a connection to the Kafka broker and sending messages to a specified topic.

### Usage

To initialize the producer:

```rust
let producer = KafkaProducer::new("localhost:9092", None);
```

To send a message to a Kafka topic:

```rust
let msg = Message::new("key1", "value1");
producer.send_message("my-topic", msg).await;
```

### Custom Configurations

You can provide custom configurations while initializing the producer:

```rust
let mut configs = HashMap::new();
configs.insert("max.in.flight.requests.per.connection", "5");
let producer_with_configs = KafkaProducer::new("localhost:9092", Some(configs));
```

## Consumer

The Kafka consumer provides functionality to consume messages from a Kafka topic.

### Usage

To initialize the consumer:

```rust
let consumer = KafkaConsumer::new("localhost:9092", "my_group", "my_topic");
```

To consume messages:

```rust
if let Some(msg) = consumer.poll().await {
    println!("Received: {} -> {}", msg.key, msg.value);
}
```

### Custom Configurations

Just like the producer, the consumer also supports custom configurations:

```rust
let mut new_configs = HashMap::new();
new_configs.insert("auto.offset.reset".to_string(), "earliest".to_string());
consumer.set_client_config("localhost:9092", "my_group", "my_topic", new_configs);
```

## Running Tests

To run tests:

```bash
cargo test
```

Ensure your Kafka broker is running and the test topic exists.

## Contribution

We welcome contributions! Feel free to open issues, submit pull requests, or just spread the word.

## License

This project is licensed under the [MIT License](LICENSE).
