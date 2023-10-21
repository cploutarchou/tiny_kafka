# Kafka Producer and Consumer in Rust

This repository contains Rust-based implementations of a Kafka producer and a Kafka consumer. It leverages the `rdkafka` library to communicate with Kafka and manage the production and consumption of messages.

## Prerequisites

- **Rust**: Ensure you have Rust and Cargo installed on your machine.
- **Kafka**: A running Kafka cluster that you can connect to.

## Setup

1. **Clone the Repository**:
   ```bash
   git clone github.com/cploutarchou/tiny_kafka
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

#### To initialize the consumer:

```rust
let consumer = KafkaConsumer::new("localhost:9092", "my_group", "my_topic");
```

#### To consume messages:

```rust
if let Some(msg) = consumer.poll().await {
    println!("Received: {} -> {}", msg.key, msg.value);
}
```
#### Full Main Function Example with Tokio and Async

Below is a detailed example of how to utilize both the Kafka producer and consumer within an asynchronous context, leveraging Tokio.

```rust
use log::info;
use std::sync::Arc;
use tiny_kafka::consumer::KafkaConsumer;
use tiny_kafka::producer::{KafkaProducer, Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

   let rt = tokio::runtime::Runtime::new().unwrap();
   // Assuming kafka_bootstrap_servers is of type String
   let brokers = Arc::new("localhost:9092".to_string());
   let topics = Arc::new(vec!["test".to_string()]);

   // Consumer task
   let brokers_for_task1 = brokers.clone();
   let topics_for_task1 = topics.clone();
   let task1 = async move {
      let consumer = KafkaConsumer::new(
         brokers_for_task1.as_str(),
         "kafka-to-elastic",
         topics_for_task1.get(0).unwrap(),
      );
      loop {
         if let Some(msg) = consumer.poll().await {
            info!(
                    "Consumed message with key: {} and value: {}",
                    msg.key, msg.value
                );
         }
      }
   };
   rt.spawn(task1);

   // Producer task
   let brokers_for_task2 = brokers.clone();
   let topics_for_task2 = topics.clone();
   let task2 = async move {
      let producer = KafkaProducer::new(brokers_for_task2.as_str(), Option::None);

      for i in 0..100 {
         let key = format!("test_key_{}", i);
         let value = format!("test_value_{}", i);
         let message = Message::new(&key, &value);

         producer
                 .send_message(topics_for_task2.get(0).unwrap(), message)
                 .await;
         tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
      }
   };
   rt.spawn(task2);

   // Wait for a ctrl-c signal
   tokio::signal::ctrl_c().await?;
   println!("ctrl-c received!");

   Ok(())
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
