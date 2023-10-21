use log::{error, info};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message as KafkaMessage;
use serde::Deserialize;
use std::collections::HashMap;
use tokio_stream::StreamExt;

/// Represents a message received from Kafka with its key and value.
#[derive(Deserialize)]
pub struct ReceivedMessage {
    pub key: String,
    pub value: String,
}

/// Provides functionality to consume messages from a Kafka topic.
///
/// The `KafkaConsumer` allows you to connect to a Kafka broker, subscribe to a topic,
/// and consume messages from that topic. The consumer can be reconfigured with custom settings
/// as needed.
///
/// # Examples
///
/// Basic usage:
///
/// ```no_run
/// use std::collections::HashMap;
/// let mut consumer = KafkaConsumer::new("localhost:9092", "my_group", "my_topic");
///
/// // Consume messages in a loop
/// tokio::spawn(async move {
///     loop {
///         if let Some(msg) = consumer.poll().await {
///             println!("Received: {} -> {}", msg.key, msg.value);
///         }
///     }
/// });
/// ```
pub struct KafkaConsumer {
    consumer: StreamConsumer,
}
impl KafkaConsumer {
    /// Creates a new instance of the Kafka consumer.
    ///
    /// Initializes the consumer with the given broker addresses, group ID, and topic name.
    /// The consumer starts reading messages from the specified topic once it's created.
    ///
    /// # Arguments
    ///
    /// * `brokers`: A comma-separated list of broker addresses.
    /// * `group_id`: The ID of the consumer group.
    /// * `topic_name`: The name of the Kafka topic to which the consumer should subscribe.
    ///
    /// # Panics
    ///
    /// * If the consumer creation fails.
    /// * If the consumer cannot subscribe to the specified topic.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```no_run
    /// let consumer = KafkaConsumer::new("localhost:9092", "my_group", "my_topic");
    /// ```
    pub fn new(brokers: &str, group_id: &str, topic_name: &str) -> Self {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .create()
            .expect("Consumer creation failed");

        consumer
            .subscribe(&[topic_name])
            .expect("Can't subscribe to specified topic");

        KafkaConsumer { consumer }
    }
    /// Polls for a message from the subscribed Kafka topic.
    ///
    /// Checks for a new message in the Kafka topic. If a message is found, it's deserialized
    /// and returned as an instance of `ReceivedMessage`. If no message is found or an error occurs,
    /// it returns `None`.
    ///
    /// This function asynchronously polls the Kafka topic and returns once a message is found or
    /// if there's an error. In the latter case, the error is logged.
    ///
    /// # Returns
    ///
    /// An `Option` containing a `ReceivedMessage` if a message is received, or `None` otherwise.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```no_run
    /// tokio::spawn(async move {
    ///     let consumer = KafkaConsumer::new("localhost:9092", "my_group", "my_topic");
    ///     if let Some(msg) = consumer.poll().await {
    ///         println!("Received: {} -> {}", msg.key, msg.value);
    ///     }
    /// });
    /// ```
    pub async fn poll(&self) -> Option<ReceivedMessage> {
        if let Some(result) = self.consumer.stream().next().await {
            match result {
                Ok(kafka_message) => {
                    let key = match kafka_message.key() {
                        Some(k) => String::from_utf8_lossy(k).to_string(),
                        None => String::from(""),
                    };

                    let value = match kafka_message.payload_view::<str>() {
                        Some(Ok(v)) => v.to_owned(),
                        Some(Err(e)) => {
                            error!("Error deserializing message payload: {:?}", e);
                            return None;
                        }
                        None => return None,
                    };

                    info!("Received message with key {}: {}", key, value);
                    Some(ReceivedMessage { key, value })
                }
                Err(e) => {
                    error!("Error while receiving message: {:?}", e);
                    None
                }
            }
        } else {
            None
        }
    }
    /// Sets custom configurations for the Kafka consumer and recreates it with the given configurations.
    ///
    /// This method allows you to modify the Kafka consumer configurations dynamically. After setting the
    /// desired configurations, the existing consumer instance is replaced with a new one using the updated
    /// configurations. The consumer then subscribes to the specified topic.
    ///
    /// # Arguments
    ///
    /// * `brokers`: A comma-separated list of broker addresses. This is used to specify to which brokers the consumer should connect.
    /// * `group_id`: The ID of the consumer group. It is used to identify a group of consumer processes that jointly consume messages from one or multiple Kafka topics.
    /// * `topic_name`: The name of the Kafka topic to which the consumer should subscribe.
    /// * `configurations`: A hashmap containing additional configurations you want to set for the Kafka consumer. Each entry in the hashmap represents a key-value configuration pair.
    ///
    /// # Panics
    ///
    /// This method will panic if:
    ///
    /// * The consumer creation fails with the given configurations.
    /// * The consumer fails to subscribe to the specified topic.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::collections::HashMap;
    /// let mut consumer = KafkaConsumer::new("localhost:9092", "my_group", "my_topic");
    /// let mut new_configs = HashMap::new();
    /// new_configs.insert("auto.offset.reset".to_string(), "earliest".to_string());
    /// consumer.set_client_config("localhost:9092", "my_group", "my_topic", new_configs);
    /// ```
    pub fn set_client_config(
        &mut self,
        brokers: &str,
        group_id: &str,
        topic_name: &str,
        configurations: HashMap<String, String>,
    ) {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", brokers);
        client_config.set("group.id", group_id);

        for (key, value) in &configurations {
            client_config.set(key, value);
        }

        self.consumer = client_config
            .create::<StreamConsumer>()
            .expect("Failed to create new consumer with updated configurations");
        self.consumer
            .subscribe(&[topic_name])
            .expect("Can't subscribe to specified topic");
    }
}
/// Unit tests for the KafkaConsumer functionality.
#[cfg(test)]
mod tests {
    use super::*;
    use log::LevelFilter;
    use simple_logger::SimpleLogger;

    /// Tests the functionality of the KafkaConsumer by initializing it, polling for a message,
    /// and logging the received message.
    #[tokio::test]
    async fn test_kafka_consumer() {
        // Initialize the logger for the test
        SimpleLogger::new()
            .with_level(LevelFilter::Info)
            .init()
            .unwrap();

        let brokers = "localhost:9092";
        let group_id = "test-group".to_string();
        let topic = "test-topic";
        let consumer = KafkaConsumer::new(brokers, group_id.as_str(), topic);
        loop {
            if let Some(msg) = consumer.poll().await {
                info!(
                    "Consumed message with key: {} and value: {}",
                    msg.key, msg.value
                );
            }
            return;
        }
    }
}
