// Import necessary libraries and modules
use log::{error, info};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use serde_json::to_string;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Represents a message with a key and value to be sent to Kafka.
#[derive(Serialize)]
pub struct Message {
    pub key: String,
    pub value: String,
}

impl Message {
    /// Create a new message instance with the specified key and value.
    ///
    /// # Arguments
    /// * `key`: The message key.
    /// * `value`: The message value.
    ///
    /// # Examples
    ///
    /// ```
    /// use tiny_kafka::producer::Message;
    /// let msg = Message::new("key1", "value1");
    /// assert_eq!(msg.key, "key1");
    /// assert_eq!(msg.value, "value1");
    /// ```
    pub fn new(key: &str, value: &str) -> Self {
        Self {
            key: key.to_string(),
            value: value.to_string(),
        }
    }
}

/// KafkaProducer provides a higher-level interface for producing messages to Kafka.
pub struct KafkaProducer {
    producer: Arc<FutureProducer>,
}

impl KafkaProducer {
    /// Initialize a new KafkaProducer with the specified broker and optional configurations.
    ///
    /// # Arguments
    /// * `brokers`: A comma-separated list of broker addresses.
    /// * `configurations`: Optional custom configurations as a key-value hashmap.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// // Initialize a KafkaProducer without any custom configurations
    /// use std::collections::HashMap;
    /// use tiny_kafka::producer::KafkaProducer;
    /// let producer = KafkaProducer::new("localhost:9092", None);
    ///
    /// // With custom configurations
    /// let mut configs = HashMap::new();
    /// configs.insert("max.in.flight.requests.per.connection", "5");
    /// let producer_with_configs = KafkaProducer::new("localhost:9092", Some(configs));
    /// ```
    pub fn new(brokers: &str, configurations: Option<HashMap<&str, &str>>) -> Self {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", brokers);

        // If additional configurations are provided, apply them.
        if let Some(configs) = configurations {
            for (key, value) in configs {
                client_config.set(key, value);
            }
        }

        let producer = Arc::new(
            client_config
                .set("message.timeout.ms", "5000")
                .create()
                .expect("Producer creation error"),
        );

        KafkaProducer { producer }
    }

    /// Send a message to the specified Kafka topic asynchronously.
    ///
    /// # Arguments
    /// * `topic_name`: The name of the Kafka topic.
    /// * `message`: The message to send.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tiny_kafka::producer::{KafkaProducer, Message};
    /// let producer = KafkaProducer::new("localhost:9092", None);
    /// let msg = Message::new("key1", "value1");
    ///
    /// // Using async runtime for demonstration, replace as needed
    /// let runtime = tokio::runtime::Runtime::new().unwrap();
    /// runtime.block_on(async {
    ///     producer.send_message("my-topic", msg).await;
    /// });
    /// ```
    pub async fn send_message(&self, topic_name: &str, message: Message) {
        let json_payload = to_string(&message.value).expect("Failed to serialize message");

        let delivery_status = self
            .producer
            .send(
                FutureRecord::to(topic_name)
                    .payload(&json_payload)
                    .key(&message.key),
                Duration::from_secs(0),
            )
            .await;

        // Log the status of the message delivery.
        match delivery_status {
            Ok((partition, offset)) => {
                info!(
                    "Message with key {} delivered to partition {} at offset {}",
                    message.key, partition, offset
                );
            }
            Err((error, _message)) => {
                error!(
                    "Failed to deliver message with key {}: {}",
                    message.key, error
                );
            }
        }
    }

    /// Set custom configurations for the Kafka producer.
    ///
    /// # Arguments
    /// * `configurations`: A hashmap of key-value pairs to set as configurations.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::collections::HashMap;
    /// use tiny_kafka::producer::KafkaProducer;
    /// let mut producer = KafkaProducer::new("localhost:9092", None);
    ///
    /// // Update producer configurations
    /// let mut new_configs = HashMap::new();
    /// new_configs.insert("acks", "all");
    /// producer.set_client_config(new_configs);
    /// ```
    pub fn set_client_config(&mut self, configurations: HashMap<&str, &str>) {
        let mut client_config = ClientConfig::new();

        for (key, value) in configurations {
            client_config.set(key, value);
        }

        self.producer = Arc::new(
            client_config
                .create()
                .expect("Failed to set new configuration"),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::LevelFilter;
    use simple_logger::SimpleLogger;

    #[tokio::test]
    async fn test_kafka_producer() {
        // Initialize the logger for the test
        SimpleLogger::new()
            .with_level(LevelFilter::Info)
            .init()
            .unwrap();

        // Define the Kafka configurations
        let brokers = "localhost:9092";
        let topic = "test-topic";
        let producer = KafkaProducer::new(brokers, Option::None);

        for i in 0..100 {
            let key = format!("test_key_{}", i);
            let value = format!("test_value_{}", i);
            let message = Message::new(&key, &value);

            producer.send_message(topic, message).await;
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }
}
