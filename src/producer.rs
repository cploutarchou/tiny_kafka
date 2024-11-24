use bytes::BytesMut;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{error, info};

const PRODUCE_API_KEY: i16 = 0;
const PRODUCE_API_VERSION: i16 = 0;
const CORRELATION_ID: i32 = 1;
const CLIENT_ID: &str = "tiny-kafka-producer";

pub struct KafkaProducer {
    broker_address: String,
    stream: Option<TcpStream>,
}

impl KafkaProducer {
    /// Create a new KafkaProducer instance.
    pub async fn new(broker_address: String) -> io::Result<Self> {
        let mut producer = KafkaProducer {
            broker_address,
            stream: None,
        };
        producer.connect().await?;
        Ok(producer)
    }

    /// Establish a connection to the Kafka broker.
    async fn connect(&mut self) -> io::Result<()> {
        info!("Connecting to Kafka broker at {}", self.broker_address);
        let stream = TcpStream::connect(&self.broker_address).await?;
        self.stream = Some(stream);
        info!("Successfully connected to Kafka broker");
        Ok(())
    }

    /// Send a message to the specified topic and partition.
    pub async fn send_message(
        &mut self,
        topic: &str,
        partition: i32,
        message: &str,
    ) -> io::Result<()> {
        info!(
            "Sending message to topic: {}, partition: {}",
            topic, partition
        );
        let request = self.create_produce_request(topic, partition, message);
        if let Some(ref mut stream) = self.stream {
            stream.write_all(&request).await?;
            stream.flush().await?;
            self.receive_response().await?;
            info!("Message sent successfully");
        } else {
            error!("Failed to send message: Not connected to Kafka broker");
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "Not connected to Kafka broker",
            ));
        }
        Ok(())
    }

    /// Create a Produce request for Kafka following the wire protocol.
    fn create_produce_request(&self, topic: &str, partition: i32, message: &str) -> BytesMut {
        let mut buffer = BytesMut::new();

        // Request Size (we'll fill this in at the end)
        buffer.extend_from_slice(&[0, 0, 0, 0]);

        // API Key (ProduceRequest = 0)
        buffer.extend_from_slice(&PRODUCE_API_KEY.to_be_bytes());

        // API Version
        buffer.extend_from_slice(&PRODUCE_API_VERSION.to_be_bytes());

        // Correlation ID
        buffer.extend_from_slice(&CORRELATION_ID.to_be_bytes());

        // Client ID
        let client_id_bytes = CLIENT_ID.as_bytes();
        buffer.extend_from_slice(&(client_id_bytes.len() as i16).to_be_bytes());
        buffer.extend_from_slice(client_id_bytes);

        // Required Acks (1 = leader only)
        buffer.extend_from_slice(&1i16.to_be_bytes());

        // Timeout (1000ms)
        buffer.extend_from_slice(&1000i32.to_be_bytes());

        // Number of topic data
        buffer.extend_from_slice(&1i32.to_be_bytes());

        // Topic name
        buffer.extend_from_slice(&(topic.len() as i16).to_be_bytes());
        buffer.extend_from_slice(topic.as_bytes());

        // Number of partitions
        buffer.extend_from_slice(&1i32.to_be_bytes());

        // Partition
        buffer.extend_from_slice(&partition.to_be_bytes());

        // Message set size (we'll fill this in later)
        let message_set_size_pos = buffer.len();
        buffer.extend_from_slice(&[0, 0, 0, 0]);

        // Message
        let message_bytes = message.as_bytes();

        // Message size
        buffer.extend_from_slice(&(message_bytes.len() as i32).to_be_bytes());

        // Message magic byte (version)
        buffer.extend_from_slice(&[0]);

        // Message attributes
        buffer.extend_from_slice(&[0]);

        // Message key (null)
        buffer.extend_from_slice(&(-1i32).to_be_bytes());

        // Message value
        buffer.extend_from_slice(&(message_bytes.len() as i32).to_be_bytes());
        buffer.extend_from_slice(message_bytes);

        // Fill in message set size
        let message_set_size = (buffer.len() - message_set_size_pos - 4) as i32;
        buffer[message_set_size_pos..message_set_size_pos + 4]
            .copy_from_slice(&message_set_size.to_be_bytes());

        // Fill in total size
        let total_size = (buffer.len() - 4) as i32;
        buffer[0..4].copy_from_slice(&total_size.to_be_bytes());

        buffer
    }

    /// Receive and handle the response from Kafka.
    async fn receive_response(&mut self) -> io::Result<()> {
        let mut size_buf = [0u8; 4];
        if let Some(ref mut stream) = self.stream {
            // Read response size
            stream.read_exact(&mut size_buf).await?;
            let response_size = i32::from_be_bytes(size_buf);

            // Read the rest of the response
            let mut response = vec![0u8; response_size as usize];
            stream.read_exact(&mut response).await?;

            // In a real implementation, we would parse the response here
            info!("Received response from Kafka broker");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn test_new_producer_success() {
        let addr = "127.0.0.1:9092".to_string();
        let producer = KafkaProducer::new(addr)
            .await
            .expect("Failed to create producer");
        assert!(producer.stream.is_some());
        assert!(logs_contain("Successfully connected to Kafka broker"));
    }

    #[tokio::test]
    #[traced_test]
    async fn test_send_message() {
        let mut producer = KafkaProducer::new("127.0.0.1:9092".to_string())
            .await
            .expect("Failed to create producer");

        // Give Kafka a moment to fully establish the connection
        tokio::time::sleep(Duration::from_secs(1)).await;

        let topic = "test-topic";
        let partition = 0;
        let message = "Hello Kafka!";

        let result = producer.send_message(topic, partition, message).await;
        match result {
            Ok(_) => {
                assert!(logs_contain("Message sent successfully"));
            }
            Err(e) => {
                panic!(
                    "Failed to send message: {:?}\nLogs: {}",
                    e,
                    std::panic::Location::caller()
                );
            }
        }
    }
}
