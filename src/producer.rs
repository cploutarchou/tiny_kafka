use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use bytes::BytesMut;
use tokio::test; // Ensure to import the macro correctly

struct KafkaProducer {
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
        let stream = TcpStream::connect(&self.broker_address).await?;
        self.stream = Some(stream);
        Ok(())
    }

    /// Send a message to the specified topic and partition.
    pub async fn send_message(&mut self, topic: &str, partition: i32, message: &str) -> io::Result<()> {
        let request = self.create_produce_request(topic, partition, message);
        if let Some(ref mut stream) = self.stream {
            stream.write_all(&request).await?;
            stream.flush().await?;
            self.receive_response().await?;
        } else {
            return Err(io::Error::new(io::ErrorKind::NotConnected, "Not connected to Kafka broker"));
        }
        Ok(())
    }

    /// Create a Produce request for Kafka.
    fn create_produce_request(&self, topic: &str, partition: i32, message: &str) -> BytesMut {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(topic.as_bytes());
        buffer.extend_from_slice(&partition.to_be_bytes());
        buffer.extend_from_slice(message.as_bytes());
        buffer
    }

    /// Receive and handle the response from Kafka.
    async fn receive_response(&mut self) -> io::Result<()> {
        let mut response = vec![0; 1024]; // Fixed size for demonstration
        if let Some(ref mut stream) = self.stream {
            stream.read_exact(&mut response).await?;
            println!("Received response: {:?}", response);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::test;

    #[tokio::test]
    async fn test_new_producer_success() {
        let addr = "127.0.0.1:9092".to_string();
        let producer = KafkaProducer::new(addr).await;
        assert!(producer.is_ok());
    }

    #[tokio::test]
    async fn test_send_message() {
        let client_stream = TcpStream::connect("127.0.0.1:9092").await.expect("Failed to connect for test");
        let mut producer = KafkaProducer {
            broker_address: "127.0.0.1:9092".to_string(),
            stream: Some(client_stream),
        };

        let topic = "test-topic";
        let partition = 0;
        let message = "Hello Kafka!";

        let result = producer.send_message(topic, partition, message).await;
        assert!(result.is_ok());
    }
}
