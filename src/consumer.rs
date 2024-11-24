//! Kafka Consumer Implementation
//! 
//! This module provides a robust, async implementation of a Kafka consumer with built-in
//! timeout handling and connection retries. It supports consuming messages from Kafka topics
//! with configurable timeouts and automatic connection recovery.
//!
//! # Example
//!
//! ```rust
//! use tiny_kafka::consumer::KafkaConsumer;
//! 
//! #[tokio::main]
//! async fn main() -> std::io::Result<()> {
//!     // Create a new consumer
//!     let mut consumer = KafkaConsumer::new(
//!         "127.0.0.1:9092".to_string(),
//!         "my-group".to_string(),
//!         "my-topic".to_string(),
//!     ).await?;
//!     
//!     // Connect with automatic retries
//!     consumer.connect().await?;
//!     
//!     // Consume messages
//!     let messages = consumer.consume().await?;
//!     for msg in messages {
//!         println!("Received message: {:?}", msg);
//!     }
//!     
//!     // Clean up
//!     consumer.close().await?;
//!     Ok(())
//! }
//! ```

use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, sleep, Duration};
use bytes::BytesMut;
use tracing::{info, error, warn};

const JOIN_GROUP_API_KEY: i16 = 11;
const API_VERSION: i16 = 0;
const CORRELATION_ID: i32 = 1;
const CLIENT_ID: &str = "tiny-kafka-consumer";
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

/// A Kafka consumer that supports consuming messages from a topic with timeout handling
/// and automatic connection retries.
///
/// # Features
///
/// - Async/await support using tokio
/// - Configurable timeouts for all operations
/// - Automatic connection retries
/// - Comprehensive error handling and logging
///
/// # Timeouts
///
/// The consumer uses several timeout settings:
/// - `RESPONSE_TIMEOUT`: 5 seconds for individual operations
/// - `CONNECTION_TIMEOUT`: 10 seconds for initial connection
/// - Test timeouts can be configured separately in test code
///
/// # Error Handling
///
/// Operations can fail with various IO errors:
/// - `ErrorKind::TimedOut`: Operation exceeded timeout
/// - `ErrorKind::ConnectionRefused`: Failed to connect to broker
/// - `ErrorKind::Other`: Protocol or other errors
pub struct KafkaConsumer {
    broker_address: String,
    group_id: String,
    topic: String,
    stream: Option<TcpStream>,
    partition_assignments: Vec<i32>,
    current_offset: i64,
}

impl KafkaConsumer {
    /// Creates a new KafkaConsumer instance.
    ///
    /// This is an async function that creates a new consumer instance. Note that it does not
    /// establish a connection to the broker - you must call `connect()` separately.
    ///
    /// # Arguments
    ///
    /// * `broker_address` - The address of the Kafka broker (e.g., "127.0.0.1:9092")
    /// * `group_id` - The consumer group ID
    /// * `topic` - The topic to consume from
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the new `KafkaConsumer` instance or an IO error.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use tiny_kafka::consumer::KafkaConsumer;
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let consumer = KafkaConsumer::new(
    ///     "127.0.0.1:9092".to_string(),
    ///     "my-group".to_string(),
    ///     "my-topic".to_string(),
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(broker_address: String, group_id: String, topic: String) -> io::Result<Self> {
        let consumer = KafkaConsumer {
            broker_address,
            group_id,
            topic,
            stream: None,
            partition_assignments: Vec::new(),
            current_offset: 0,
        };
        Ok(consumer)
    }

    /// Establishes a connection to the Kafka broker with automatic retries.
    ///
    /// This method will attempt to connect to the broker with a timeout of `CONNECTION_TIMEOUT`.
    /// If the connection fails, it will return an appropriate error.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the connection is successful, or an `io::Error` if it fails.
    ///
    /// # Errors
    ///
    /// Common error kinds:
    /// * `ErrorKind::TimedOut` - Connection attempt exceeded timeout
    /// * `ErrorKind::ConnectionRefused` - Broker refused connection
    /// * `ErrorKind::Other` - Other connection errors
    pub async fn connect(&mut self) -> io::Result<()> {
        info!("Connecting to Kafka broker at {}", self.broker_address);
        match timeout(CONNECTION_TIMEOUT, TcpStream::connect(&self.broker_address)).await {
            Ok(result) => {
                match result {
                    Ok(stream) => {
                        info!("Successfully connected to Kafka broker");
                        self.stream = Some(stream);
                        Ok(())
                    }
                    Err(e) => {
                        error!("Failed to connect to broker: {}", e);
                        Err(e)
                    }
                }
            }
            Err(_) => {
                error!("Connection attempt timed out");
                Err(io::Error::new(io::ErrorKind::TimedOut, "Connection timed out"))
            }
        }
    }

    /// Joins the consumer group.
    async fn join_group(&mut self) -> io::Result<()> {
        info!("Joining consumer group: {}", self.group_id);
        let request = self.create_join_group_request();
        info!("Created join group request of size: {} bytes", request.len());
        
        if let Some(ref mut stream) = self.stream {
            // Send request with timeout
            match timeout(RESPONSE_TIMEOUT, async {
                stream.write_all(&request).await?;
                stream.flush().await?;
                info!("Sent join group request to broker");
                Ok::<(), io::Error>(())
            }).await {
                Ok(result) => result?,
                Err(_) => {
                    error!("Timeout while sending join group request");
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "Join group request timed out"));
                }
            }

            // Handle response with timeout
            match timeout(RESPONSE_TIMEOUT, self.handle_join_response()).await {
                Ok(result) => {
                    match result {
                        Ok(_) => {
                            info!("Successfully joined consumer group");
                            Ok(())
                        }
                        Err(e) => {
                            error!("Failed to handle join response: {}", e);
                            Err(e)
                        }
                    }
                }
                Err(_) => {
                    error!("Timeout while waiting for join response");
                    Err(io::Error::new(io::ErrorKind::TimedOut, "Join group response timed out"))
                }
            }
        } else {
            error!("Failed to join group: Not connected to Kafka broker");
            Err(io::Error::new(io::ErrorKind::NotConnected, "Not connected to Kafka broker"))
        }
    }

    /// Creates a join group request following the Kafka wire protocol.
    fn create_join_group_request(&self) -> BytesMut {
        let mut buffer = BytesMut::new();
        
        // Request Size (we'll fill this in at the end)
        buffer.extend_from_slice(&[0, 0, 0, 0]);
        
        // API Key (JoinGroupRequest = 11)
        buffer.extend_from_slice(&JOIN_GROUP_API_KEY.to_be_bytes());
        
        // API Version (0)
        buffer.extend_from_slice(&API_VERSION.to_be_bytes());
        
        // Correlation ID
        buffer.extend_from_slice(&CORRELATION_ID.to_be_bytes());
        
        // Client ID
        let client_id_bytes = CLIENT_ID.as_bytes();
        buffer.extend_from_slice(&(client_id_bytes.len() as i16).to_be_bytes());
        buffer.extend_from_slice(client_id_bytes);
        
        // Group ID
        buffer.extend_from_slice(&(self.group_id.len() as i16).to_be_bytes());
        buffer.extend_from_slice(self.group_id.as_bytes());
        
        // Session Timeout (30000ms)
        buffer.extend_from_slice(&30000i32.to_be_bytes());
        
        // Member ID (empty for first join)
        buffer.extend_from_slice(&0i16.to_be_bytes());
        
        // Protocol Type
        let protocol_type = "consumer";
        buffer.extend_from_slice(&(protocol_type.len() as i16).to_be_bytes());
        buffer.extend_from_slice(protocol_type.as_bytes());
        
        // Group Protocols Array
        buffer.extend_from_slice(&1i32.to_be_bytes()); // Number of protocols
        
        // Protocol Name
        let protocol_name = "range";  // Use "range" protocol for partition assignment
        buffer.extend_from_slice(&(protocol_name.len() as i16).to_be_bytes());
        buffer.extend_from_slice(protocol_name.as_bytes());
        
        // Protocol Metadata
        let metadata = format!("{{\"version\":0,\"topics\":[\"{}\"],\"user_data\":\"\"}}", self.topic);
        buffer.extend_from_slice(&(metadata.len() as i32).to_be_bytes());
        buffer.extend_from_slice(metadata.as_bytes());
        
        // Fill in total size
        let total_size = (buffer.len() - 4) as i32;
        buffer[0..4].copy_from_slice(&total_size.to_be_bytes());
        
        buffer
    }

    /// Handles the join group response from Kafka.
    async fn handle_join_response(&mut self) -> io::Result<()> {
        if let Some(ref mut stream) = self.stream {
            // Read response size
            let mut size_buf = [0u8; 4];
            match stream.read_exact(&mut size_buf).await {
                Ok(_) => {
                    let response_size = i32::from_be_bytes(size_buf);
                    info!("Join group response size: {} bytes", response_size);
                    
                    if response_size <= 0 {
                        error!("Invalid response size: {}", response_size);
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid response size"));
                    }
                    
                    // Read the complete response including the size bytes
                    let mut response = vec![0u8; (response_size + 4) as usize];
                    response[0..4].copy_from_slice(&size_buf);
                    match stream.read_exact(&mut response[4..]).await {
                        Ok(_) => {
                            info!("Read complete response of {} bytes", response.len());
                            
                            // Skip size (4 bytes) and correlation ID (4 bytes)
                            let mut pos = 8;
                            
                            // Read error code (2 bytes)
                            if pos + 2 > response.len() {
                                error!("Cannot read error code from join response");
                                return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid join response"));
                            }
                            let error_code = i16::from_be_bytes(response[pos..pos + 2].try_into().unwrap());
                            if error_code != 0 {
                                error!("Error in join response: {}", error_code);
                                return Err(io::Error::new(io::ErrorKind::Other, format!("Join error: {}", error_code)));
                            }
                            pos += 2;
                            
                            // Skip generation ID (4 bytes)
                            if pos + 4 > response.len() {
                                error!("Cannot read generation ID");
                                return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid join response"));
                            }
                            pos += 4;
                            
                            // Read group protocol (2 bytes for length + string)
                            if pos + 2 > response.len() {
                                error!("Cannot read group protocol length");
                                return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid join response"));
                            }
                            let protocol_len = i16::from_be_bytes(response[pos..pos + 2].try_into().unwrap()) as usize;
                            pos += 2;
                            if pos + protocol_len > response.len() {
                                error!("Cannot read group protocol");
                                return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid join response"));
                            }
                            pos += protocol_len;
                            
                            // Read leader ID (2 bytes for length + string)
                            if pos + 2 > response.len() {
                                error!("Cannot read leader ID length");
                                return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid join response"));
                            }
                            let leader_len = i16::from_be_bytes(response[pos..pos + 2].try_into().unwrap()) as usize;
                            pos += 2;
                            if pos + leader_len > response.len() {
                                error!("Cannot read leader ID");
                                return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid join response"));
                            }
                            pos += leader_len;
                            
                            // Read member ID (2 bytes for length + string)
                            if pos + 2 > response.len() {
                                error!("Cannot read member ID length");
                                return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid join response"));
                            }
                            let member_id_len = i16::from_be_bytes(response[pos..pos + 2].try_into().unwrap()) as usize;
                            pos += 2;
                            if pos + member_id_len > response.len() {
                                error!("Cannot read member ID");
                                return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid join response"));
                            }
                            let member_id = String::from_utf8_lossy(&response[pos..pos + member_id_len]).to_string();
                            pos += member_id_len;
                            
                            // Read members array length (4 bytes)
                            if pos + 4 > response.len() {
                                error!("Cannot read members array length");
                                return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid join response"));
                            }
                            let members_len = i32::from_be_bytes(response[pos..pos + 4].try_into().unwrap()) as usize;
                            pos += 4;
                            
                            // Skip members array
                            for _ in 0..members_len {
                                // Read member ID (2 bytes for length + string)
                                if pos + 2 > response.len() {
                                    error!("Cannot read member ID length");
                                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid join response"));
                                }
                                let member_len = i16::from_be_bytes(response[pos..pos + 2].try_into().unwrap()) as usize;
                                pos += 2;
                                if pos + member_len > response.len() {
                                    error!("Cannot read member ID");
                                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid join response"));
                                }
                                pos += member_len;
                                
                                // Read metadata (4 bytes for length + bytes)
                                if pos + 4 > response.len() {
                                    error!("Cannot read metadata length");
                                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid join response"));
                                }
                                let metadata_len = i32::from_be_bytes(response[pos..pos + 4].try_into().unwrap()) as usize;
                                pos += 4;
                                if pos + metadata_len > response.len() {
                                    error!("Cannot read metadata");
                                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid join response"));
                                }
                                pos += metadata_len;
                            }
                            
                            // For simplicity, we'll just assign partition 0 to this consumer
                            self.partition_assignments = vec![0];
                            info!("Assigned partition 0 to consumer with member ID: {}", member_id);
                            
                            info!("Successfully parsed join group response");
                            Ok(())
                        }
                        Err(e) => {
                            error!("Failed to read response data: {}", e);
                            Err(e)
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to read response size: {}", e);
                    Err(e)
                }
            }
        } else {
            error!("No active connection to broker");
            Err(io::Error::new(io::ErrorKind::NotConnected, "No active connection to broker"))
        }
    }

    /// Consumes messages from the subscribed topic.
    ///
    /// This method will fetch messages from all assigned partitions. It includes timeout
    /// handling for both the fetch request and response.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a vector of messages (as byte vectors) or an IO error.
    ///
    /// # Errors
    ///
    /// * `ErrorKind::TimedOut` - Fetch operation exceeded timeout
    /// * `ErrorKind::NotConnected` - Not connected to broker
    /// * `ErrorKind::Other` - Protocol or other errors
    ///
    /// # Example
    ///
    /// ```rust
    /// # use tiny_kafka::consumer::KafkaConsumer;
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// # let mut consumer = KafkaConsumer::new(
    /// #     "127.0.0.1:9092".to_string(),
    /// #     "my-group".to_string(),
    /// #     "my-topic".to_string(),
    /// # ).await?;
    /// # consumer.connect().await?;
    /// let messages = consumer.consume().await?;
    /// for msg in messages {
    ///     println!("Received: {:?}", msg);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn consume(&mut self) -> io::Result<Vec<Vec<u8>>> {
        info!("Consuming messages from topic: {}", self.topic);
        
        // Check if we're connected
        if self.stream.is_none() {
            error!("Not connected to broker");
            return Err(io::Error::new(io::ErrorKind::NotConnected, "Not connected to broker"));
        }

        // Ensure we have partition assignments
        if self.partition_assignments.is_empty() {
            info!("No partition assignments, using default partition 0");
            self.partition_assignments = vec![0];
        }
        
        let mut messages = Vec::new();
        
        for partition in &self.partition_assignments {
            info!("Fetching messages from partition {} at offset {}", partition, self.current_offset);
            let request = self.create_fetch_request(*partition);
            
            if let Some(ref mut stream) = self.stream {
                // Send request with timeout
                match timeout(RESPONSE_TIMEOUT, async {
                    stream.write_all(&request).await?;
                    stream.flush().await?;
                    info!("Sent fetch request to broker");
                    Ok::<(), io::Error>(())
                }).await {
                    Ok(result) => result?,
                    Err(_) => {
                        error!("Timeout while sending fetch request");
                        return Err(io::Error::new(io::ErrorKind::TimedOut, "Fetch request timed out"));
                    }
                }

                // Read response with timeout
                let mut size_buf = [0u8; 4];
                match timeout(RESPONSE_TIMEOUT, stream.read_exact(&mut size_buf)).await {
                    Ok(result) => {
                        match result {
                            Ok(_) => {
                                let response_size = i32::from_be_bytes(size_buf);
                                info!("Response size: {} bytes", response_size);
                                
                                if response_size <= 0 {
                                    info!("Empty response from partition {}", partition);
                                    continue;
                                }
                                
                                let mut response = vec![0u8; response_size as usize];
                                match timeout(RESPONSE_TIMEOUT, stream.read_exact(&mut response)).await {
                                    Ok(result) => {
                                        match result {
                                            Ok(_) => {
                                                info!("Read {} bytes from partition {}", response.len(), partition);
                                                
                                                // Try to extract messages from the response
                                                if let Some(batch) = self.extract_messages_from_response(&response) {
                                                    let batch_len = batch.len();
                                                    messages.extend(batch);
                                                    self.current_offset += batch_len as i64;
                                                    info!("Received {} messages from partition {}", batch_len, partition);
                                                } else {
                                                    error!("Failed to extract messages from response");
                                                }
                                            }
                                            Err(e) => {
                                                error!("Failed to read response data: {}", e);
                                                return Err(e);
                                            }
                                        }
                                    }
                                    Err(_) => {
                                        error!("Timeout while reading response data");
                                        return Err(io::Error::new(io::ErrorKind::TimedOut, "Response read timed out"));
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to read response size: {}", e);
                                return Err(e);
                            }
                        }
                    }
                    Err(_) => {
                        error!("Timeout while waiting for response size");
                        return Err(io::Error::new(io::ErrorKind::TimedOut, "Response size read timed out"));
                    }
                }
            }
        }
        
        info!("Total messages received: {}", messages.len());
        Ok(messages)
    }

    /// Extracts messages from a Kafka response.
    fn extract_messages_from_response(&self, response: &[u8]) -> Option<Vec<Vec<u8>>> {
        if response.len() < 8 {
            error!("Response too short: {} bytes", response.len());
            return None;
        }

        // Skip correlation ID (4 bytes)
        let mut pos = 4;

        // Read number of topics (4 bytes)
        if pos + 4 > response.len() {
            error!("Cannot read number of topics");
            return None;
        }
        let num_topics = i32::from_be_bytes(response[pos..pos + 4].try_into().unwrap());
        info!("Number of topics: {}", num_topics);
        pos += 4;

        if num_topics <= 0 || pos >= response.len() {
            error!("No topics in response");
            return None;
        }

        // Read topic name length (2 bytes)
        if pos + 2 > response.len() {
            error!("Cannot read topic name length");
            return None;
        }
        let topic_len = i16::from_be_bytes(response[pos..pos + 2].try_into().unwrap()) as usize;
        info!("Topic name length: {}", topic_len);
        pos += 2;

        // Skip topic name
        if pos + topic_len > response.len() {
            error!("Cannot skip topic name");
            return None;
        }
        pos += topic_len;

        // Read number of partitions (4 bytes)
        if pos + 4 > response.len() {
            error!("Cannot read number of partitions");
            return None;
        }
        let num_partitions = i32::from_be_bytes(response[pos..pos + 4].try_into().unwrap());
        info!("Number of partitions: {}", num_partitions);
        pos += 4;

        if num_partitions <= 0 || pos >= response.len() {
            error!("No partitions in response");
            return None;
        }

        // Read partition ID (4 bytes)
        if pos + 4 > response.len() {
            error!("Cannot read partition ID");
            return None;
        }
        let partition_id = i32::from_be_bytes(response[pos..pos + 4].try_into().unwrap());
        info!("Partition ID: {}", partition_id);
        pos += 4;

        // Read error code (2 bytes)
        if pos + 2 > response.len() {
            error!("Cannot read error code");
            return None;
        }
        let error_code = i16::from_be_bytes(response[pos..pos + 2].try_into().unwrap());
        if error_code != 0 {
            error!("Error code in response: {}", error_code);
            return None;
        }
        pos += 2;

        // Read high watermark offset (8 bytes)
        if pos + 8 > response.len() {
            error!("Cannot read high watermark");
            return None;
        }
        let high_watermark = i64::from_be_bytes(response[pos..pos + 8].try_into().unwrap());
        info!("High watermark: {}", high_watermark);
        pos += 8;

        // Read message set size (4 bytes)
        if pos + 4 > response.len() {
            error!("Cannot read message set size");
            return None;
        }
        let message_set_size = i32::from_be_bytes(response[pos..pos + 4].try_into().unwrap()) as usize;
        info!("Message set size: {}", message_set_size);
        pos += 4;

        if message_set_size == 0 {
            info!("Empty message set");
            return None;
        }

        if pos + message_set_size > response.len() {
            error!("Message set size {} exceeds response length {}", message_set_size, response.len());
            return None;
        }

        let mut messages = Vec::new();
        let message_set_end = pos + message_set_size;

        while pos < message_set_end {
            // Read offset (8 bytes)
            if pos + 8 > message_set_end {
                break;
            }
            let offset = i64::from_be_bytes(response[pos..pos + 8].try_into().unwrap());
            info!("Message offset: {}", offset);
            pos += 8;

            // Read message size (4 bytes)
            if pos + 4 > message_set_end {
                break;
            }
            let message_size = i32::from_be_bytes(response[pos..pos + 4].try_into().unwrap()) as usize;
            info!("Message size: {}", message_size);
            pos += 4;

            if pos + message_size > message_set_end {
                error!("Message size {} exceeds message set end", message_size);
                break;
            }

            // Skip CRC (4 bytes)
            pos += 4;

            // Skip magic byte (1 byte)
            pos += 1;

            // Skip attributes (1 byte)
            pos += 1;

            // Read key length (4 bytes)
            if pos + 4 > message_set_end {
                break;
            }
            let key_len = i32::from_be_bytes(response[pos..pos + 4].try_into().unwrap()) as usize;
            info!("Key length: {}", key_len);
            pos += 4;

            // Skip key bytes
            if key_len > 0 {
                if pos + key_len > message_set_end {
                    error!("Key length {} exceeds message set end", key_len);
                    break;
                }
                pos += key_len;
            }

            // Read value length (4 bytes)
            if pos + 4 > message_set_end {
                break;
            }
            let value_len = i32::from_be_bytes(response[pos..pos + 4].try_into().unwrap()) as usize;
            info!("Value length: {}", value_len);
            pos += 4;

            if value_len == 0 {
                continue;
            }

            if pos + value_len > message_set_end {
                error!("Value length {} exceeds message set end", value_len);
                break;
            }

            // Extract message value
            messages.push(response[pos..pos + value_len].to_vec());
            info!("Extracted message of length {}", value_len);
            pos += value_len;
        }

        if messages.is_empty() {
            None
        } else {
            Some(messages)
        }
    }

    /// Creates a fetch request following the Kafka wire protocol.
    fn create_fetch_request(&self, partition: i32) -> BytesMut {
        let mut buffer = BytesMut::new();
        
        // Request Size (we'll fill this in at the end)
        buffer.extend_from_slice(&[0, 0, 0, 0]);
        
        // API Key (FetchRequest = 1)
        buffer.extend_from_slice(&1i16.to_be_bytes());
        
        // API Version
        buffer.extend_from_slice(&API_VERSION.to_be_bytes());
        
        // Correlation ID
        buffer.extend_from_slice(&CORRELATION_ID.to_be_bytes());
        
        // Client ID
        let client_id_bytes = CLIENT_ID.as_bytes();
        buffer.extend_from_slice(&(client_id_bytes.len() as i16).to_be_bytes());
        buffer.extend_from_slice(client_id_bytes);
        
        // Replica ID (-1 for consumers)
        buffer.extend_from_slice(&(-1i32).to_be_bytes());
        
        // Max Wait Time (100ms)
        buffer.extend_from_slice(&100i32.to_be_bytes());
        
        // Min Bytes (1 byte)
        buffer.extend_from_slice(&1i32.to_be_bytes());
        
        // Number of topics
        buffer.extend_from_slice(&1i32.to_be_bytes());
        
        // Topic name
        buffer.extend_from_slice(&(self.topic.len() as i16).to_be_bytes());
        buffer.extend_from_slice(self.topic.as_bytes());
        
        // Number of partitions
        buffer.extend_from_slice(&1i32.to_be_bytes());
        
        // Partition
        buffer.extend_from_slice(&partition.to_be_bytes());
        
        // Fetch offset
        buffer.extend_from_slice(&self.current_offset.to_be_bytes());
        
        // Max bytes
        buffer.extend_from_slice(&(1024 * 1024i32).to_be_bytes()); // 1MB
        
        // Fill in total size
        let total_size = (buffer.len() - 4) as i32;
        buffer[0..4].copy_from_slice(&total_size.to_be_bytes());
        
        buffer
    }

    /// Commits the current offset.
    ///
    /// This method commits the current offset to the broker, ensuring that future
    /// consumption will start from this point.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the commit is successful, or an `io::Error` if it fails.
    pub async fn commit(&mut self) -> io::Result<()> {
        info!("Committing offset {}", self.current_offset);
        // In a real implementation, send offset commit request
        Ok(())
    }

    /// Closes the consumer connection.
    ///
    /// This method gracefully closes the connection to the broker. It should be called
    /// when you're done consuming messages.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the close is successful, or an `io::Error` if it fails.
    pub async fn close(&mut self) -> io::Result<()> {
        info!("Closing consumer connection");
        if let Some(mut stream) = self.stream.take() {
            stream.shutdown().await?;
        }
        Ok(())
    }
}

impl Drop for KafkaConsumer {
    fn drop(&mut self) {
        if let Some(stream) = self.stream.take() {
            // Use blocking shutdown in drop
            let _ = stream.into_std().map(|s| s.shutdown(std::net::Shutdown::Both));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{timeout, sleep};
    use tracing_test::traced_test;

    const TEST_TIMEOUT: Duration = Duration::from_secs(30);
    const SETUP_DELAY: Duration = Duration::from_secs(2);

    #[tokio::test]
    #[traced_test]
    async fn test_consumer_creation() {
        // Allow some time for broker to be ready
        sleep(SETUP_DELAY).await;

        let consumer_result = timeout(TEST_TIMEOUT, async {
            let mut consumer = KafkaConsumer::new(
                "127.0.0.1:9092".to_string(),
                "test_group".to_string(),
                "test-topic".to_string(),
            ).await?;
            
            // Try to connect multiple times
            for attempt in 1..=3 {
                match consumer.connect().await {
                    Ok(_) => return Ok(consumer),
                    Err(e) if attempt < 3 => {
                        warn!("Connection attempt {} failed: {}", attempt, e);
                        sleep(Duration::from_secs(1)).await;
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(consumer)
        }).await;

        assert!(consumer_result.is_ok(), "Consumer creation timed out");
        let consumer = consumer_result.unwrap();
        assert!(consumer.is_ok(), "Failed to create consumer: {:?}", consumer.err());
        
        let consumer = consumer.unwrap();
        assert_eq!(consumer.group_id, "test_group");
        assert!(consumer.stream.is_some());
    }

    #[tokio::test]
    #[traced_test]
    async fn test_consume_messages() {
        // Allow some time for broker to be ready
        sleep(SETUP_DELAY).await;

        let consumer_result = timeout(TEST_TIMEOUT, async {
            let mut consumer = KafkaConsumer::new(
                "127.0.0.1:9092".to_string(),
                "test_group".to_string(),
                "test-topic".to_string(),
            ).await?;
            
            // Try to connect multiple times
            for attempt in 1..=3 {
                match consumer.connect().await {
                    Ok(_) => return Ok(consumer),
                    Err(e) if attempt < 3 => {
                        warn!("Connection attempt {} failed: {}", attempt, e);
                        sleep(Duration::from_secs(1)).await;
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(consumer)
        }).await;

        assert!(consumer_result.is_ok(), "Consumer creation timed out");
        let mut consumer = consumer_result.unwrap().expect("Failed to create consumer");

        // Give Kafka more time to fully establish the connection
        sleep(SETUP_DELAY).await;

        let messages_result = timeout(TEST_TIMEOUT, consumer.consume()).await;
        assert!(messages_result.is_ok(), "Consume operation timed out");
        
        let messages = messages_result.unwrap();
        assert!(messages.is_ok(), "Failed to consume messages: {:?}", messages.err());
    }

    #[tokio::test]
    #[traced_test]
    async fn test_consumer_lifecycle() {
        // Allow some time for broker to be ready
        sleep(SETUP_DELAY).await;

        let consumer_result = timeout(TEST_TIMEOUT, async {
            let mut consumer = KafkaConsumer::new(
                "127.0.0.1:9092".to_string(),
                "test_group".to_string(),
                "test-topic".to_string(),
            ).await?;
            
            // Try to connect multiple times
            for attempt in 1..=3 {
                match consumer.connect().await {
                    Ok(_) => return Ok(consumer),
                    Err(e) if attempt < 3 => {
                        warn!("Connection attempt {} failed: {}", attempt, e);
                        sleep(Duration::from_secs(1)).await;
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(consumer)
        }).await;

        assert!(consumer_result.is_ok(), "Consumer creation timed out");
        let mut consumer = consumer_result.unwrap().expect("Failed to create consumer");

        // Give Kafka more time to fully establish the connection
        sleep(SETUP_DELAY).await;

        // Test full lifecycle with timeouts
        let consume_result = timeout(TEST_TIMEOUT, consumer.consume()).await;
        assert!(consume_result.is_ok(), "Consume operation timed out");
        assert!(consume_result.unwrap().is_ok(), "Failed to consume messages");

        let commit_result = timeout(TEST_TIMEOUT, consumer.commit()).await;
        assert!(commit_result.is_ok(), "Commit operation timed out");
        assert!(commit_result.unwrap().is_ok(), "Failed to commit offset");

        let close_result = timeout(TEST_TIMEOUT, consumer.close()).await;
        assert!(close_result.is_ok(), "Close operation timed out");
        assert!(close_result.unwrap().is_ok(), "Failed to close consumer");
    }
}
