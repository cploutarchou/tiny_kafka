use thiserror::Error;

#[derive(Error, Debug)]
pub enum KafkaError {
    #[error("Failed to connect to broker: {0}")]
    ConnectionError(String),

    #[error("Failed to join consumer group: {0}")]
    GroupJoinError(String),

    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Timeout error: {0}")]
    TimeoutError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Broker error: code={code}, message={message}")]
    BrokerError {
        code: i16,
        message: String,
    },

    #[error("Offset out of range for partition {partition} in topic {topic}")]
    OffsetOutOfRange {
        topic: String,
        partition: i32,
    },

    #[error("Unknown error: {0}")]
    Unknown(String),
}
