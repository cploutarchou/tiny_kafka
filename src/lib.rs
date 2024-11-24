pub mod consumer;
pub mod producer;
pub mod error;

pub use consumer::KafkaConsumer;
pub use producer::KafkaProducer;
pub use error::KafkaError;

pub type Result<T> = std::result::Result<T, KafkaError>;
