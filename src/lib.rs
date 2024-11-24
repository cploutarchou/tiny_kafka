pub mod consumer;
pub mod error;
pub mod producer;

pub use consumer::KafkaConsumer;
pub use error::KafkaError;
pub use producer::KafkaProducer;

pub type Result<T> = std::result::Result<T, KafkaError>;
