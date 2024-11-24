use std::time::Duration;
use tiny_kafka::{KafkaConsumer as Consumer, KafkaProducer as Producer};
use tokio;
use tracing_test::traced_test;

async fn setup_kafka() {
    // Wait for Kafka to be ready
    tokio::time::sleep(Duration::from_secs(2)).await;
}

#[tokio::test]
#[traced_test]
async fn test_producer_consumer() {
    setup_kafka().await;

    let topic = "test_topic";
    let mut producer = Producer::new("localhost:9092".to_string())
        .await
        .expect("Failed to create producer");
    let mut consumer = Consumer::new(
        "localhost:9092".to_string(),
        "test_group".to_string(),
        topic.to_string(),
    )
    .await
    .expect("Failed to create consumer");

    let test_message = "test message";

    // Produce message
    producer
        .send_message(topic, 0, test_message)
        .await
        .expect("Failed to send message");

    // Consume message
    let messages = consumer
        .consume()
        .await
        .expect("Failed to consume messages");

    // Verify message was received
    assert!(!messages.is_empty());
    assert_eq!(String::from_utf8_lossy(&messages[0]), test_message);

    // Clean up
    consumer.close().await.expect("Failed to close consumer");
}

#[tokio::test]
#[traced_test]
async fn test_multiple_messages() {
    setup_kafka().await;

    let topic = "test_topic_multiple";
    let mut producer = Producer::new("localhost:9092".to_string())
        .await
        .expect("Failed to create producer");
    let mut consumer = Consumer::new(
        "localhost:9092".to_string(),
        "test_group_multiple".to_string(),
        topic.to_string(),
    )
    .await
    .expect("Failed to create consumer");

    let test_messages = vec!["message1", "message2", "message3"];

    // Produce messages
    for msg in &test_messages {
        producer
            .send_message(topic, 0, msg)
            .await
            .expect("Failed to send message");
    }

    // Consume messages
    let messages = consumer
        .consume()
        .await
        .expect("Failed to consume messages");

    assert_eq!(messages.len(), test_messages.len());
    for (received, original) in messages.iter().zip(test_messages.iter()) {
        assert_eq!(String::from_utf8_lossy(&received), original.to_string());
    }

    // Clean up
    consumer.close().await.expect("Failed to close consumer");
}

#[tokio::test]
#[traced_test]
async fn test_empty_topic() {
    setup_kafka().await;

    let topic = "empty_topic";
    let mut consumer = Consumer::new(
        "localhost:9092".to_string(),
        "test_group_empty".to_string(),
        topic.to_string(),
    )
    .await
    .expect("Failed to create consumer");

    // Try to consume from empty topic
    let messages = consumer
        .consume()
        .await
        .expect("Failed to consume messages");

    assert!(messages.is_empty());

    // Clean up
    consumer.close().await.expect("Failed to close consumer");
}
