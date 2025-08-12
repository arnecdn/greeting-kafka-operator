
use kube::{Error};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::ClientConfig;
use std::sync::Arc;
use crate::kafka_topic_controller::KafkaTopic;

pub async fn create_topic(kafka_topic: Arc<KafkaTopic>) -> Result<(), Error> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set(
            "bootstrap.servers",
            kafka_topic.spec.bootstrap_server.clone(),
        )
        .create()
        .expect("Admin client creation failed");

    let new_topics = vec![NewTopic::new(
        &*kafka_topic.spec.topic,
        kafka_topic.spec.partitions,
        TopicReplication::Fixed(kafka_topic.spec.replication_factor),
    )];
    let res = admin.create_topics(&new_topics, &AdminOptions::new());

    match futures::executor::block_on(res) {
        Ok(results) => {
            for r in results {
                match r {
                    Ok(topic) => println!("Created topic: {}", topic),
                    Err((topic, err)) => println!("Failed to create topic {}: {:?}", topic, err),
                }
            }
        }
        Err(e) => println!("Admin operation failed: {:?}", e),
    }
    Ok(())
}

pub async fn delete_topic(kafka_topic: Arc<KafkaTopic>) -> Result<(), Error> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set(
            "bootstrap.servers",
            kafka_topic.spec.bootstrap_server.clone(),
        )
        .create()
        .expect("Admin client creation failed");

    let delete_admin =
        &AdminOptions::new().operation_timeout(Some(std::time::Duration::from_secs(30)));

    let res = admin.delete_topics(&[&*kafka_topic.spec.topic], delete_admin);

    match futures::executor::block_on(res) {
        Ok(results) => {
            for r in results {
                match r {
                    Ok(topic) => println!("Deleted topic: {}", topic),
                    Err((topic, err)) => println!("Failed to create topic {}: {:?}", topic, err),
                }
            }
        }
        Err(e) => println!("Admin operation failed: {:?}", e),
    }
    Ok(())
}
