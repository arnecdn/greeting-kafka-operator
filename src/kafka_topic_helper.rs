use crate::kafka_topic_controller::KafkaTopic;


use log::{error};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::KafkaError;
use std::sync::Arc;
use std::time::Duration;

//
pub trait KafkaTopicOps {
    async fn create_topic(&self, kafka_topic: Arc<KafkaTopic>) -> Result<(), KafkaError>;
    async fn delete_topic(&self, kafka_topic: Arc<KafkaTopic>) -> Result<(), KafkaError>;
    async fn topic_exists(&self, kafka_topic: Arc<KafkaTopic>) -> Result<bool, KafkaError>;
}

pub struct KafkaAdminClient {
    pub(crate) inner_kafka_client: AdminClient<DefaultClientContext>,
}

impl KafkaTopicOps for KafkaAdminClient {
    async fn create_topic(&self, kafka_topic: Arc<KafkaTopic>) -> Result<(), KafkaError> {
        let new_topics = vec![NewTopic::new(
            &*kafka_topic.spec.topic,
            kafka_topic.spec.partitions,
            TopicReplication::Fixed(kafka_topic.spec.replication_factor),
        )];
        let options = AdminOptions::new()
            .operation_timeout(Some(Duration::from_secs(10)))
            .request_timeout(Some(Duration::from_secs(10)));
        let res = self
            .inner_kafka_client
            .create_topics(&new_topics, &options)
            .await?;

        match res.into_iter().next() {
            Some(Ok(_)) => Ok(()),
            Some(Err((topic, err))) => {
                error!("Failed to create topic {}: {:?}", topic, err);
                Err(KafkaError::AdminOp(err))
            }
            None => Ok(()),
        }
    }

    async fn delete_topic(&self, kafka_topic: Arc<KafkaTopic>) -> Result<(), KafkaError> {
        let options = AdminOptions::new()
            .operation_timeout(Some(Duration::from_secs(10)))
            .request_timeout(Some(Duration::from_secs(10)));
        let res = self
            .inner_kafka_client
            .delete_topics(&[&*kafka_topic.spec.topic], &options)
            .await?;

        match res.into_iter().next() {
            Some(Ok(_)) => Ok(()),
            Some(Err((topic, err))) => {
                error!("Failed to delete topic {}: {:?}", topic, err);
                Err(KafkaError::AdminOp(err))
            }
            None => Ok(()),
        }
    }

    async fn topic_exists(&self, kafka_topic: Arc<KafkaTopic>) -> Result<bool, KafkaError> {
        let res = self
            .inner_kafka_client
            .inner()
            .fetch_metadata(Some(&kafka_topic.spec.topic), Duration::from_secs(10));

        match res {
            Ok(metadata) => {
                if metadata
                    .topics()
                    .iter()
                    .any(|t| t.name() == kafka_topic.spec.topic)
                {
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Err(e) => {
                error!(
                    "Failed to fetch Kafka metadata for topic {}. KafkaError: {}",
                    kafka_topic.spec.topic, e
                );
                Err(e)
            }
        }
    }
}
