use crate::kafka_topic_controller::KafkaTopic;
use kube::Error;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use std::sync::Arc;
use log::{error, info};


//
pub trait KafkaTopicOps {
    async fn create_topic(&self, kafka_topic: Arc<KafkaTopic>) -> Result<(), Error>;
    async fn delete_topic(&self, kafka_topic: Arc<KafkaTopic>) -> Result<(), Error>;
    async fn topic_exists(&self, kafka_topic: Arc<KafkaTopic>) -> Result<bool, Error>;
}

pub struct KafkaAdminClient {
    pub(crate) inner_kafka_client: AdminClient<DefaultClientContext>,
}

impl KafkaTopicOps for KafkaAdminClient {


    async fn create_topic(
        &self,
        kafka_topic: Arc<KafkaTopic>,
    ) -> Result<(), Error> {
        let new_topics = vec![NewTopic::new(
            &*kafka_topic.spec.topic,
            kafka_topic.spec.partitions,
            TopicReplication::Fixed(kafka_topic.spec.replication_factor),
        )];
        let res = self.inner_kafka_client.create_topics(&new_topics, &AdminOptions::new());

        match futures::executor::block_on(res) {
            Ok(results) => {
                for r in results {
                    match r {
                        Ok(topic) => info!("Created topic: {}", topic),
                        Err((topic, err)) => println!("Failed to create topic {}: {:?}", topic, err),
                    }
                }
            }
            Err(e) => println!("Admin operation failed: {:?}", e),
        }
        Ok(())
    }

    async fn delete_topic(
        &self,
        kafka_topic: Arc<KafkaTopic>,
    ) -> Result<(), Error> {
        let delete_admin =
            &AdminOptions::new().operation_timeout(Some(std::time::Duration::from_secs(30)));

        let res = self.inner_kafka_client.delete_topics(&[&*kafka_topic.spec.topic], delete_admin);

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

    async fn topic_exists(&self, kafka_topic: Arc<KafkaTopic>) -> Result<bool, Error> {

        let res = self.inner_kafka_client.inner()
            .fetch_metadata(None, std::time::Duration::from_secs(5));

        match res {
            Ok(metadata) => {
                if metadata.topics().iter().any(|t| t.name() == kafka_topic.spec.topic) {
                    Ok(true)
                } else {
                    info!("Topic {} not found", kafka_topic.spec.topic);
                    Ok(false)
                }
            }
            Err(e) => {
                error!("Topic {} not found. Error: {}", kafka_topic.spec.topic, e);
                Ok(false) },
        }
    }
}
