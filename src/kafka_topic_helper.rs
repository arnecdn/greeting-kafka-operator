use crate::kafka_topic_controller::KafkaTopic;
use kube::Error;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use std::sync::Arc;

//
pub trait KafkaTopicOps {
    async fn create_topic(&self, kafka_topic: Arc<KafkaTopic>) -> Result<(), Error>;
    async fn delete_topic(&self, kafka_topic: Arc<KafkaTopic>) -> Result<(), Error>;
}

pub struct KafkaAdminClient {
    pub(crate) admin: AdminClient<DefaultClientContext>,
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
        let res = self.admin.create_topics(&new_topics, &AdminOptions::new());

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

    async fn delete_topic(
        &self,
        kafka_topic: Arc<KafkaTopic>,
    ) -> Result<(), Error> {
        let delete_admin =
            &AdminOptions::new().operation_timeout(Some(std::time::Duration::from_secs(30)));

        let res = self.admin.delete_topics(&[&*kafka_topic.spec.topic], delete_admin);

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
}
