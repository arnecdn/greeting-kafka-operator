use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec};
use k8s_openapi::api::core::v1::{Container, ContainerPort, PodSpec, PodTemplateSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use kube::api::{DeleteParams, Patch, PatchParams, PostParams};
use kube::{Api, Client, CustomResource, Error};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::ClientConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(CustomResource, Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[kube(
    group = "arnecdn.github.com",
    version = "v1",
    kind = "KafkaTopic",
    plural = "kafkatopics",
    derive = "PartialEq",
    namespaced
)]
pub struct KafkaTopicSpec {
    pub bootstrapServer: String,
    pub topic: String,
    pub partitions: i32,
    pub replicationFactor: i32,
}

pub async fn create_topic(kafka_topic: Arc<KafkaTopic>) -> Result<(), Error> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set(
            "bootstrap.servers",
            kafka_topic.spec.bootstrapServer.clone(),
        )
        .create()
        .expect("Admin client creation failed");

    let new_topics = vec![NewTopic::new(
        &*kafka_topic.spec.topic,
        kafka_topic.spec.partitions,
        TopicReplication::Fixed(kafka_topic.spec.replicationFactor),
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
            kafka_topic.spec.bootstrapServer.clone(),
        )
        .create()
        .expect("Admin client creation failed");

    let deleteAdmin =
        &AdminOptions::new().operation_timeout(Some(std::time::Duration::from_secs(30)));

    let res = admin.delete_topics(&[&*kafka_topic.spec.topic], deleteAdmin);

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

pub async fn add_finalizer(
    client: Client,
    name: &str,
    namespace: &str,
) -> Result<KafkaTopic, Error> {
    let api: Api<KafkaTopic> = Api::namespaced(client, namespace);
    let finalizer: Value = json!({
        "metadata": {
            "finalizers": ["arnecdn.github.com/finalizer"]
        }
    });

    let patch: Patch<&Value> = Patch::Merge(&finalizer);
    api.patch(name, &PatchParams::default(), &patch).await
}

pub async fn delete_finalizer(
    client: Client,
    name: &str,
    namespace: &str,
) -> Result<KafkaTopic, Error> {
    let api: Api<KafkaTopic> = Api::namespaced(client, namespace);
    let finalizer: Value = json!({
        "metadata": {
            "finalizers": null
        }
    });

    let patch: Patch<&Value> = Patch::Merge(&finalizer);
    api.patch(name, &PatchParams::default(), &patch).await
}
