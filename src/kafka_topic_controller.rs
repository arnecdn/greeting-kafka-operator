
use crate::kafka_topic_helper::{KafkaTopicOps};
use kube::api::{Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Api, CustomResource, Resource, ResourceExt};
use kube_client::Client;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

#[derive(CustomResource, Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[kube(
    group = "arnecdn.github.com",
    version = "v1",
    kind = "KafkaTopic",
    plural = "kafkatopics",
    derive = "PartialEq",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicSpec {
    pub bootstrap_server: String,
    pub topic: String,
    pub partitions: i32,
    pub replication_factor: i32,
}

pub trait KubeClientCrdOps {
    async fn add_finalizer(&self, name: &str, namespace: &str) -> Result<KafkaTopic, kube::Error>;

    async fn delete_finalizer(
        &self,
        name: &str,
        namespace: &str,
    ) -> Result<KafkaTopic, kube::Error>;
}

pub(crate) struct KubeClient {
    pub(crate) client: Client,
}

impl KubeClientCrdOps for KubeClient {
    async fn add_finalizer(&self, name: &str, namespace: &str) -> Result<KafkaTopic, kube::Error> {
        let api: Api<KafkaTopic> = Api::namespaced(self.client.clone(), namespace);
        let finalizer: Value = json!({
            "metadata": {
                "finalizers": ["arnecdn.github.com/finalizer"]
            }
        });

        let patch: Patch<&Value> = Patch::Merge(&finalizer);
        api.patch(name, &PatchParams::default(), &patch).await
    }

    async fn delete_finalizer(
        &self,
        name: &str,
        namespace: &str,
    ) -> Result<KafkaTopic, kube::Error> {
        let api: Api<KafkaTopic> = Api::namespaced(self.client.clone(), namespace);
        let finalizer: Value = json!({
            "metadata": {
                "finalizers": null
            }
        });

        let patch: Patch<&Value> = Patch::Merge(&finalizer);
        api.patch(name, &PatchParams::default(), &patch).await
    }
}

pub(crate) struct ContextData<T: KafkaTopicOps, E: KubeClientCrdOps> {
    kafka_topic_client: T,
    kube_client: E,
}

impl<T: KafkaTopicOps, E: KubeClientCrdOps> ContextData<T, E> {
    pub fn new(kafka_topic_client: T, kube_client: E) -> Self {
        ContextData {
            kafka_topic_client,
            kube_client,
        }
    }
}

enum KafkaTopicAction {
    /// Create the subresources and Kafka topics
    Create,
    /// Delete all subresources created in the `Create` phase
    Delete,
    /// This `KafkaTopic` resource is in desired state and requires no actions to be taken
    NoOp,
}

pub async fn reconcile<T: KafkaTopicOps, E: KubeClientCrdOps>(
    kafka_topic: Arc<KafkaTopic>,
    context: Arc<ContextData<T, E>>,
) -> Result<Action, Error> {
    // The resource of `KafkaTopic` kind is required to have a namespace set. However, it is not guaranteed
    // the resource will have a `namespace` set. Therefore, the `namespace` field on object's metadata
    // is optional and Rust forces the programmer to check for it's existence first.
    let namespace: String = match kafka_topic.namespace() {
        None => {
            // If there is no namespace to deploy to defined, reconciliation ends with an error immediately.
            return Err(Error::UserInputError(
                "Expected Echo resource to be namespaced. Can't deploy to an unknown namespace."
                    .to_owned(),
            ));
        }
        // If namespace is known, proceed. In a more advanced version of the operator, perhaps
        // the namespace could be checked for existence first.
        Some(namespace) => namespace,
    };
    let name = kafka_topic.name_any(); // Name of the Echo resource is used to name the subresources as well.

    match determine_action(&kafka_topic) {
        KafkaTopicAction::Create => {
            // Creates a new CR with a Kafka Topic, but applies a finalizer first.
            // Finalizer is applied first, as the operator might be shut down and restarted
            // at any time, leaving subresources in intermediate state. This prevents leaks on
            // the `KafkaTopic` resource deletion.
            context.kube_client.add_finalizer(&name, &namespace).await?;

            context.kafka_topic_client.create_topic(kafka_topic).await?;
            Ok(Action::requeue(Duration::from_secs(10)))
        }
        KafkaTopicAction::Delete => {
            // Deletes any subresources related to this `KafkaTopic` resources. If and only if all subresources
            // are deleted, the finalizer is removed and Kubernetes is free to remove the `KafkaTopic` resource.
            context.kafka_topic_client.delete_topic(kafka_topic).await?;

            context.kube_client.delete_finalizer(&name, &namespace).await?;
            Ok(Action::await_change())
        }
        // The resource is already in desired state, do nothing and re-check after 10 seconds
        KafkaTopicAction::NoOp => Ok(Action::requeue(Duration::from_secs(10))),
    }
}

/// Resources arrives into reconciliation queue in a certain state. This function looks at
/// the state of given `KafkaTopic` resource and decides which actions needs to be performed.
/// The finite set of possible actions is represented by the `KafkaTopicAction` enum.
fn determine_action(kafka_topic: &KafkaTopic) -> KafkaTopicAction {
    if kafka_topic.meta().deletion_timestamp.is_some() {
        KafkaTopicAction::Delete
    } else if kafka_topic
        .meta()
        .finalizers
        .as_ref()
        .map_or(true, |finalizers| finalizers.is_empty())
    {
        KafkaTopicAction::Create
    } else {
        KafkaTopicAction::NoOp
    }
}

/// Actions to be taken when a reconciliation fails - for whatever reason.
/// Prints out the error to `stderr` and requeues the resource for another reconciliation.
pub(crate) fn on_error<T: KafkaTopicOps, E: KubeClientCrdOps>(
    echo: Arc<KafkaTopic>,
    error: &Error,
    _context: Arc<ContextData<T, E>>,
) -> Action {
    eprintln!("Reconciliation error:\n{:?}.\n{:?}", error, echo);
    Action::requeue(Duration::from_secs(5))
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Any error originating from the `kube-rs` crate
    #[error("Kubernetes reported error: {source}")]
    KubeError {
        #[from]
        source: kube::Error,
    },
    /// Error in user input or Echo resource definition, typically missing fields.
    #[error("Invalid Echo CRD: {0}")]
    UserInputError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_reconcile_create_action() {

        struct KafkaTopicCLientMock;
        impl KafkaTopicOps for KafkaTopicCLientMock {
            async fn create_topic(
                &self,
                kafka_topic: Arc<KafkaTopic>,
            ) -> Result<(), kube_client::Error> {
                Ok(())
            }

            async fn delete_topic(
                &self,
                kafka_topic: Arc<KafkaTopic>,
            ) -> Result<(), kube_client::Error> {
                Ok(())
            }
        }
        let kafka_topic_spec = KafkaTopic {
            metadata: kube::core::ObjectMeta {
                name: Some("test-topic".to_string()),
                namespace: Some("default".to_string()),
                finalizers: None,
                ..Default::default()
            },
            spec: KafkaTopicSpec {
                bootstrap_server: "offline:9092".to_string(),
                topic: "test-topic".to_string(),
                partitions: 3,
                replication_factor: 1,
            },
        };

        struct KafkaTopicMock {
            kafka_topic: KafkaTopic,
        }
        let kafka_topic_mock = KafkaTopicMock {
            kafka_topic: kafka_topic_spec.clone(),
        };

        impl KubeClientCrdOps for KafkaTopicMock {
            async fn add_finalizer(
                &self,
                name: &str,
                namespace: &str,
            ) -> Result<KafkaTopic, kube_client::Error> {
                Ok(self.kafka_topic.clone())
            }

            async fn delete_finalizer(
                &self,
                name: &str,
                namespace: &str,
            ) -> Result<KafkaTopic, kube_client::Error> {
                Ok(self.kafka_topic.clone())
            }
        }

        let data = ContextData::new(KafkaTopicCLientMock {}, kafka_topic_mock);
        let context = Arc::new(data);

        // Mock helper behavior
        let create_called = Arc::new(Mutex::new(false));
        let create_called_clone = create_called.clone();

        // Call reconcile
        let result = reconcile(Arc::new(kafka_topic_spec.clone()), context.clone()).await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Action::requeue(Duration::from_secs(10)));
    }
}
