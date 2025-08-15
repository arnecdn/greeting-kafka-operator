use crate::kafka_topic_helper;
use crate::kafka_topic_helper::{KafkaTopicClient, KafkaTopicClientOps};
use kube::api::{Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Api, CustomResource, Resource, ResourceExt};
use kube_client::Client;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
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

trait KubeClientOps {
    async fn add_finalizer(
        &self,
        name: &str,
        namespace: &str,
    ) -> Result<KafkaTopic, kube::Error>;

    async fn delete_finalizer(
        &self,
        name: &str,
        namespace: &str,
    ) -> Result<KafkaTopic, kube::Error>;
}

pub(crate) struct KubeClient {
    pub(crate) client: Client,
}

impl KubeClientOps for KubeClient {
    async fn add_finalizer(
        &self,
        name: &str,
        namespace: &str,
    ) -> Result<KafkaTopic, kube::Error> {
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

pub struct ContextData {
    kafka_topic_client: KafkaTopicClient,
    /// Kubernetes client to make Kubernetes API requests with. Required for K8S resource management.
    client: KubeClient,
}

impl ContextData {
    /// Constructs a new instance of ContextData.
    ///
    /// # Arguments:
    /// - `client`: A Kubernetes client to make Kubernetes REST API requests with. Resources
    /// will be created and deleted with this client.
    pub fn new(kafka_topic_client: KafkaTopicClient, client: KubeClient) -> Self {
        ContextData {
            kafka_topic_client,
            client,
        }
    }
}

/// Action to be taken upon an `Echo` resource during reconciliation
enum KafkaTopicAction {
    /// Create the subresources and Kafka topics
    Create,
    /// Delete all subresources created in the `Create` phase
    Delete,
    /// This `KafkaTopic` resource is in desired state and requires no actions to be taken
    NoOp,
}

pub async fn reconcile(
    kafka_topic: Arc<KafkaTopic>,
    context: Arc<ContextData>,
) -> Result<Action, Error> {
    ; // The `Client` is shared -> a clone from the reference is obtained

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

    // let kafka_topic_ops =  KafkaTopicOps{admin : context.kafka_admin_client};

    // Performs action as decided by the `determine_action` function.
    match determine_action(&kafka_topic) {
        KafkaTopicAction::Create => {
            // Creates a new CR with a Kafka Topic, but applies a finalizer first.
            // Finalizer is applied first, as the operator might be shut down and restarted
            // at any time, leaving subresources in intermediate state. This prevents leaks on
            // the `KafkaTopic` resource deletion.

            // Apply the finalizer first. If that fails, the `?` operator invokes automatic conversion
            // of `kube::Error` to the `Error` defined in this crate.
            &context.client.add_finalizer( &name, &namespace).await?;
            // Invoke creation of a Kubernetes built-in resource named deployment with `n` echo service pods.
            // kafka_topic::deploy(client, &name, kafkaTopic.spec.partitions, &namespace).await?;
            &context.kafka_topic_client.create_topic(kafka_topic).await?;
            Ok(Action::requeue(Duration::from_secs(10)))
        }
        KafkaTopicAction::Delete => {
            // Deletes any subresources related to this `KafkaTopic` resources. If and only if all subresources
            // are deleted, the finalizer is removed and Kubernetes is free to remove the `KafkaTopic` resource.

            //First, delete the KafkaTopic. If there is any error deleting the topic, it is
            // automatically converted into `Error` defined in this crate and the reconciliation is ended
            // with that error.
            // Note: A more advanced implementation would check for the topics's existence.
            // kafka_topic::finalizer_delete(client.clone(), &name, &namespace).await?;
            &context.kafka_topic_client.delete_topic(kafka_topic).await?;
            // Once the topics is successfully removed, remove the finalizer to make it possible
            // for Kubernetes to delete the `KafkaTopic` resource.
            &context.client.delete_finalizer(&name, &namespace).await?;
            Ok(Action::await_change()) // Makes no sense to delete after a successful delete, as the resource is gone
        }
        // The resource is already in desired state, do nothing and re-check after 10 seconds
        KafkaTopicAction::NoOp => Ok(Action::requeue(Duration::from_secs(10))),
    }
}

/// Resources arrives into reconciliation queue in a certain state. This function looks at
/// the state of given `KafkaTopic` resource and decides which actions needs to be performed.
/// The finite set of possible actions is represented by the `KafkaTopicAction` enum.
///
/// # Arguments
/// - `echo`: A reference to `Echo` being reconciled to decide next action upon.
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
/// Prints out the error to `stderr` and requeues the resource for another reconciliation after
/// five seconds.
///
/// # Arguments
/// - `echo`: The erroneous resource.
/// - `error`: A reference to the `kube::Error` that occurred during reconciliation.
/// - `_context`: Unused argument. Context Data "injected" automatically by kube-rs.
pub fn on_error(echo: Arc<KafkaTopic>, error: &Error, _context: Arc<ContextData>) -> Action {
    eprintln!("Reconciliation error:\n{:?}.\n{:?}", error, echo);
    Action::requeue(Duration::from_secs(5))
}

/// All errors possible to occur during reconciliation
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
    use kube::Client;
    use kubernetes_mock::make_mocker;
    use rdkafka::ClientConfig;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_reconcile_create_action() {
        // Mock KafkaTopic resource
        let kafka_topic = Arc::new(KafkaTopic {
            metadata: kube::core::ObjectMeta {
                name: Some("test-topic".to_string()),
                namespace: Some("default".to_string()),
                finalizers: None,
                ..Default::default()
            },
            spec: KafkaTopicSpec {
                bootstrap_server: "localhost:9092".to_string(),
                topic: "test-topic".to_string(),
                partitions: 3,
                replication_factor: 1,
            },
        });


        let topic_client = KafkaTopicClient{
            admin:ClientConfig::new()
            .create()
            .expect("Admin client creation failed")
        };
        let client = KubeClient{client: Client::try_default().await.unwrap()};

        let data = ContextData::new(topic_client, client);
        let context = Arc::new(data);

        // Mock helper behavior
        let create_called = Arc::new(Mutex::new(false));
        let create_called_clone = create_called.clone();

        // Call reconcile
        let result = reconcile(kafka_topic.clone(), context.clone()).await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Action::requeue(Duration::from_secs(10)));
        assert!(*create_called.lock().await);
    }
}
