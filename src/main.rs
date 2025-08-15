use crate::kafka_topic_controller::{ContextData, KafkaTopic, on_error, reconcile, KafkaTopicSpec};
use futures::stream::StreamExt;
use kube::runtime::Controller;
use kube::runtime::controller::Action;
use kube::runtime::watcher::Config;
use kube::{Api, Client};
use std::sync::Arc;
use dotenv::dotenv;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::ClientConfig;
use crate::kafka_topic_helper::KafkaTopicClient;

mod kafka_topic_controller;
mod kafka_topic_helper;

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();
    // First, a Kubernetes client must be obtained using the `kube` crate
    // The client will later be moved to the custom controller
    let kubernetes_client: Client = Client::try_default()
        .await
        .expect("Expected a valid KUBECONFIG environment variable.");

    // Preparation of resources used by the `kube_runtime::Controller`
    let crd_api: Api<KafkaTopic> = Api::all(kubernetes_client.clone());

    let topic_client = KafkaTopicClient{
        admin:ClientConfig::new()
            .set(
                "bootstrap.servers",
                std::env::var("APP__KAFKA__BROKER").expect("Missing environmentvariable for Kafkas bootstrap.server"),

            )
            .create()
            .expect("Admin client creation failed")
    };
    let kube_client = kafka_topic_controller::KubeClient {client: Client::try_default().await.unwrap()};

    let data = ContextData::new(topic_client, kube_client);
    let context = Arc::new(data);

    // let context: Arc<ContextData> = Arc::new(ContextData::new( admin, kubernetes_client.clone()));
    // The controller comes from the `kube_runtime` crate and manages the reconciliation process.
    // It requires the following information:
    // - `kube::Api<T>` this controller "owns". In this case, `T = KafkaTopic`, as this controller owns the `KafkaTopic` resource,
    // - `kube::runtime::watcher::Config` can be adjusted for precise filtering of `Echo` resources before the actual reconciliation, e.g. by label,
    // - `reconcile` function with reconciliation logic to be called each time a resource of `KafkaTopic` kind is created/updated/deleted,
    // - `on_error` function to call whenever reconciliation fails.


    Controller::new(crd_api.clone(), Config::default())
        .run(reconcile, on_error, context)
        .for_each(|reconciliation_result| async move {
            if let Err(reconciliation_err) = reconciliation_result {
                eprintln!("Reconciliation error: {:?}", reconciliation_err)
            }
        })
        .await;
}
