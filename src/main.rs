use crate::kafka_topic_controller::{on_error, reconcile, ContextData, KafkaTopic};
use crate::kafka_topic_helper::KafkaAdminClient;
use dotenv::dotenv;
use futures::stream::StreamExt;
use kube::runtime::watcher::Config;
use kube::runtime::Controller;
use kube::{Api, Client};
use rdkafka::ClientConfig;
use std::sync::Arc;

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

    let topic_client = KafkaAdminClient {
        admin: ClientConfig::new()
            .set(
                "bootstrap.servers",
                std::env::var("APP__KAFKA__BROKER")
                    .expect("Missing environmentvariable for Kafkas bootstrap.server"),
            )
            .create()
            .expect("Admin client creation failed"),
    };
    let kube_client = kafka_topic_controller::KubeClient {
        client: Client::try_default().await.unwrap(),
    };

    let data = ContextData::new(topic_client, kube_client);
    let context = Arc::new(data);
    
    Controller::new(crd_api.clone(), Config::default())
        .run(reconcile, on_error, context)
        .for_each(|reconciliation_result| async move {
            if let Err(reconciliation_err) = reconciliation_result {
                eprintln!("Reconciliation error: {:?}", reconciliation_err)
            }
        })
        .await;
}
