use dotenv::dotenv;
use kube::runtime::{Controller, watcher::Config};
use kube::{Api, Client};
use futures::stream::StreamExt;
use rdkafka::ClientConfig;
use std::sync::Arc;
use crate::kafka_topic_controller::{on_error, reconcile, ContextData, KafkaTopic};
use crate::kafka_topic_helper::KafkaAdminClient;

mod kafka_topic_controller;
mod kafka_topic_helper;

#[tokio::main]
async fn main() {
    env_logger::init();
    dotenv().ok();

    let kafka_broker = std::env::var("APP__KAFKA__BROKER")
        .expect("Missing Kafka bootstrap.server environment variable");

    let kubernetes_client = Client::try_default()
        .await
        .expect("Invalid KUBECONFIG environment variable");

    let crd_api = Api::<KafkaTopic>::all(kubernetes_client.clone());

    let context = Arc::new(ContextData::new(
        KafkaAdminClient {
            admin: ClientConfig::new()
                .set("bootstrap.servers", kafka_broker)
                .create()
                .expect("Failed to create Admin client"),
        },
        kafka_topic_controller::KubeClient {
            inner_kube_client: kubernetes_client.clone(),
        },
    ));

    Controller::new(crd_api, Config::default())
        .run(reconcile, on_error, context)
        .for_each(|result| async move {
            if let Err(err) = result {
                eprintln!("Reconciliation error: {:?}", err);
            }
        })
        .await;
}