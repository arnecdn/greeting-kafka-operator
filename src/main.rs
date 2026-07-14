use dotenv::dotenv;
use kube::runtime::{Controller, watcher::Config};
use kube::{Api, Client};
use futures::stream::StreamExt;
use rdkafka::ClientConfig;
use std::sync::Arc;
use log::{error, info};
use crate::kafka_topic_controller::{on_error, reconcile, ContextData, KafkaTopic};
use crate::kafka_topic_helper::KafkaAdminClient;

mod kafka_topic_controller;
mod kafka_topic_helper;

#[tokio::main]
async fn main() {
    info!("Starting kafka-topic controller");
    dotenv().ok();
    let my_pod_name = std::env::var("APP__KUBE__MY_POD_NAME").expect("Missing my_pod_name");
    let otel_endpoint = std::env::var("APP__OTEL_COLLECTOR__OLTP_ENDPOINT").expect("Missing otel endpoint");

    let providers = greeting_otel::init_otel(&otel_endpoint,"greeting-kafka-operator", &my_pod_name).await;

    let kubernetes_client = Client::try_default()
        .await
        .expect("Invalid KUBECONFIG environment variable");

    let crd_api = Api::<KafkaTopic>::all(kubernetes_client.clone());

    let kafka_broker = std::env::var("APP__KAFKA__BROKER")
        .expect("Missing Kafka bootstrap.server environment variable");

    info!("Kafka bootstrap.servers={}", kafka_broker);

    let mut kafka_config = ClientConfig::new();
    kafka_config
        .set("bootstrap.servers", &kafka_broker)
        .set("request.timeout.ms", "10000")
        .set("socket.timeout.ms", "10000")
        .set("api.version.request", "true");

    if let Ok(security_protocol) = std::env::var("APP__KAFKA__SECURITY_PROTOCOL") {
        kafka_config.set("security.protocol", security_protocol);
    }
    if let Ok(sasl_mechanism) = std::env::var("APP__KAFKA__SASL_MECHANISM") {
        kafka_config.set("sasl.mechanism", sasl_mechanism);
    }
    if let Ok(sasl_username) = std::env::var("APP__KAFKA__SASL_USERNAME") {
        kafka_config.set("sasl.username", sasl_username);
    }
    if let Ok(sasl_password) = std::env::var("APP__KAFKA__SASL_PASSWORD") {
        kafka_config.set("sasl.password", sasl_password);
    }
    if let Ok(ca_location) = std::env::var("APP__KAFKA__SSL_CA_LOCATION") {
        kafka_config.set("ssl.ca.location", ca_location);
    }

    let context = Arc::new(ContextData::new(
        KafkaAdminClient {
            inner_kafka_client: kafka_config
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

    if let Err(e) = providers.shutdown().await{
        error!("Failed to shut down: {:?}", e);
    }
}