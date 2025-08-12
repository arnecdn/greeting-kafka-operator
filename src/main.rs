use futures::stream::StreamExt;
use kube::runtime::watcher::Config;
use kube::runtime::Controller;
use kube::{Api, Client};
use std::sync::Arc;
use crate::kafka_topic_controller::{on_error, reconcile, ContextData, KafkaTopic};

mod kafka_topic_helper;
mod kafka_topic_controller;

#[tokio::main]
async fn main() {
    // First, a Kubernetes client must be obtained using the `kube` crate
    // The client will later be moved to the custom controller
    let kubernetes_client: Client = Client::try_default()
        .await
        .expect("Expected a valid KUBECONFIG environment variable.");

    // Preparation of resources used by the `kube_runtime::Controller`
    let crd_api: Api<KafkaTopic> = Api::all(kubernetes_client.clone());
    let context: Arc<ContextData> = Arc::new(ContextData::new(kubernetes_client.clone()));
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

