# Greeting-kafka-operator
The operator manages Kafka in a Kubernetes cluster using a custom resource definition (CRD) for the Greeting sample application.

## Prerequisites
- A Kubernetes cluster ex. minikube, kind, or any cloud provider.
- kubectl command-line tool installed and configured to access your cluster.
- Kafka cluster running and accessible from the Kubernetes cluster.
- Kafka Topic Operator image built and available in a container registry.
- Kafka Topic Operator deployed in the Kubernetes cluster.
- Kafka Topic Operator CRD installed in the Kubernetes cluster.
## Installation
To install the Kafka Topic Operator, you need to apply the custom resource definition (CRD) and deploy the operator in your Kubernetes cluster.
### Step 1: Install the Custom Resource Definition (CRD)
```bash
# Install the CRD for Kafka topics
kubectl apply -f ./kubernetes/crd.yaml
# This step is necessary to define the custom resource that the operator will manage.
# It allows you to create, update, and delete Kafka topics using Kubernetes resources.
# You can find the CRD definition in the `kubernetes/crd.yaml` file.
# Make sure to apply it before deploying the operator.
```     
To uninstall the CRD, you can run the following command:
```bash
kubectl delete -f crd.yaml

```
## Managing greeting topic for producer and consumer
```
kubectl exec -it kafka-0 -- bash
kafka-topics --list --topic greetings --bootstrap-server kafka-0:9092
kafka-topics --create --topic greetings --partitions 10 --bootstrap-server kafka-0:9092
kafka-topics --alter --topic greetings --partitions 10 --bootstrap-server kafka-0:9092
kafka-topics --delete --topic greetings --bootstrap-server kafka-0:9092
```
