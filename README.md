# kafka-topic-operator
The operator manages Kafka topics in a Kubernetes cluster using a custom resource definition (CRD).

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
kubectl apply -f ./kubernetes/resourcedefinition.yaml
# This step is necessary to define the custom resource that the operator will manage.
# It allows you to create, update, and delete Kafka topics using Kubernetes resources.
# You can find the CRD definition in the `kubernetes/resourcedefinition.yaml` file.
# Make sure to apply it before deploying the operator.
```     
To uninstall the CRD, you can run the following command:
```bash
kubectl delete -f resourcedefinition.yaml

```

kafka-topics --create --topic greetings --partitions 10 --bootstrap-server kafka-0:9092