# Greeting-kafka-operator
The operator manages Kafka in a Kubernetes cluster using a custom resource definition (CRD) for the Greeting sample application.

The operator ensures that the Kafka topics are always in sync with the custom resource definition.

Features:
- Creates Kafka topics based on the custom resource definition.
- Deletes Kafka topics when they are removed from the custom resource definition.
- Monitors Kafka topics and recreates them if they are deleted from Kafka.
- Supports topic configuration such as partitions and replication factor.
- Provides logging and metrics for monitoring the operator's activity.
How it works:
- The operator watches for changes in the custom resource definition.
- When a change is detected, it compares the desired state (from the CRD) with the actual state (in Kafka).
- It performs the necessary actions to reconcile the two states.


## Prerequisites
- A Kubernetes cluster ex. minikube, kind, or any cloud provider.
- kubectl command-line tool installed and configured to access your cluster.
- Kafka cluster running and accessible from the Kubernetes cluster.
- Kafka Topic Operator image built and available in a container registry.
- Kafka Topic Operator deployed in the Kubernetes cluster.
- Kafka Topic Operator CRD installed in the Kubernetes cluster.
- 
## Installation
The instructions below assume you followed the steps in the Prerequisites section.
Installation involves deploying the Kafka Topic Operator and the Custom Resource Definition (CRD) for managing Kafka topics first. 
Then define a CR definition for the greeting topic used by the producer and consumer applications.

### Step 1: Build and Push the Kafka Topic Operator Image
Build the Kafka Topic Operator Docker image using the provided Makefile.
The Makefile includes a target to build the Docker image, tag it, and push it to a container registry, and deploy it to the Kubernetes cluster.
```bash
make
```

### Step 2: Install the Custom Resource Definition (CRD)
```bash
kubectl apply -f ./kubernetes/cr-kafka-topic.yaml``     
```
To uninstall the CRD, you can run the following command:
```bash
kubectl delete -f ./kubernetes/cr-kafka-topic.yaml`` 

```
## Managing greeting topic for producer and consumer inside Kafka
```
kubectl exec -it kafka-0 -- bash
kafka-topics --list --topic greetings --bootstrap-server kafka-0:9092
kafka-topics --create --topic greetings --partitions 10 --bootstrap-server kafka-0:9092
kafka-topics --alter --topic greetings --partitions 10 --bootstrap-server kafka-0:9092
kafka-topics --delete --topic greetings --bootstrap-server kafka-0:9092
```

