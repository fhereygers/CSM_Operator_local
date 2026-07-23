# Setup of Cloudera Streams Messaging 

Following steps deploy CSM on Minikube running on your Macbook.

## Pre-requisites

- Following packages need to be installed: Docker (or podman), Minikube, helm
- valid cloudera license and its corresponding user name and password, generated on www.cloudera.com in the download section.

If you clone this repository and also add the license file in the directory, the below commands should work without much changes.

In case of podman make sure that the virtual machine is big enough.

```
podman machine init --cpus 6 --memory 14336
podman machine start
```

## Setup

Open a terminal window and Spin up Minikube with enough resources for the full stack

```
minikube start --cpus 4 --memory 12288
```
Define envirnomental variables that will be used in the subsequent commands

```
export CLOUDERA_LICENSE_USER="<User_Name>"
export CLOUDERA_LICENSE_PASSWORD="User_password"
```
## Installing Strimzi with Helm

Create a namespace in your Kubernetes cluster

```
kubectl create namespace cld-streaming
```
Log in into the cloudera docker registry with helm

```
helm registry login container.repository.cloudera.com -u "$CLOUDERA_LICENSE_USER" -p "$CLOUDERA_LICENSE_PASSWORD"
```
Create a Kubernetes secret containing your cloudera credentials

```
kubectl create secret docker-registry cloudera-creds \
  --namespace cld-streaming \
  --docker-server=container.repository.cloudera.com \
  --docker-username="$CLOUDERA_LICENSE_USER" \
  --docker-password="$CLOUDERA_LICENSE_PASSWORD" 
```
Install Strimzi with helm install
```
helm install strimzi-cluster-operator \
  --namespace cld-streaming \
  --set 'image.imagePullSecrets[0].name=cloudera-creds' \
  --set-file clouderaLicense.fileContent=frank_hereygers_2025_2026_cloudera_license.txt \
  --set watchAnyNamespace=true \
  oci://container.repository.cloudera.com/cloudera-helm/csm-operator/strimzi-kafka-operator \
  --version 1.6.0-b99
```

Verify your installation
```
kubectl get deployments --namespace cld-streaming

```

```
kubectl get pods --namespace cld-streaming
```

## Deploy a kafka cluster

kubectl apply \
  --filename Kafka.yaml,broker.yaml,controller.yaml \
  --namespace cld-streaming

kubectl get pods --namespace cld-streaming

## Validate the kafka cluster

IMAGE=$(kubectl get pod my-cluster-broker-0  --namespace cld-streaming --output jsonpath='{.spec.containers[0].image}')


kubectl run kafka-admin -it \
  --namespace cld-streaming \
  --image=$IMAGE \
  --rm=true \
  --restart=Never \
  --command -- /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server my-cluster-kafka-bootstrap:9092 \
    --create \
    --topic my-topic



kubectl run kafka-producer -it \
  --namespace cld-streaming \
  --image=$IMAGE \
  --rm=true \
  --restart=Never \
  --command -- /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server my-cluster-kafka-bootstrap:9092 \
    --topic my-topic


kubectl run kafka-consumer -it \
  --namespace cld-streaming \
  --image=$IMAGE \
  --rm=true \
  --restart=Never \
  --command -- /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server my-cluster-kafka-bootstrap:9092 \
    --topic my-topic \
    --from-beginning









## Deploy Surveyor

minikube service cloudera-surveyor-service --namespace cld-streaming
