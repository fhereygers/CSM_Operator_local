# Setup of Cloudera Streams Messaging 

Following steps deploy CSM on Minikube running on your Macbook.

## Pre-requisites

- Docker or podman
- Minikube 
- helm
- valid cloudera license and its corresponding user name and password, generated on www.cloudera.com in the download section.

In case of podman make sure that the virtual machine is big enough

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


```
kubectl get deployments --namespace cld-streaming

```

```
kubectl get pods --namespace cld-streaming
```













minikube service cloudera-surveyor-service --namespace cld-streaming
