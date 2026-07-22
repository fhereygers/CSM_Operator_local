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












minikube service cloudera-surveyor-service --namespace cld-streaming
