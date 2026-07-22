# Setup of Cloudera Streams Messaging on Macbook running on Minikube

## Pre-requisites

- Docker or podman
- Minikube 
- helm


Setup with pod man


```
{
	podman machine init --cpus 6 --memory 14336
  podman machine start

}
```




# Spin up Minikube with enough resources for the full stack
minikube start --cpus 4 --memory 12288



export CLOUDERA_LICENSE_USER="<User_Name>"
export CLOUDERA_LICENSE_PASSWORD="User_password"












minikube service cloudera-surveyor-service --namespace cld-streaming
