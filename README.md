# flightaware
Projects for Containers that run on an Azure IoT Edge device, that grab local FlightAware data and ingress it to Azure. 

Docs for Development: https://learn.microsoft.com/en-us/azure/iot-edge/how-to-vs-code-develop-module?view=iotedge-1.4&tabs=csharp&pivots=iotedge-dev-cli

Notes when using CodeSpaces to develop and deploy the "DataFetcher" module:

Install arm64 support first:
docker run --privileged --rm tonistiigi/binfmt --install all

Get the Azure CLI for your Codespace:
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

Then:
az config set extension.use_dynamic_install=yes_without_prompt

Build Steps for IoT Edge Module:

1) Clone the repo, and CD to the "DataFetcher" directory
2) Create an .env file with the following values set:

CONTAINER_REGISTRY_SERVER="cloudyflightaware.azurecr.io"
CONTAINER_REGISTRY_USERNAME="cloudyflightaware"
CONTAINER_REGISTRY_PASSWORD="<registry_password>"
IMAGE="cloudyflightaware.azurecr.io/datafetcher:0.0.2-arm64v8"

PiAwareUri="http://<Your PIAware IP>:8080/data/aircraft.json"

3) Run "iotedgedev build"

4) Build docker image for arm64 raspberry PI running IoT Edge from the DataFetcher directory: 
  docker buildx build --platform linux/arm64 --rm -f "./modules/DataFetcher/Dockerfile.arm64v8" -t cloudyflightaware.azurecr.io/datafetcher:0.0.2-arm64v8 "./modules/DataFetcher"

5) Push image:
docker push myacr.azurecr.io/datafetcher:0.0.2-arm64v8

6) Update deployment template:
find . -type f -exec sed -i 's/<CONTAINER_REGISTRY_SERVER>/$CONTAINER_REGISTRY_SERVER/g' {} +
find . -type f -exec sed -i 's/<CONTAINER_REGISTRY_USERNAME>/$CONTAINER_REGISTRY_USERNAME/g' {} +
find . -type f -exec sed -i 's/<CONTAINER_REGISTRY_PASSWORD>/$CONTAINER_REGISTRY_PASSWORD/g' {} +
find . -type f -exec sed -i 's/<IMAGE>/$IMAGE/g' {} +

6) Login to Azure so we can update IoT Edge:
   az login --use-device-code
   az account set --subscription <your subscription id>

   HUB_NAME="$(az iot hub list --query [].name --output tsv)"
   DEVICE_ID="$(az iot hub device-identity list --hub-name $HUB_NAME --query [].deviceId --output tsv)"
   CONNECTION_STRING="$(az iot hub connection-string show -n $HUB_NAME --policy-name iothubowner --output tsv)"

   az iot edge set-modules --hub-name $HUB_NAME --device-id $DEVICE_ID --content ./deployment.template.json --login "$CONNECTION_STRING"
