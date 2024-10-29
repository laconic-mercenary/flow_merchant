#!/bin/bash

set -euf -o pipefail

RESOURCE_GROUP_NAME='stockton-jpe01'
FUNCTION_NAME="${RESOURCE_GROUP_NAME}-flow-merchant"


echo "Logging in to Azure..."
az login

#zip -r fm.zip .

#az functionapp config appsettings set --name $FUNCTION_NAME \
#    --resource-group $RESOURCE_GROUP_NAME \
#    --settings AzureWebJobsFeatureFlags=EnableWorkerIndexing

#az functionapp config appsettings set --name $FUNCTION_NAME \
#    --resource-group $RESOURCE_GROUP_NAME \
#    --settings WEBSITE_RUN_FROM_PACKAGE=1

#az functionapp deployment source config-zip \
#  --resource-group $RESOURCE_GROUP_NAME \
#  --name $FUNCTION_NAME \
#  --src fm.zip

func azure functionapp publish ${FUNCTION_NAME} --build remote --python
