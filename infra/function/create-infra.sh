#!/bin/bash

set -euf -o pipefail

echo "Logging in to Azure..."
az login

echo "Verifying jq is installed..."
jq --version

RESOURCE_GROUP_NAME='stockton-jpe01'
STORAGE_ACCOUNT_NAME='stocktonjpe01fm'
STORAGE_ACCOUNT_LOCATION='japaneast'
FUNCTION_NAME="${RESOURCE_GROUP_NAME}-flow-merchant"
TABLE_NAME='flowmerchant'
QUEUE_NAME='merchantsignals'

echo "Creating storage account..."
# Create storage account
az storage account create \
    --name ${STORAGE_ACCOUNT_NAME} \
    --resource-group ${RESOURCE_GROUP_NAME} \
    --location ${STORAGE_ACCOUNT_LOCATION} \
    --sku Standard_LRS \
    --kind StorageV2

echo "Getting connection string..."
# Get connection string
CONNECTION_STRING=$(az storage account show-connection-string \
    --name ${STORAGE_ACCOUNT_NAME} \
    --resource-group ${RESOURCE_GROUP_NAME} \
    --output tsv \
    --query connectionString)

echo "Creating queue..."
# Create queue
az storage queue create \
    --name ${QUEUE_NAME} \
    --connection-string ${CONNECTION_STRING}

echo "Getting storage account key..."
# Get storage account key and table endpoint
STORAGE_ACCOUNT_KEY=$(az storage account keys list --account-name ${STORAGE_ACCOUNT_NAME} --query '[0].value' -o tsv)

echo "Getting table endpoint..."
STORAGE_ACCOUNT_TABLE_ENDPOINT=$(az storage account show --name ${STORAGE_ACCOUNT_NAME} | jq '.primaryEndpoints.table')

echo "Creating table..."
# Create table
az storage table create --name ${TABLE_NAME} \
                        --account-key ${STORAGE_ACCOUNT_KEY} \
                        --account-name ${STORAGE_ACCOUNT_NAME} \
                        --connection-string ${CONNECTION_STRING} \
                        --table-endpoint ${STORAGE_ACCOUNT_TABLE_ENDPOINT}

echo "Generating access policy..."
# Set DATE_START and DATE_END variables
DATE_START=$(date -u +"%Y-%m-%dT%H:%MZ")
DATE_END=$(date -u -d "+10 years" +"%Y-%m-%dT%H:%MZ")

# Generate access policy
az storage table policy create --name ${TABLE_NAME}crud \
                                --debug \
                                --table-name ${TABLE_NAME} \
                                --account-key ${STORAGE_ACCOUNT_KEY} \
                                --account-name ${STORAGE_ACCOUNT_NAME} \
                                --expiry ${DATE_END} \
                                --permissions rad \
                                --start ${DATE_START} 
                                
echo "Creating function app..."
az functionapp create --resource-group ${RESOURCE_GROUP_NAME} \
    --consumption-plan-location ${STORAGE_ACCOUNT_LOCATION} \
    --runtime python \
    --os-type Linux \
    --runtime-version 3.10 \
    --functions-version 4 \
    --name ${FUNCTION_NAME} \
    --storage-account ${STORAGE_ACCOUNT_NAME}

echo "Setting storage account connection string..."
az functionapp config appsettings set --name ${FUNCTION_NAME} \
    --resource-group ${RESOURCE_GROUP_NAME} \
    --settings "storageAccountConnectionString=${CONNECTION_STRING}"

echo "Storage account, queue, and table created successfully."

###
echo ">>> NOTE THE FOLLOWING <<<"
STORAGE_ACCOUNT_KEY=$(az storage account keys list --account-name ${STORAGE_ACCOUNT_NAME} --query '[0].value' -o tsv)
QUEUE_URL=$(az storage account show --name ${STORAGE_ACCOUNT_NAME} --query "primaryEndpoints.queue" -o tsv)merchantsignals

echo "STORAGE_ACCOUNT_NAME=${STORAGE_ACCOUNT_NAME}"
echo "STORAGE_ACCOUNT_KEY=${STORAGE_ACCOUNT_KEY}"
echo "STORAGE_ACCOUNT_TABLE_ENDPOINT=${STORAGE_ACCOUNT_TABLE_ENDPOINT}"
echo "STORAGE_QUEUE_URL=${QUEUE_URL}"
echo "STORAGE_ACCT_CONN_STR=${CONNECTION_STRING}"