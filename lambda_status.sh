#!/bin/bash

LAMBDA_NAME=$1
STATUS=$(awslocal lambda get-function --function-name $LAMBDA_NAME | jq -r '.Configuration.State')

while [ "$STATUS" != "Active" ]; do
    echo "Lambda function status: $STATUS"
    if [ "$STATUS" == "Failed" ]; then
        echo "Lambda function creation failed."
        exit 1
    fi
    sleep 5
    STATUS=$(awslocal lambda get-function --function-name $LAMBDA_NAME | jq -r '.Configuration.State')
done

echo "Lambda function is now active and ready."
