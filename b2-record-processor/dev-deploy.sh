STACK_NAME="test-record-processor"
BUCKET_NAME=ecoation-backend-dev
sam build --use-container

cd .aws-sam/build

sam package \
--template-file template.yaml \
--output-template-file package.yaml \
--s3-bucket $BUCKET_NAME \
--s3-prefix $STACK_NAME \
--profile dev 

sam deploy \
--template-file package.yaml \
--stack-name $STACK_NAME \
--capabilities CAPABILITY_IAM \
--profile dev

