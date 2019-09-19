STACK_NAME="lambda-b2-record-processor"

sam build --use-container

cd .aws-sam/build

sam package \
--template-file template.yaml \
--output-template-file package.yaml \
--s3-bucket eis-b2-lambda-dev \
--s3-prefix $STACK_NAME \
--profile b2


sam deploy \
--template-file package.yaml \
--stack-name $STACK_NAME \
--capabilities CAPABILITY_IAM \
--profile b2

