sam build --use-container
sam local invoke InitFunction \
--event event.json \
--env-vars localenv.json \
--profile default

