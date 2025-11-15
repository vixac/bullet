#This is the mongo connection string. Assign it to MONGO_PASS.ts mongodb://admin:secret@localhost:27017
docker run -d --name my-mongo \
  -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=admin \
  -e MONGO_INITDB_ROOT_PASSWORD=secret \
  mongo:7.0
