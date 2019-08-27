# Query-seller-rate Index
Java project to transfer query/seller rate data from Google BigQuery, store to Google Cloud Storage, then build an ElasticSearch index from it.

## Technology
* elasticsearch-rest-high-level-client
* google-cloud-bigquery
* google-cloud-storage
* docker

## How to build Docker image
```
git clone
```
```
cd to repo
```
```
docker build -t seller-bulk-index .
```

## Configure Elastic Search
Change properties from `./config.properties` file:
```
HOST = uat-browser-esfive-1.svr.tiki.services
PORT = 9200
```

## How to run
### First time (start container with name : sellers-container)
```
docker run --name sellers-container -v "$(pwd)":/src -v "$(pwd)"/docker/supervisord/supervisor.d:/etc/supervisor.d seller-bulk-index
```

### Next time

SSH to container
```
docker exec -it sellers-container /bin/bash
```
```
cd /src
```
Run command 
```
mvn exec:java -Dexec.mainClass="BulkUpload"
```

or if you want to separate it to 2 different jobs: *upload-gs* and *insert-es*

```
mvn exec:java -Dexec.mainClass="BulkUpload" -Dexec.args="upload-gs"
```
```
mvn exec:java -Dexec.mainClass="BulkUpload" -Dexec.args="insert-es"
```

or if you wish to reindex in the past, for (for example 19/8/2019)

```
mvn exec:java -Dexec.mainClass="BulkUpload" -Dexec.args="insert-es 20190819"
```

## After run
Check data in http://uat-browser-esfive-1.svr.tiki.services:9200/query_seller_rate/_search/?pretty




