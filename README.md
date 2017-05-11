# Elasticsearch Consumer Producer

This component fetches messages from a message queue and stores in Elasticsearch. 

## Build and Deploy
In order to build this project as well as deploy a new image to docker hub, perform the following steps:

* Invoke `mvn package` to install and package the project into a JAR located in the target folder.

* Login to your account to dockerhub: `docker login`
* From the root of this project, build a new docker image with the respective version: `docker build -t tweetsentimentanalysis/elasticsearch-producer:0.0.1 .`
* Run `docker push tweetsentimentanalysis/elasticsearch-producer:0.0.1` to deploy to dockerhub

## Get the container
If you do not want to build the container by your own, you may use the following to get the image from DockerHub:
`docker pull tweetsentimentanalysis/elasticsearch-producer:latest`

## Run the container
After you have built the container, you may run it using the following command: `docker run -e AWS_ACCESS_KEY_ID='...' -e AWS_SECRET_ACCESS_KEY='...'  -e NO_THREADS='1' -e ES_HOST='https://url' -e ES_PORT='443' -e ES_USERNAME='elastic' -e ES_PASSWORD='changeme' tweetsentimentanalysis/elasticsearch-producer:0.0.1` which runs the container, i.e. consumes analyzed tweets and publishes them into Elasticsearch.
Use the appropriate credentials for this.

## Remove built images
In order to clean up, you may want to remove the previously created image and container:

* Run `docker ps -a` in order list created containers.
* Choose the corresponding container id and invoke `docker stop <ID> && docker rm <ID>`
* Run `docker images` to list all created images
* Run `docker rmi <ID>` with the id of the image to remove it
