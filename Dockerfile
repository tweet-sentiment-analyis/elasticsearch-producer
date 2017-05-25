FROM frolvlad/alpine-oraclejdk8:slim
VOLUME /tmp
ADD target/elasticsearch-producer-0.0.1-jar-with-dependencies.jar app.jar
RUN sh -c 'touch /app.jar'

# name of queue where to put analysed tweets into
ENV SQS_QUEUE_NAME="analyised-tweets"
# Elasticsearch index name from where to get tweets
ENV ES_INDEX_NAME="twitter"
# Elasticsearch type name from where to get tweets
ENV ES_TYPE_NAME="tweet"
# Host on which Elasticsearch is running
ENV ES_HOST="https://some.host.com"
# Port on which Elasticsearch is running (usually 443 on AWS)
ENV ES_PORT=443
# Username to access Elasticsearch
ENV ES_USERNAME=elastic
# Password to access Elasticsearch
ENV ES_PASSWORD=changeme
# Key for having access to AWS SQS
ENV AWS_ACCESS_KEY_ID="changeme"
# Secret for key above
ENV AWS_SECRET_ACCESS_KEY="changeme"

# Some Java Env variable
ENV JAVA_OPTS="-Xms512m -Xmx768m"
ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /app.jar" ]