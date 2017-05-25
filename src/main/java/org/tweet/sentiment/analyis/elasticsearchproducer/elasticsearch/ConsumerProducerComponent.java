package org.tweet.sentiment.analyis.elasticsearchproducer.elasticsearch;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.mapping.PutMapping;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

public class ConsumerProducerComponent extends Thread {

    private static final Logger logger = Logger.getLogger(ConsumerProducerComponent.class.getName());

    public static final String ES_INDEX_NAME = "ES_INDEX_NAME";
    public static final String ES_TYPE_NAME  = "ES_TYPE_NAME";

    public static final String ES_HOST     = "ES_HOST";
    public static final String ES_PORT     = "ES_PORT";
    public static final String ES_USERNAME = "ES_USERNAME";
    public static final String ES_PASSWORD = "ES_PASSWORD";

    public static final String SQS_QUEUE_NAME = "SQS_QUEUE_NAME";

    private AmazonSQS  sqs;
    private JestClient elasticsearchClient;

    public ConsumerProducerComponent()
            throws IOException {
        this.init();
    }

    private void init()
            throws IOException {
        AWSCredentials credentials;
        try {
            credentials = new EnvironmentVariableCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the environment. " +
                            "Please make sure that your credentials are located in the environment variables " +
                            "AWS_ACCESS_KEY_ID resp. AWS_SECRET_ACCESS_KEY",
                    e);
        }

        AmazonSQSClientBuilder clientBuilder = AmazonSQSClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials));
        clientBuilder.setRegion(Regions.US_WEST_2.getName());
        sqs = clientBuilder.build();

        String esHost = System.getenv(ES_HOST);
        int esPort = Integer.parseInt(System.getenv(ES_PORT));
        String esUsername = System.getenv(ES_USERNAME);
        String esPassword = System.getenv(ES_PASSWORD);
        String esIndex = System.getenv(ES_INDEX_NAME);
        String esType = System.getenv(ES_TYPE_NAME);

        logger.info("Trying to connect to elasticsearch on " + esHost + ":" + esPort + " using " + esUsername + ":" + esPassword);

        // Construct a new Jest client according to configuration via factory
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(esHost + ":" + esPort)
                .defaultCredentials(esUsername, esPassword)
                .multiThreaded(true)
                //Per default this implementation will create no more than 2 concurrent connections per given route
                .defaultMaxTotalConnectionPerRoute(1)
                // and no more 20 connections in total
                .maxTotalConnection(20)
                .build());
        this.elasticsearchClient = factory.getObject();

        // check whether the index and the type already exist
        // create otherwise
        logger.info("Checking whether ES index " + esIndex + " exists...");
        boolean indexExists = this.elasticsearchClient.execute(new IndicesExists.Builder(esIndex).build()).isSucceeded();
        logger.info("Index exists: " + indexExists);

        if (! indexExists) {
            logger.info("Creating index " + esIndex);
            this.elasticsearchClient.execute(new CreateIndex.Builder(esIndex).build());
        }

        // create mapping
        PutMapping putMapping = new PutMapping.Builder(
                esIndex,
                esType,
                "{\n" +
                        "  \"properties\": {\n" +
                        "    \"id\": {\n" +
                        "      \"type\": \"long\"\n" +
                        "    },\n" +
                        "    \"timestamp\": {\n" +
                        "      \"type\": \"long\"\n" +
                        "    },\n" +
                        "    \"tweet\": {\n" +
                        "      \"type\": \"object\"\n" +
                        "      \"enabled\": false " +
                        "    },\n" +
                        "    \"term\": {\n" +
                        "      \"type\": \"keyword\"\n" +
                        "    },\n" +
                        "    \"sentiment\": {\n" +
                        "      \"type\": \"double\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        ).build();
        this.elasticsearchClient.execute(putMapping);
    }

    @Override
    public void run() {
        // Get twitter message from queue
        String queueName = System.getenv(SQS_QUEUE_NAME);
        logger.info("Using queue '" + queueName + "' for sending tweets to");

        try {
            while (true) {
                // Receive messages
                //System.out.println("Receiving messages from MyQueue.\n");
                String queueUrl = sqs.getQueueUrl(queueName).getQueueUrl();
                ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
                List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

                if (messages.size() == 0) {
                    // wait for some new messages
                    Thread.sleep(500);
                    continue;
                }


                sendToElasticsearch(messages);

                for (Message msg : messages) {
                    // Delete a message
                    logger.info("Deleting message with AWS id " + msg.getMessageId());
                    String messageReceiptHandle = msg.getReceiptHandle();
                    sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageReceiptHandle));
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void sendToElasticsearch(List<Message> messages) {
        String esIndex = System.getenv(ES_INDEX_NAME);
        String esType = System.getenv(ES_TYPE_NAME);

        //index documents
        Bulk.Builder bulkBuilder = new Bulk.Builder()
                .defaultIndex(esIndex)
                .defaultType(esType);


        for (Message msg : messages) {
            logger.info("Adding message with AWS id " + msg.getMessageId() + " to ES Bulk Index request");
            bulkBuilder.addAction(new Index.Builder(msg.getBody()).build());
        }

        Bulk bulk = bulkBuilder.build();
        try {
            BulkResult response = this.elasticsearchClient.execute(bulk);

            if (! response.isSucceeded()) {
                logger.warning("Could not index documents: " + response.getErrorMessage() + ". " + response.getJsonString());
            }
        } catch (IOException e) {
            logger.warning("Could not index documents using bulk request: " + e.getMessage());
        }
    }
}
