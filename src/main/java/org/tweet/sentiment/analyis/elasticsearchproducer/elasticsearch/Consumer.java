package org.tweet.sentiment.analyis.elasticsearchproducer.elasticsearch;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
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

public class Consumer extends Thread {

    private static final Logger logger = Logger.getLogger(Consumer.class.getName());

    public static final String ES_INDEX_NAME = "twitter";
    public static final String ES_TYPE_NAME  = "tweet";

    //    public static final String ES_HOST = "https://search-twitter-es-nhegy3p4ogvptnpu4y6zmpbj2i.us-west-2.es.amazonaws.com";
    public static final String ES_HOST = "http://192.168.99.100";
    public static final int    ES_PORT = 9200;

    private AmazonSQS  sqs;
    private JestClient elasticsearchClient;

    public Consumer()
            throws IOException {
        this.init();
    }

    private void init()
            throws IOException {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("sqs").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

        AmazonSQSClientBuilder clientBuilder = AmazonSQSClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials));
        clientBuilder.setRegion(Regions.US_WEST_2.getName());
        sqs = clientBuilder.build();


        // Construct a new Jest client according to configuration via factory
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(ES_HOST + ":" + ES_PORT)
                .defaultCredentials("elastic", "changeme")
                .multiThreaded(true)
                //Per default this implementation will create no more than 2 concurrent connections per given route
                .defaultMaxTotalConnectionPerRoute(1)
                // and no more 20 connections in total
                .maxTotalConnection(20)
                .build());
        this.elasticsearchClient = factory.getObject();

        // check whether the index and the type already exist
        // create otherwise
        logger.info("Checking whether ES index " + ES_INDEX_NAME + " exists...");
        boolean indexExists = this.elasticsearchClient.execute(new IndicesExists.Builder(ES_INDEX_NAME).build()).isSucceeded();
        logger.info("Index exists: " + indexExists);

        if (! indexExists) {
            logger.info("Creating index " + ES_INDEX_NAME);
            this.elasticsearchClient.execute(new CreateIndex.Builder(ES_INDEX_NAME).build());
        }

        // create mapping
        PutMapping putMapping = new PutMapping.Builder(
                ES_INDEX_NAME,
                ES_TYPE_NAME,
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
        try {
            while (true) {
                // Receive messages
                //System.out.println("Receiving messages from MyQueue.\n");
                String queueUrl = sqs.getQueueUrl("analyised-tweets").getQueueUrl();
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
        //index documents
        Bulk.Builder bulkBuilder = new Bulk.Builder()
                .defaultIndex(ES_INDEX_NAME)
                .defaultType(ES_TYPE_NAME);


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
