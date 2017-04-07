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
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class Consumer extends Thread {

    public static final String INDEX_NAME = "twitter";
    public static final String TYPE_NAME  = "tweet";

    public static final String ES_HOST = "192.168.99.100";
    public static final int    ES_PORT = 9200;

    private AmazonSQS       sqs;
    private TransportClient elasticsearchClient;
    private JSONParser      parser;

    public Consumer()
            throws UnknownHostException {
        // set cluster name
        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();

        this.elasticsearchClient = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(Consumer.ES_HOST), Consumer.ES_PORT));

        this.parser = new JSONParser();

        this.init();
    }

    private void init() {
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

        // now setup ElasticSearch

        IndicesAdminClient indicesAdminClient = this.elasticsearchClient.admin().indices();
        indicesAdminClient.prepareCreate("twitter").get();
    }

    @Override
    public void run() {
        // Get twitter message from queue
        try {
            while (true) {
                // Receive messages
                //System.out.println("Receiving messages from MyQueue.\n");
                String queueUrl = sqs.getQueueUrl("fetched-tweets").getQueueUrl();
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
                    System.out.println("Deleting message with AWS id " + msg.getMessageId());
                    String messageReceiptHandle = msg.getReceiptHandle();
                    sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageReceiptHandle));
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void sendToElasticsearch(List<Message> messages) {
        BulkRequestBuilder bulkRequest = this.elasticsearchClient.prepareBulk();

        for (Message msg : messages) {
            System.out.println("Adding message with AWS id " + msg.getMessageId() + " to ES Bulk Index request");
            try {
                JSONObject tweetObj = (JSONObject) this.parser.parse(msg.getBody());
                Long tweetId = (Long) tweetObj.get("id");

                bulkRequest
                        .add(
                                this.elasticsearchClient.prepareIndex(Consumer.INDEX_NAME, Consumer.TYPE_NAME, tweetId.toString())
                                        .setSource(msg.getBody())
                        );
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        BulkResponse response = bulkRequest.get();
        if (response.hasFailures()) {
            System.err.println("Failed to index using bulk request: " + response.buildFailureMessage());
        }
    }

}
