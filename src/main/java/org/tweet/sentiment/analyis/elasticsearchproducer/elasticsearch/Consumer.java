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

import java.util.List;

public class Consumer extends Thread {

    private AmazonSQS sqs;

    public Consumer() {

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

                System.out.println("Message");
                System.out.println("MessageId:     " + messages.get(0).getMessageId());
                System.out.println("Body:          " + messages.get(0).getBody());


                // Delete a message
                System.out.println("Deleting a message.\n");
                String messageReceiptHandle = messages.get(0).getReceiptHandle();
                sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageReceiptHandle));

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
