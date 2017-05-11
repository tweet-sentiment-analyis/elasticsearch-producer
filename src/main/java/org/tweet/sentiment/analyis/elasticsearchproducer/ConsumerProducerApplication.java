package org.tweet.sentiment.analyis.elasticsearchproducer;

import org.tweet.sentiment.analyis.elasticsearchproducer.elasticsearch.ConsumerProducerComponent;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class ConsumerProducerApplication {

    private static final Logger logger          = Logger.getLogger(ConsumerProducerApplication.class.getName());
    private static final String NO_THREADS      = "NO_THREADS";
    private static final int    DEFAULT_THREADS = 1;

    private static ExecutorService executorService;

    public static void main(String[] args) {
        logger.info("Starting elasticsearch producer...");
        logger.info("Looking for environment variable '" + NO_THREADS + "' for number of threads...");

        int noOfThreads = DEFAULT_THREADS;
        if (null == System.getenv(NO_THREADS)) {
            logger.warning("Did not found environment variable specifying no. of threads. Assuming " + DEFAULT_THREADS);
        } else {
            logger.info("Found environment variable specifying no. of threads");
            noOfThreads = Integer.parseInt(System.getenv(NO_THREADS));
        }

        logger.info("Starting elasticsearch producer using '" + noOfThreads + "' threads.");
        executorService = Executors.newFixedThreadPool(noOfThreads);

        // starting dynamic number of same thread
        for (int i = 0; i < noOfThreads; i++) {

            try {
                ConsumerProducerComponent consumerProducerComponentProducer = new ConsumerProducerComponent();
                executorService.submit(consumerProducerComponentProducer);
            } catch (IOException e) {
                logger.severe("Failed to start elasticsearch producer: " + e.getMessage());
            }
        }
    }
}
