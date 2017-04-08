package org.tweet.sentiment.analyis.elasticsearchproducer;

import org.tweet.sentiment.analyis.elasticsearchproducer.elasticsearch.Consumer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ElasticSearchProducer {

    private static ExecutorService executorService = Executors.newSingleThreadExecutor();

    public static void main(String[] strings)
            throws IOException {

        Consumer consumer = new Consumer();

        executorService.submit(consumer);

    }
}
