package com.fincatto.springbootgcp;

import com.google.pubsub.v1.PubsubMessage;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class Publisher {

    private static final String TOPIC = "test-topic";
    private final PubSubTemplate template;

    public Publisher(final PubSubTemplate template) {
        this.template = template;
    }

    public String publish(final String mensagem) throws InterruptedException, ExecutionException, TimeoutException {
        final ListenableFuture<String> messageIdFuture = this.template.publish(TOPIC, mensagem);
        return messageIdFuture.get(15, TimeUnit.SECONDS);
    }

    public String publish(final PubsubMessage pubsubMessage) throws InterruptedException, ExecutionException, TimeoutException {
        final ListenableFuture<String> messageIdFuture = this.template.publish(TOPIC, pubsubMessage);
        return messageIdFuture.get(15, TimeUnit.SECONDS);
    }
}