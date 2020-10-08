package com.fincatto.springbootgcp;

import com.google.cloud.storage.Bucket;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.stereotype.Component;

@Component
public class Consumer implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
    private static final String SUBSCRITION = "teste-pull";
    private final PubSubTemplate template;
    private final Bucket bucket;

    public Consumer(final Bucket bucket, final PubSubTemplate template) {
        this.template = template;
        this.bucket = bucket;
    }

    @Override
    public void run(ApplicationArguments args) {
        this.template.subscribe(SUBSCRITION, basicAcknowledgeablePubsubMessage -> {
            final PubsubMessage message = basicAcknowledgeablePubsubMessage.getPubsubMessage();
            LOGGER.info("[{}] {} - {}", message.getMessageId(), message.getData().toStringUtf8(), message.getAttributesMap());
            bucket.get(message.getAttributesOrThrow("name")).delete();
            basicAcknowledgeablePubsubMessage.ack();
        });
    }
}