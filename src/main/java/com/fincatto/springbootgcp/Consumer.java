package com.fincatto.springbootgcp;

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

    public Consumer(PubSubTemplate template) {
        this.template = template;
    }

    @Override
    public void run(ApplicationArguments args) {
        this.template.subscribe(SUBSCRITION, basicAcknowledgeablePubsubMessage -> {
            final PubsubMessage message = basicAcknowledgeablePubsubMessage.getPubsubMessage();
            LOGGER.info("[{}] {} - {}", message.getMessageId(), message.getData().toStringUtf8(), message.getAttributesMap());
            basicAcknowledgeablePubsubMessage.ack();
        });
    }
}