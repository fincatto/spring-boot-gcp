package com.fincatto.springbootgcp;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class Producer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);
    private static final String TOPIC = "test-topic";

    private final Bucket bucket;
    private final PubSubTemplate template;

    public Producer(final Bucket bucket, final PubSubTemplate template) {
        this.bucket = bucket;
        this.template = template;
    }

    @Scheduled(fixedDelay = 5000)
    public void scheduled() throws InterruptedException, ExecutionException, TimeoutException {
        //cria o arquivo
        final String fileName = String.format("%s.txt", System.nanoTime());
        final String fileContents = LocalDateTime.now().toString();
        final Blob arquivo = bucket.create(fileName, fileContents.getBytes(StandardCharsets.UTF_8));

        //mando mensagem para o vivente processar
        final PubsubMessage message = PubsubMessage.newBuilder()
                .putAttributes("name", arquivo.getName())
                .putAttributes("md5", arquivo.getMd5ToHexString())
                .setData(ByteString.copyFromUtf8(fileContents))
                .build();

        final ListenableFuture<String> messageIdFuture = this.template.publish(TOPIC, message);
        final String messageID = messageIdFuture.get(15, TimeUnit.SECONDS);
        LOGGER.info("[{}] Sent", messageID);
    }
}
