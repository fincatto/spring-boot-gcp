package com.fincatto.springbootgcp;

import com.google.api.core.ApiFuture;
import com.google.api.gax.paging.Page;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SpringBootApplication
public class SpringBootGcpApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringBootGcpApplication.class);
    private static final BucketInfo BUCKET = BucketInfo.of("spring-boot-gcp");

    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException, TimeoutException {
        SpringApplication.run(SpringBootGcpApplication.class, args);
        testeBucket();
        testePubSub();
    }

    private static void testePubSub() throws InterruptedException, ExecutionException, IOException, TimeoutException {
        final TopicName topicName = TopicName.of("dfteste", "test-topic");
        testePubSubPubish(topicName);

    }

    private static void testePubSubPubish(TopicName topicName) throws InterruptedException, ExecutionException, TimeoutException, IOException {
       Publisher publisher = null;
        try {
            publisher = Publisher.newBuilder(topicName).build();

            final ByteString data = ByteString.copyFromUtf8("Oi mundo");
            final PubsubMessage pubsubMessage = PubsubMessage.newBuilder().putAttributes("att1", "v1").setData(data).build();
            final ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
            final String messageId = messageIdFuture.get(15, TimeUnit.SECONDS);
            LOGGER.info("Published message ID: " + messageId);
        } finally {
            if (publisher != null) {
                publisher.shutdown();
                publisher.awaitTermination(15, TimeUnit.SECONDS);
            }
        }
    }

    private static void testeBucket() {
        // teste bucket
        final Storage storage = StorageOptions.getDefaultInstance().getService();
        final Bucket bucket = getBucket(storage);

        //cria o arquivo
        final Blob arquivo = bucket.create("teste.txt", "testando a criacao de conteudos nem tao novos assim".getBytes(StandardCharsets.UTF_8));
        LOGGER.info("Obj '{}' criado com sucesso!", arquivo.getGeneratedId());

        //lista os arquivos do bucket
        final Page<Blob> objects = bucket.list();
        for (Blob object : objects.getValues()) {
            LOGGER.info("List: {}", object.getGeneratedId());
        }
    }

    private static Bucket getBucket(Storage storage) {
        final Bucket bucket = storage.get(BUCKET.getName());
        if (bucket == null) {
            return storage.create(BUCKET);
        }
        return bucket;
    }
}
