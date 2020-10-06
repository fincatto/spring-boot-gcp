package com.fincatto.springbootgcp;

import com.google.api.core.ApiFuture;
import com.google.api.gax.paging.Page;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.storage.*;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
        testePubSubPubish(TopicName.of("dfteste", "test-topic"));
        testePubSubReceive(ProjectSubscriptionName.of("dfteste", "teste-pull"));
    }

    private static void testePubSubReceive(ProjectSubscriptionName subscriptionName) {
        // Instantiate an asynchronous message receiver.
        MessageReceiver receiver =
                (PubsubMessage message, AckReplyConsumer consumer) -> {
                    LOGGER.info("S[{}] {}", message.getMessageId(), message.getData().toStringUtf8());
                    consumer.ack();
                };

        Subscriber subscriber = null;
        try {
            subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
            subscriber.startAsync().awaitRunning();
            LOGGER.info("Listening for messages on '{}':", subscriptionName.toString());
            subscriber.awaitTerminated(30, TimeUnit.SECONDS);
        } catch (TimeoutException timeoutException) {
            subscriber.stopAsync();
        }
    }

    private static void testePubSubPubish(TopicName topicName) throws InterruptedException, ExecutionException, TimeoutException, IOException {
        Publisher publisher = null;
        try {
            publisher = Publisher.newBuilder(topicName).build();

            final ByteString data = ByteString.copyFromUtf8("Oi mundo");
            final PubsubMessage pubsubMessage = PubsubMessage.newBuilder().putAttributes("att1", "v1").setData(data).build();
            final ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
            final String messageId = messageIdFuture.get(15, TimeUnit.SECONDS);
            LOGGER.info("P[{}] {} ", messageId, data.toStringUtf8());
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
