package com.fincatto.springbootgcp;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@SpringBootApplication
public class SpringBootGcpApplication implements CommandLineRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringBootGcpApplication.class);
    //private static final BucketInfo BUCKET = BucketInfo.of("spring-boot-gcp");

    @Autowired
    private Publisher publisher;

    public static void main(String[] args) {
        SpringApplication.run(SpringBootGcpApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        while (true) {
            final PubsubMessage message = PubsubMessage.newBuilder()
                    .putAttributes("at1", "av1")
                    .setData(ByteString.copyFromUtf8(String.format("Oi mundo: %s", LocalDateTime.now())))
                    .build();
            final String messageID = publisher.publish(message);
            LOGGER.info("[{}] Sent", messageID);
            Thread.sleep(5000);
        }
    }
//
//    @Bean
//    public Publisher createPublisher() throws IOException {
//        return Publisher.newBuilder(TopicName.of("dfteste", "test-topic")).build();
//    }
//
//    @Bean
//    @Autowired
//    public Subscriber createConsumer(MessageReceiver receiver) {
//        final ProjectSubscriptionName name = ProjectSubscriptionName.of("dfteste", "teste-pull");
//        Subscriber subscriber = Subscriber.newBuilder(name, receiver).build();
//        subscriber.startAsync().awaitRunning();
//        return subscriber;
//    }
//
//    @Bean
//    public MessageReceiver createReceiver() {
//        return (PubsubMessage message, AckReplyConsumer consumer) -> {
//            LOGGER.info("S[{}] {}", message.getMessageId(), message.getData().toStringUtf8());
//            consumer.ack();
//        };
//    }
//
//
//    private static void testePubSub() throws InterruptedException, ExecutionException, IOException, TimeoutException {
//        testePubSubPubish(TopicName.of("dfteste", "test-topic"));
//        testePubSubReceive(ProjectSubscriptionName.of("dfteste", "teste-pull"));
//    }
//
//    private static void testePubSubReceive(ProjectSubscriptionName subscriptionName) {
//        // Instantiate an asynchronous message receiver.
//        MessageReceiver receiver =
//                (PubsubMessage message, AckReplyConsumer consumer) -> {
//                    LOGGER.info("S[{}] {}", message.getMessageId(), message.getData().toStringUtf8());
//                    consumer.ack();
//                };
//
//        Subscriber subscriber = null;
//        try {
//            subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
//            subscriber.startAsync().awaitRunning();
//            LOGGER.info("Listening for messages on '{}':", subscriptionName.toString());
//            subscriber.awaitTerminated(30, TimeUnit.SECONDS);
//        } catch (TimeoutException timeoutException) {
//            subscriber.stopAsync();
//        }
//    }
//
//    private static void testePubSubPubish(TopicName topicName) throws InterruptedException, ExecutionException, TimeoutException, IOException {
//        Publisher publisher = null;
//        try {
//            publisher = Publisher.newBuilder(topicName).build();
//            final PubsubMessage pubsubMessage = PubsubMessage.newBuilder().putAttributes("att1", "v1").setData(ByteString.copyFromUtf8("Oi mundo")).build();
//            final ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
//            final String messageId = messageIdFuture.get(15, TimeUnit.SECONDS);
//            LOGGER.info("P[{}] {} ", messageId, pubsubMessage.getData().toStringUtf8());
//        } finally {
//            if (publisher != null) {
//                publisher.shutdown();
//                publisher.awaitTermination(15, TimeUnit.SECONDS);
//            }
//        }
//    }
//
//    private static void testeBucket() {
//        // teste bucket
//        final Storage storage = StorageOptions.getDefaultInstance().getService();
//        final Bucket bucket = getBucket(storage);
//
//        //cria o arquivo
//        final Blob arquivo = bucket.create("teste.txt", "testando a criacao de conteudos nem tao novos assim".getBytes(StandardCharsets.UTF_8));
//        LOGGER.info("Obj '{}' criado com sucesso!", arquivo.getGeneratedId());
//
//        //lista os arquivos do bucket
//        final Page<Blob> objects = bucket.list();
//        for (Blob object : objects.getValues()) {
//            LOGGER.info("List: {}", object.getGeneratedId());
//        }
//    }
//
//    private static Bucket getBucket(Storage storage) {
//        final Bucket bucket = storage.get(BUCKET.getName());
//        if (bucket == null) {
//            return storage.create(BUCKET);
//        }
//        return bucket;
//    }

//    @Autowired
//    private Publisher publisher;


}
