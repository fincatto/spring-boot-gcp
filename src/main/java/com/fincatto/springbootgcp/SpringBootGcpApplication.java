package com.fincatto.springbootgcp;

import com.google.cloud.storage.*;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

@SpringBootApplication
public class SpringBootGcpApplication implements CommandLineRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringBootGcpApplication.class);
    private static final BucketInfo BUCKET = BucketInfo.of("spring-boot-gcp");

    @Autowired
    private Publisher publisher;

    @Autowired
    private Bucket bucket;

    public static void main(String[] args) {
        SpringApplication.run(SpringBootGcpApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        while (true) {
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
            final String messageID = publisher.publish(message);
            LOGGER.info("[{}] Sent", messageID);
            Thread.sleep(5000);
        }
    }

    @Bean
    public Bucket getBucket() {
        final Storage storage = StorageOptions.getDefaultInstance().getService();
        final Bucket bucket = storage.get(BUCKET.getName());
        if (bucket == null) {
            return storage.create(BUCKET);
        }
        return bucket;
    }
}
