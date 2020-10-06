package com.fincatto.springbootgcp;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.nio.charset.StandardCharsets;

@SpringBootApplication
public class SpringBootGcpApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringBootGcpApplication.class);
    private static final BucketInfo BUCKET = BucketInfo.of("spring-boot-gcp");

    public static void main(String[] args) {
        SpringApplication.run(SpringBootGcpApplication.class, args);

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
