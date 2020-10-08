package com.fincatto.springbootgcp;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class SpringBootGcpApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootGcpApplication.class, args);
    }

    @Bean
    public Bucket getBucket() {
        final Storage storage = StorageOptions.getDefaultInstance().getService();
        final BucketInfo bucketInfo = BucketInfo.of("spring-boot-gcp");
        final Bucket bucket = storage.get(bucketInfo.getName());
        if (bucket == null) {
            return storage.create(bucketInfo);
        }
        return bucket;
    }
}
