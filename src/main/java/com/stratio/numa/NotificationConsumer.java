package com.stratio.numa;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;

/**
 * Created by rdiez on 15/02/18.
 */
public class NotificationConsumer {

    Logger logger = LoggerFactory.getLogger(NotificationConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final Long interval;
    private ExecutorService executor;
    private final String keystore;
    private final String keypass;
    private final String truststore;
    private final String trustpass;


    public NotificationConsumer(HashMap Map) {

        this.keystore = Map.get("keystore-secrets").toString();
        this.keypass = Map.get("keystore-password").toString();
        this.truststore = Map.get("truststore-secrets").toString();
        this.trustpass = Map.get("truststore-password").toString();

        Properties prop = createConsumerConfig(Map.get("broker-list").toString(),
                Map.get("groupid").toString(),
                this.keystore,
                this.keypass,
                this.truststore,
                this.trustpass);

        this.consumer = new KafkaConsumer<>(prop);
        this.topic = Map.get("topic").toString();
        this.interval = Long.valueOf(Map.get("interval-time").toString());
        this.consumer.subscribe(Arrays.asList(this.topic));

    }

    /**
     * Creates a {@link ThreadPoolExecutor} with a given number of threads to consume the messages
     * from the broker.
     *
     * @param numberOfThreads The number of threads will be used to consume the message
     */
    public void execute(int numberOfThreads) {

        executor = new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

        while (true) {
            try
            {
                ConsumerRecords<String, String> records = consumer.poll(interval);
                for (final ConsumerRecord record : records) {
                    executor.submit(new ConsumerThreadHandler(record));
                }
            }catch(RuntimeException e) {
                logger.error("Error capturado: " + e.getMessage());
            }
        }
    }

    private static Properties createConsumerConfig(String brokers,
                                                   String groupId,
                                                   String keystore,
                                                   String keypass,
                                                   String truststore,
                                                   String trustpass) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // configure the following three settings for SSL Authentication
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustpass);
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystore);
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keypass);

        return props;
    }

    public void shutdown() {
        if (consumer != null) {
            consumer.close();
        }
        if (executor != null) {
            executor.shutdown();
        }
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                logger.info("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted during shutdown, exiting uncleanly");
        }
    }

}
