package com.stratio.numa;

import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;

/**
 * Created by rdiez on 15/02/18.
 */
public class NotificationProducerThread implements Runnable {

    Logger logger = LoggerFactory.getLogger(NotificationProducerThread.class);

    private final KafkaProducer producer;
    private final String topic;
    private final Long interval;

    public NotificationProducerThread(HashMap Map) {

        Properties prop = createProducerConfig(Map.get("broker-list").toString(),
                Map.get("keystore-secrets").toString(),
                Map.get("keystore-password").toString(),
                Map.get("truststore-secrets").toString(),
                Map.get("truststore-password").toString());

        this.producer = new KafkaProducer<String,String>(prop);
        this.topic = Map.get("topic").toString();
        this.interval = Long.valueOf(Map.get("interval-time").toString());
    }

    private static Properties createProducerConfig(String brokers,
                                                   String keystore,
                                                   String keypass,
                                                   String truststore,
                                                   String trustpass) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers); // 9092
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 100);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("offsets.topic.replication.factor",1);
        // configure the following three settings for SSL Authentication
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustpass);
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystore);
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keypass);

        return props;
    }

    @Override
    public void run() {
        while(true) {
            String msg = "Message to be send";
            producer.send(new ProducerRecord<String, String>(topic, msg), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null)
                        logger.error("Exception",e);
                    else
                        logger.info("PRODUCER msg:" + msg + ", Offset: " + metadata.offset()
                                + ", topic: " + metadata.topic() + ", partition: " + metadata.partition()
                                +", timestamp: " + metadata.timestamp() );
                }
            });
            try {
                logger.debug("Sleep Interval: " + interval + " ...");
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
                producer.close();
            }
        }
    }
}
