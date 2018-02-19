package com.stratio.numa;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Created by rdiez on 15/02/18.
 */
public class ConsumerThreadHandler implements Runnable {

    Logger logger = LoggerFactory.getLogger(ConsumerThreadHandler.class);

    private ConsumerRecord consumerRecord;

    public ConsumerThreadHandler(ConsumerRecord consumerRecord) {
        this.consumerRecord = consumerRecord;
    }

    public void run() {
        logger.info("CONSUMER msg: " + consumerRecord.value() + ", Offset: " + consumerRecord.offset()
                + ", By ThreadID: " + Thread.currentThread().getId()
                + ", topic: " + consumerRecord.topic() + ", partition: " + consumerRecord.partition()
                + ", timestamp: " + consumerRecord.timestamp() );
    }
}
