package com.stratio.numa;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Created by rdiez on 15/02/18.
 */
public class Main {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(Main.class);
        HashMap map = new HashMap();

        new Cli(args).parse(map);

        int numberOfThread = 1;

        // Start Notification Producer Thread
        NotificationProducerThread producerThread = new NotificationProducerThread(map);
        Thread t1 = new Thread(producerThread);
        t1.start();

        // Start group of Notification Consumer Thread
        NotificationConsumer consumers = new NotificationConsumer(map);

        consumers.execute(numberOfThread);

    }
}
