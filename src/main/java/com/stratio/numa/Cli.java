package com.stratio.numa;

/**
 * Created by rdiez on 15/02/18.
 */

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class Cli {

    private static final Logger logger = LoggerFactory.getLogger(Cli.class);
    private String[] args = null;
    private Options options = new Options();

    public Cli(String[] args) {
        this.args = args;
        options.addOption("h", "help", false, "Show class options");
        options.addOption("topic", "topic-name", true, "Topic name");
        options.addOption("group", "group-id", true, "Group id");
        options.addOption("brokers", "brokers-list", true, "Kafka brokers list");
        options.addOption("keystore","keystore-secrets",true,"Keystore needed to access kafka cluster");
        options.addOption("keypass","keystore-password",true,"Keystore password");
        options.addOption("truststore","truststore-secrets",true,"Truststore needed to access kafka cluster");
        options.addOption("trustpass","truststore-password",true,"Truststore password");
        options.addOption("interval","interval-time",true,"Interval time in milliseconds between messages");
    }

    public void parse(HashMap map) {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("h"))
                help();

            if (cmd.hasOption("topic")) {
                logger.info("Using cli argument -topic=" + cmd.getOptionValue("topic"));
                map.put("topic", cmd.getOptionValue("topic"));
            } else {
                logger.error("Missing topic option");
                help();
            }
            if (cmd.hasOption("group")) {
                logger.info("Using cli argument -group=" + cmd.getOptionValue("group"));
                map.put("groupid", cmd.getOptionValue("group"));
            } else {
                logger.error("Missing group option");
                help();
            }
            if (cmd.hasOption("brokers")) {
                logger.info("Using cli argument -brokers=" + cmd.getOptionValue("brokers"));
                map.put("broker-list", cmd.getOptionValue("brokers"));
            } else {
                logger.error("Missing brokers option");
                help();
            }
            if (cmd.hasOption("truststore")) {
                logger.info("Using cli argument -truststore=" + cmd.getOptionValue("truststore"));
                map.put("truststore-secrets", cmd.getOptionValue("truststore"));
            } else {
                logger.error("Truststore needed to access kafka cluster");
                help();
            }
            if (cmd.hasOption("trustpass")) {
                logger.info("Using cli argument -trustpass=" + cmd.getOptionValue("trustpass"));
                map.put("truststore-password", cmd.getOptionValue("trustpass"));
            } else {
                logger.error("Truststore password needed to access kafka cluster");
                help();
            }
            if (cmd.hasOption("keystore")) {
                logger.info("Using cli argument -keystore=" + cmd.getOptionValue("keystore"));
                map.put("keystore-secrets", cmd.getOptionValue("keystore"));
            } else {
                logger.error("Keystore needed to access kafka cluster");
                help();
            }
            if (cmd.hasOption("keypass")) {
                logger.info("Using cli argument -keypass=" + cmd.getOptionValue("keypass"));
                map.put("keystore-password", cmd.getOptionValue("keypass"));
            } else {
                logger.error("Keystore password needed to access kafka cluster");
                help();
            }
            if (cmd.hasOption("interval")) {
                logger.info("Using cli argument -interval=" + cmd.getOptionValue("interval"));
                map.put("interval-time", cmd.getOptionValue("interval"));
            } else {
                logger.error("Missing interval time option");
                help();
            }

        } catch (ParseException e) {
            logger.error("Failed to parse command line properties", e);
            help();
        }
    }

    private void help() {
        HelpFormatter formater = new HelpFormatter();
        formater.printHelp("Main", options);
        System.exit(0);
    }

}

