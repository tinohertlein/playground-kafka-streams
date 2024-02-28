package dev.hertlein.payground.kafkastreams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {

    private static final Logger logger
            = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        logger.info("Example log from {}", Main.class.getSimpleName());
    }
}
