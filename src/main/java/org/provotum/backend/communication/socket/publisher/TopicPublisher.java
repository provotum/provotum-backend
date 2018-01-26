package org.provotum.backend.communication.socket.publisher;

import org.provotum.backend.communication.message.base.AResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.logging.Logger;

@Component
public class TopicPublisher {

    public static final String DEPLOYMENT_TOPIC = "/topic/deployments";
    public static final String REMOVAL_TOPIC = "/topic/removals";
    public static final String EVENT_TOPIC = "/topic/events";
    public static final String VOTE_TOPIC = "/topic/votes";
    public static final String META_TOPIC = "/topic/meta";

    private static final Logger logger = Logger.getLogger(TopicPublisher.class.getName());
    private final SimpMessagingTemplate messageTemplate;

    @Autowired
    public TopicPublisher(SimpMessagingTemplate messageTemplate) {
        this.messageTemplate = messageTemplate;
    }

    public void send(String topic, AResponse response) {
        Random rnd = new Random();
        int timeout = rnd.nextInt(2);

        logger.info("Simulating a waiting operation of " + timeout + "s before sending to topic");

        try {
            Thread.sleep(timeout * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        logger.info("Sending response with id " + response.getId() + " to topic " + topic);
        this.messageTemplate.convertAndSend(topic, response);
    }

}
