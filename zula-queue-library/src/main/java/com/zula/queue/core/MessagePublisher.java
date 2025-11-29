package com.zula.queue.core;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MessagePublisher {

    private final RabbitTemplate rabbitTemplate;
    private final QueueManager queueManager;

    @Autowired
    public MessagePublisher(QueueManager queueManager, RabbitTemplate rabbitTemplate) {
        this.queueManager = queueManager;
        this.rabbitTemplate = rabbitTemplate;
    }

    public <T> void publishToService(String serviceName, T message) {
        String messageType = deriveMessageType(message);
        publishToService(serviceName, messageType, "process", message);
    }

    public <T> void publishToService(String serviceName, String action, T message) {
        String messageType = deriveMessageType(message);
        publishToService(serviceName, messageType, action, message);
    }

    public <T> void publishToService(String serviceName, String messageType, String action, T message) {
        String exchange = queueManager.generateExchangeName(messageType);
        String routingKey = messageType.toLowerCase() + "." + action.toLowerCase();

        queueManager.createServiceQueue(serviceName, messageType);

        rabbitTemplate.convertAndSend(exchange, routingKey, message);

        System.out.println("Zula: Published " + messageType + " " + action + " to " + serviceName);
    }

    private <T> String deriveMessageType(T message) {
        String className = message.getClass().getSimpleName();

        if (className.endsWith("Message")) {
            return className.substring(0, className.length() - 7).toLowerCase();
        }
        return className.toLowerCase();
    }
}