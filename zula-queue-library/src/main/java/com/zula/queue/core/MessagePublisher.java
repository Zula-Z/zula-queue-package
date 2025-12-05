package com.zula.queue.core;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;

@Component
public class MessagePublisher {

    private final RabbitTemplate rabbitTemplate;
    private final QueueManager queueManager;

    @Autowired(required = false)
    private QueuePersistenceService queuePersistenceService;

    @Value("${spring.application.name:unknown-service}")
    private String serviceName;

    @Autowired
    public MessagePublisher(QueueManager queueManager, RabbitTemplate rabbitTemplate) {
        this.queueManager = queueManager;
        this.rabbitTemplate = rabbitTemplate;
    }

    /**
     * Publish using defaults declared on the message class via @ZulaPublish.
     */
    public <T> void publish(T message) {
        ZulaPublish publish = message.getClass().getAnnotation(ZulaPublish.class);
        if (publish == null) {
            throw new IllegalArgumentException("Message class " + message.getClass().getName()
                    + " is missing @ZulaPublish(service=...) to infer destination");
        }
        publishToService(publish.service(), deriveMessageType(message), publish.action(), message);
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
        String messageId = ensureRequestId(message);
        String exchange = queueManager.generateExchangeName(messageType);
        String routingKey = messageType.toLowerCase() + "." + action.toLowerCase();

        queueManager.createServiceQueue(serviceName, messageType);

        persistOutbox(messageId, messageType, serviceName, message);

        rabbitTemplate.convertAndSend(exchange, routingKey, message, msg -> {
            msg.getMessageProperties().setHeader("x-source-service", this.serviceName);
            msg.getMessageProperties().setHeader("x-message-id", messageId);
            msg.getMessageProperties().setHeader("x-message-type", messageType);
            return msg;
        });

        System.out.println("Zula: Published " + messageType + " " + action + " to " + serviceName);
    }

    private <T> String deriveMessageType(T message) {
        Class<?> clazz = message.getClass();
        ZulaCommand command = clazz.getAnnotation(ZulaCommand.class);
        if (command != null && !command.commandType().isEmpty()) {
            return command.commandType().toLowerCase();
        }
        ZulaMessage annotation = clazz.getAnnotation(ZulaMessage.class);
        if (annotation != null && !annotation.messageType().isEmpty()) {
            return annotation.messageType().toLowerCase();
        }
        String className = clazz.getSimpleName();
        if (className.endsWith("Message")) {
            return className.substring(0, className.length() - 7).toLowerCase();
        }
        return className.toLowerCase();
    }

    private void persistOutbox(String messageId, String messageType, String targetService, Object message) {
        if (queuePersistenceService == null) {
            return;
        }
        try {
            queuePersistenceService.persistOutbox(message, messageType, targetService, messageId);
        } catch (Exception ex) {
            System.out.println("Zula: Could not persist outbox message " + messageId + " - " + ex.getMessage());
        }
    }

    private <T> String ensureRequestId(T message) {
        try {
            java.lang.reflect.Method getter = null;
            try {
                getter = message.getClass().getMethod("getRequestId");
            } catch (NoSuchMethodException ignored) { }

            Object current = getter != null ? getter.invoke(message) : null;
            if (current != null && current.toString().trim().length() > 0) {
                return current.toString();
            }

            String newId = java.util.UUID.randomUUID().toString();

            try {
                java.lang.reflect.Method setter = message.getClass().getMethod("setRequestId", String.class);
                setter.invoke(message, newId);
                return newId;
            } catch (NoSuchMethodException ignored) { }

            try {
                java.lang.reflect.Field field = message.getClass().getDeclaredField("requestId");
                field.setAccessible(true);
                field.set(message, newId);
                return newId;
            } catch (NoSuchFieldException ignored) { }

            return newId;
        } catch (Exception ex) {
            // best-effort; ignore errors
        }
        return java.util.UUID.randomUUID().toString();
    }
}
