package com.zula.queue.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class MessagePublisher {

    private final RabbitTemplate rabbitTemplate;
    private final QueueManager queueManager;
    private final ObjectMapper objectMapper;

    @Autowired(required = false)
    private QueuePersistenceService queuePersistenceService;

    @Value("${spring.application.name:unknown-service}")
    private String serviceName;

    @Autowired
    public MessagePublisher(QueueManager queueManager, RabbitTemplate rabbitTemplate) {
        this.queueManager = queueManager;
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Publish using defaults declared on the message class via @ZulaPublish.
     */
    public <T> void publish(T message) {
        publish(message, null);
    }

    public <T> void publish(T message, MessageInitiator initiator) {
        ZulaPublish publish = message.getClass().getAnnotation(ZulaPublish.class);
        if (publish == null) {
            throw new IllegalArgumentException("Message class " + message.getClass().getName()
                    + " is missing @ZulaPublish(service=...) to infer destination");
        }
        publishToService(publish.service(), deriveMessageType(message), publish.action(), message, initiator);
    }

    public <T> void publishToService(String serviceName, T message) {
        publishToService(serviceName, message, null);
    }

    public <T> void publishToService(String serviceName, T message, MessageInitiator initiator) {
        String messageType = deriveMessageType(message);
        publishToService(serviceName, messageType, "process", message, initiator);
    }

    public <T> void publishToService(String serviceName, String action, T message) {
        publishToService(serviceName, action, message, null);
    }

    public <T> void publishToService(String serviceName, String action, T message, MessageInitiator initiator) {
        String messageType = deriveMessageType(message);
        publishToService(serviceName, messageType, action, message, initiator);
    }

    public <T> void publishToService(String serviceName, String messageType, String action, T message) {
        publishToService(serviceName, messageType, action, message, null);
    }

    public <T> void publishToService(String serviceName, String messageType, String action, T message, MessageInitiator initiator) {
        String messageId = ensureRequestId(message);
        String exchange = queueManager.generateExchangeName(messageType);
        String routingKey = messageType.toLowerCase() + "." + action.toLowerCase();

        queueManager.createServiceQueue(serviceName, messageType);

        persistOutbox(messageId, messageType, serviceName, message, initiator);

        rabbitTemplate.convertAndSend(exchange, routingKey, message, msg -> {
            msg.getMessageProperties().setHeader("x-source-service", this.serviceName);
            msg.getMessageProperties().setHeader("x-message-id", messageId);
            msg.getMessageProperties().setHeader("x-message-type", messageType);
            applyInitiatorHeaders(msg, initiator);
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

    private void persistOutbox(
            String messageId,
            String messageType,
            String targetService,
            Object message,
            MessageInitiator initiator
    ) {
        if (queuePersistenceService == null) {
            return;
        }
        try {
            queuePersistenceService.persistOutbox(message, messageType, targetService, messageId, initiator);
        } catch (Exception ex) {
            System.out.println("Zula: Could not persist outbox message " + messageId + " - " + ex.getMessage());
        }
    }

    private void applyInitiatorHeaders(org.springframework.amqp.core.Message message, MessageInitiator initiator) {
        if (initiator == null) {
            return;
        }
        if (initiator.getType() != null) {
            message.getMessageProperties().setHeader(MessageMetadataHelper.HEADER_INITIATOR_TYPE, initiator.getType());
        }
        if (initiator.getId() != null) {
            message.getMessageProperties().setHeader(MessageMetadataHelper.HEADER_INITIATOR_ID, initiator.getId());
        }
        if (initiator.getName() != null) {
            message.getMessageProperties().setHeader(MessageMetadataHelper.HEADER_INITIATOR_NAME, initiator.getName());
        }
        String payload = toJson(initiator);
        if (payload != null) {
            message.getMessageProperties().setHeader(MessageMetadataHelper.HEADER_INITIATOR_PAYLOAD, payload);
        }
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception ignored) {
            return null;
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
