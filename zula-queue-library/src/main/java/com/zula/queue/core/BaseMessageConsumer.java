package com.zula.queue.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zula.queue.core.ZulaCommand;
import com.zula.queue.core.ZulaMessage;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;

import jakarta.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;

public abstract class BaseMessageConsumer<T> {

    @Autowired
    @Lazy
    private QueueManager queueManager;

    @Value("${spring.application.name:unknown-service}")
    private String serviceName;

    @Autowired(required = false)
    private ConnectionFactory connectionFactory;

    @Autowired(required = false)
    private ObjectMapper objectMapper;

    @Autowired(required = false)
    private QueuePersistenceService queuePersistenceService;

    private final String messageType;
    private final Class<T> messageClass;

    protected BaseMessageConsumer() {
        this.messageType = deriveMessageTypeFromGeneric();
        this.messageClass = deriveMessageClassFromGeneric();
    }

    protected BaseMessageConsumer(String customMessageType) {
        this.messageType = customMessageType.toLowerCase();
        this.messageClass = deriveMessageClassFromGeneric();
    }

    @PostConstruct
    public void init() {
        String queueName = queueManager.generateQueueName(serviceName, messageType);
        queueManager.createServiceQueue(serviceName, messageType);
        System.out.println("Zula: " + getClass().getSimpleName() + " listening on " + queueName);

        // If a ConnectionFactory is available, create a listener container programmatically so
        // consuming services don't need to use @RabbitListener + SpEL on annotation attributes.
        if (connectionFactory != null) {
            SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
            container.setQueueNames(queueName);
            final ObjectMapper mapper = this.objectMapper != null ? this.objectMapper : new ObjectMapper();

            container.setMessageListener((Message message) -> {
                try {
                    byte[] body = message.getBody();
                    String rawPayload = new String(body, StandardCharsets.UTF_8);
                    if (messageClass != null) {
                        T obj = mapper.readValue(body, messageClass);
                        String messageId = MessageMetadataHelper.extractMessageId(message, obj);
                        String sourceService = MessageMetadataHelper.extractSourceService(message);
                        recordInbox(messageId, sourceService, rawPayload);
                        processMessage(obj);
                        markInboxProcessed(messageId);
                    } else {
                        System.out.println("Zula: Received message but cannot determine target class. Raw: " + rawPayload);
                    }
                } catch (Exception ex) {
                    System.err.println("Zula: Error processing message in " + getClass().getSimpleName());
                    ex.printStackTrace();
                }
            });

            // start the container
            container.start();
        } else {
            System.out.println("Zula: No ConnectionFactory available in context; consumer will not start a listener container. If you use @RabbitListener in the application, avoid SpEL on annotation attributes.");
        }
    }

    public abstract void processMessage(T message);

    @SuppressWarnings("unchecked")
    private String deriveMessageTypeFromGeneric() {
        try {
            java.lang.reflect.Type genericSuperclass = getClass().getGenericSuperclass();
            if (genericSuperclass instanceof java.lang.reflect.ParameterizedType) {
                java.lang.reflect.Type actualType = ((java.lang.reflect.ParameterizedType) genericSuperclass).getActualTypeArguments()[0];
                String className = actualType.getTypeName();

                String simpleName = className.substring(className.lastIndexOf('.') + 1);
                if (actualType instanceof Class<?>) {
                    Class<?> clazz = (Class<?>) actualType;
                    ZulaCommand commandAnnotation = clazz.getAnnotation(ZulaCommand.class);
                    if (commandAnnotation != null && !commandAnnotation.commandType().isEmpty()) {
                        return commandAnnotation.commandType().toLowerCase();
                    }
                    ZulaMessage messageAnnotation = clazz.getAnnotation(ZulaMessage.class);
                    if (messageAnnotation != null && !messageAnnotation.messageType().isEmpty()) {
                        return messageAnnotation.messageType().toLowerCase();
                    }
                }
                return convertClassNameToMessageType(simpleName);
            }
        } catch (Exception e) {
            System.out.println("Could not derive message type from generic, using class name");
        }

        return deriveMessageTypeFromClassName();
    }

    @SuppressWarnings("unchecked")
    private Class<T> deriveMessageClassFromGeneric() {
        try {
            java.lang.reflect.Type genericSuperclass = getClass().getGenericSuperclass();
            if (genericSuperclass instanceof java.lang.reflect.ParameterizedType) {
                java.lang.reflect.Type actualType = ((java.lang.reflect.ParameterizedType) genericSuperclass).getActualTypeArguments()[0];
                if (actualType instanceof Class) {
                    return (Class<T>) actualType;
                } else if (actualType instanceof java.lang.reflect.ParameterizedType) {
                    return (Class<T>) ((java.lang.reflect.ParameterizedType) actualType).getRawType();
                } else {
                    // Fallback: try to load by type name
                    String typeName = actualType.getTypeName();
                    return (Class<T>) Class.forName(typeName);
                }
            }
        } catch (Exception e) {
            // ignore - we'll return null below
        }
        return null;
    }

    private String convertClassNameToMessageType(String className) {
        if (className.endsWith("Command")) {
            return className.substring(0, className.length() - "Command".length()).toLowerCase();
        }
        if (className.endsWith("Message")) {
            return className.substring(0, className.length() - 7).toLowerCase();
        }
        return className.toLowerCase();
    }

    private String deriveMessageTypeFromClassName() {
        String className = getClass().getSimpleName();
        if (className.endsWith("MessageConsumer")) {
            return className.substring(0, className.length() - 15).toLowerCase();
        }
        if (className.endsWith("Consumer")) {
            return className.substring(0, className.length() - 8).toLowerCase();
        }
        return "default";
    }

    public String getMessageType() {
        return messageType;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getQueueName() {
        if (queueManager != null) {
            return queueManager.generateQueueName(serviceName, messageType);
        }
        return serviceName + "." + messageType;
    }

    public Class<T> getMessageClass() {
        return messageClass;
    }

    private void recordInbox(String messageId, String sourceService, String payload) {
        if (queuePersistenceService == null) {
            return;
        }
        try {
            queuePersistenceService.recordInboxReceived(messageId, messageType, sourceService, payload);
        } catch (Exception ex) {
            System.out.println("Zula: Could not persist inbox message " + messageId + " - " + ex.getMessage());
        }
    }

    private void markInboxProcessed(String messageId) {
        if (queuePersistenceService == null) {
            return;
        }
        try {
            queuePersistenceService.markInboxProcessed(messageId);
        } catch (Exception ex) {
            System.out.println("Zula: Could not mark inbox message " + messageId + " as processed - " + ex.getMessage());
        }
    }
}
