package com.zula.queue.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zula.queue.core.ZulaCommand;
import com.zula.queue.core.ZulaMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;

import jakarta.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;

public abstract class BaseMessageConsumer<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseMessageConsumer.class);

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

    @Autowired(required = false)
    private DlqService dlqService;

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
        DeadLetterConfig deadLetterConfig = DeadLetterConfig.from(messageClass == null ? null : messageClass.getAnnotation(ZulaCommandRetry.class));
        ZulaHandlerRetry override = getClass().getAnnotation(ZulaHandlerRetry.class);
        deadLetterConfig = DeadLetterConfig.merge(deadLetterConfig, override);
        final DeadLetterConfig effectiveDeadLetterConfig = deadLetterConfig;

        queueManager.createServiceQueue(serviceName, messageType, effectiveDeadLetterConfig);
        LOGGER.info("Zula: {} listening on {}", getClass().getSimpleName(), queueName);

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
                        try {
                            processMessage(obj);
                            markInboxProcessed(messageId);
                        } catch (Exception handlerException) {
                            if (dlqService != null) {
                                dlqService.handleFailure(serviceName, messageType, effectiveDeadLetterConfig, message, obj, handlerException.getMessage());
                            }
                        }
                    } else {
                        LOGGER.warn("Zula: Received message but cannot determine target class. Raw: {}", rawPayload);
                    }
                } catch (Exception ex) {
                    LOGGER.error("Zula: Error processing message in {}", getClass().getSimpleName(), ex);
                }
            });

            // start the container
            container.start();
        } else {
            LOGGER.warn("Zula: No ConnectionFactory available in context; consumer will not start a listener container.");
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
            LOGGER.warn("Zula: Could not persist inbox message {} - {}", messageId, ex.getMessage());
        }
    }

    private void markInboxProcessed(String messageId) {
        if (queuePersistenceService == null) {
            return;
        }
        try {
            queuePersistenceService.markInboxProcessed(messageId);
        } catch (Exception ex) {
            LOGGER.warn("Zula: Could not mark inbox message {} as processed - {}", messageId, ex.getMessage());
        }
    }
}
