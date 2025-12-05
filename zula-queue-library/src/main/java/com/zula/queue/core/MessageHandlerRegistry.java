package com.zula.queue.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zula.queue.core.ZulaCommand;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * Provides a composition-based way to register handlers without subclassing.
 *
 * Usage in a service:
 * <pre>
 *   @Component
 *   public class AuthResponseHandler {
 *       private final MessageHandlerRegistry registry;
 *
 *       public AuthResponseHandler(MessageHandlerRegistry registry) {
 *           this.registry = registry;
 *       }
 *
 *       @PostConstruct
 *       void init() {
 *           registry.register(AuthResponseMessage.class, this::handleResponse);
 *       }
 *
 *       void handleResponse(AuthResponseMessage msg) { ... }
 *   }
 * </pre>
 */
@Component
public class MessageHandlerRegistry {

    private final QueueManager queueManager;
    private final ConnectionFactory connectionFactory;
    private final ObjectMapper objectMapper;
    private final org.springframework.core.env.Environment environment;
    private final QueuePersistenceService queuePersistenceService;

    public MessageHandlerRegistry(QueueManager queueManager,
                                  ConnectionFactory connectionFactory,
                                  ObjectMapper objectMapper,
                                  org.springframework.core.env.Environment environment,
                                  @org.springframework.beans.factory.annotation.Autowired(required = false) QueuePersistenceService queuePersistenceService) {
        this.queueManager = queueManager;
        this.connectionFactory = connectionFactory;
        this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
        this.environment = environment;
        this.queuePersistenceService = queuePersistenceService;
    }

    public <T> void register(Class<T> messageClass, Consumer<T> handler) {
        Assert.notNull(messageClass, "messageClass must not be null");
        Assert.notNull(handler, "handler must not be null");
        String messageType = deriveMessageType(messageClass);
        register(messageType, messageClass, handler);
    }

    public <T> void register(String messageType, Class<T> messageClass, Consumer<T> handler) {
        Assert.hasText(messageType, "messageType must not be empty");
        Assert.notNull(messageClass, "messageClass must not be null");
        Assert.notNull(handler, "handler must not be null");

        String serviceName = environment.getProperty("spring.application.name", "unknown-service");
        String queueName = queueManager.generateQueueName(serviceName, messageType);

        queueManager.createServiceQueue(serviceName, messageType);
        System.out.println("Zula: registering handler for " + queueName);

        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueueNames(queueName);
        container.setMessageListener((Message message) -> {
            try {
                byte[] body = message.getBody();
                String rawPayload = new String(body, StandardCharsets.UTF_8);
                T obj = objectMapper.readValue(body, messageClass);
                String messageId = MessageMetadataHelper.extractMessageId(message, obj);
                String sourceService = MessageMetadataHelper.extractSourceService(message);
                recordInbox(messageId, messageType, sourceService, rawPayload);
                handler.accept(obj);
                markInboxProcessed(messageId);
            } catch (Exception ex) {
                System.err.println("Zula: Error processing message for " + queueName);
                ex.printStackTrace();
                String raw = new String(message.getBody(), StandardCharsets.UTF_8);
                System.err.println("Raw message: " + raw);
            }
        });
        container.start();
    }

    private String deriveMessageType(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            return deriveMessageType(clazz);
        } catch (ClassNotFoundException e) {
            // fall through
        }
        if (className.endsWith("Message")) {
            return className.substring(0, className.length() - 7).toLowerCase();
        }
        return className.toLowerCase();
    }

    private String deriveMessageType(Class<?> clazz) {
        ZulaCommand commandAnnotation = clazz.getAnnotation(ZulaCommand.class);
        if (commandAnnotation != null && !commandAnnotation.commandType().isEmpty()) {
            return commandAnnotation.commandType().toLowerCase();
        }
        ZulaMessage annotation = clazz.getAnnotation(ZulaMessage.class);
        if (annotation != null && !annotation.messageType().isEmpty()) {
            return annotation.messageType().toLowerCase();
        }
        String simpleName = clazz.getSimpleName();
        if (simpleName.endsWith("Command")) {
            return simpleName.substring(0, simpleName.length() - "Command".length()).toLowerCase();
        }
        if (simpleName.endsWith("Message")) {
            return simpleName.substring(0, simpleName.length() - 7).toLowerCase();
        }
        return simpleName.toLowerCase();
    }

    private void recordInbox(String messageId, String messageType, String sourceService, String payload) {
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
