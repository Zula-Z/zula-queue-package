package com.zula.queue.core;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;

import javax.annotation.PostConstruct;

public abstract class BaseMessageConsumer<T> {

    @Autowired
    @Lazy
    private QueueManager queueManager;

    @Value("${spring.application.name:unknown-service}")
    private String serviceName;

    private final String messageType;

    protected BaseMessageConsumer() {
        this.messageType = deriveMessageTypeFromGeneric();
    }

    protected BaseMessageConsumer(String customMessageType) {
        this.messageType = customMessageType.toLowerCase();
    }

    @PostConstruct
    public void init() {
        queueManager.createServiceQueue(serviceName, messageType);
        System.out.println("Zula: " + getClass().getSimpleName() + " listening on " + serviceName + "." + messageType);
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
                return convertClassNameToMessageType(simpleName);
            }
        } catch (Exception e) {
            System.out.println("Could not derive message type from generic, using class name");
        }

        return deriveMessageTypeFromClassName();
    }

    private String convertClassNameToMessageType(String className) {
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
        return serviceName + "." + messageType;
    }
}