package com.zula.queue.core;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class DlqService {

    private final QueueManager queueManager;
    private final RabbitTemplate rabbitTemplate;

    @Autowired(required = false)
    private DlqPersistenceService dlqPersistenceService;

    public DlqService(QueueManager queueManager, RabbitTemplate rabbitTemplate) {
        this.queueManager = queueManager;
        this.rabbitTemplate = rabbitTemplate;
    }

    public void handleFailure(
            String serviceName,
            String messageType,
            DeadLetterConfig dlqConfig,
            Message springMessage,
            Object deserializedPayload,
            String errorMessage
    ) {
        if (dlqConfig == null || !dlqConfig.isEnabled()) {
            return;
        }

        int currentRetry = extractRetryCount(springMessage);
        if (dlqConfig.isRetryable() && currentRetry < dlqConfig.getMaxRetries()) {
            republishForRetry(serviceName, messageType, springMessage, currentRetry + 1, dlqConfig.getRetryDelayMs());
        } else {
            routeToDlq(serviceName, messageType, springMessage, deserializedPayload, currentRetry,
                    dlqConfig.getMaxRetries(), dlqConfig.getRetryDelayMs(), errorMessage);
        }
    }

    public boolean replay(String messageId) {
        if (dlqPersistenceService == null) {
            return false;
        }

        DlqRecord record = dlqPersistenceService.find(messageId);
        if (record == null) {
            return false;
        }

        String exchange = queueManager.generateExchangeName(record.getMessageType());
        String routingKey = record.getRoutingKey() != null && !record.getRoutingKey().isBlank()
                ? record.getRoutingKey()
                : record.getMessageType() + ".process";

        try {
            Object replayPayload = record.getPayload();
            try {
                replayPayload = new com.fasterxml.jackson.databind.ObjectMapper().readValue(record.getPayload(), Map.class);
            } catch (Exception ignored) {
                // fall back to raw payload
            }

            rabbitTemplate.convertAndSend(exchange, routingKey, replayPayload, message -> {
                message.getMessageProperties().setHeader("x-zula-retry-count", 0);
                message.getMessageProperties().setHeader("x-message-type", record.getMessageType());
                message.getMessageProperties().setHeader("x-message-id", record.getMessageId());
                message.getMessageProperties().setHeader("x-source-service", record.getServiceName());
                return message;
            });

            dlqPersistenceService.delete(messageId);
            dlqPersistenceService.logReplay(record.getMessageId(), record.getMessageType(), record.getQueueName(),
                    record.getServiceName(), "replay", null);
            return true;
        } catch (Exception exception) {
            return false;
        }
    }

    public int replayByQueue(String queueName, int limit) {
        if (dlqPersistenceService == null) {
            return 0;
        }
        int replayed = 0;
        for (DlqRecord record : dlqPersistenceService.listByQueueForReplay(queueName, limit)) {
            if (replay(record.getMessageId())) {
                replayed++;
            }
        }
        return replayed;
    }

    public int replayByService(String serviceName, int limit) {
        if (dlqPersistenceService == null) {
            return 0;
        }
        int replayed = 0;
        for (DlqRecord record : dlqPersistenceService.listByServiceForReplay(serviceName, limit)) {
            if (replay(record.getMessageId())) {
                replayed++;
            }
        }
        return replayed;
    }

    public int delete(String messageId) {
        return dlqPersistenceService == null ? 0 : dlqPersistenceService.delete(messageId);
    }

    public int deleteByQueue(String queueName) {
        return dlqPersistenceService == null ? 0 : dlqPersistenceService.deleteByQueue(queueName);
    }

    public int deleteByService(String serviceName) {
        return dlqPersistenceService == null ? 0 : dlqPersistenceService.deleteByService(serviceName);
    }

    private void republishForRetry(String serviceName, String messageType, Message original, int retryCount, long delayMs) {
        String queueName = queueManager.generateQueueName(serviceName, messageType);
        if (delayMs > 0) {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }
        rabbitTemplate.send(queueName, enrichRetry(original, retryCount));
    }

    private void routeToDlq(
            String serviceName,
            String messageType,
            Message original,
            Object deserializedPayload,
            int retryCount,
            int maxRetries,
            long retryDelayMs,
            String errorMessage
    ) {
        String dlx = queueManager.generateDeadLetterExchangeName(messageType);
        String routingKey = messageType + ".dlq";
        Message outbound = enrichRetry(original, retryCount);
        queueManager.createServiceQueue(serviceName, messageType, new DeadLetterConfig(true, true, maxRetries, 0));
        rabbitTemplate.send(dlx, routingKey, outbound);

        if (dlqPersistenceService != null) {
            String queueName = queueManager.generateQueueName(serviceName, messageType);
            String messageId = MessageMetadataHelper.extractMessageId(original, deserializedPayload);
            String headersJson = toJson(original.getMessageProperties().getHeaders());
            String receivedRoutingKey = original.getMessageProperties().getReceivedRoutingKey();
            dlqPersistenceService.record(messageId, messageType, queueName, serviceName,
                    deserializedPayload != null ? deserializedPayload : new String(original.getBody()),
                    headersJson, receivedRoutingKey, retryDelayMs, errorMessage, retryCount, maxRetries, "DLQ");
        }
    }

    private Message enrichRetry(Message original, int retryCount) {
        original.getMessageProperties().setHeader("x-zula-retry-count", retryCount);
        return original;
    }

    private int extractRetryCount(Message message) {
        Object header = message.getMessageProperties().getHeaders().get("x-zula-retry-count");
        if (header == null) {
            return 0;
        }
        try {
            return Integer.parseInt(header.toString());
        } catch (NumberFormatException exception) {
            return 0;
        }
    }

    private String toJson(Object value) {
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(value);
        } catch (Exception exception) {
            return "{}";
        }
    }
}
