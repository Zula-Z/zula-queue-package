package com.zula.queue.testkit;

import com.zula.queue.config.QueueProperties;
import com.zula.queue.core.DeadLetterConfig;
import com.zula.queue.core.QueueManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RecordingQueueManager extends QueueManager {

    private final List<RecordedQueue> createdQueues = new ArrayList<>();

    public RecordingQueueManager() {
        this(new QueueProperties());
    }

    public RecordingQueueManager(QueueProperties properties) {
        super(null, properties);
    }

    @Override
    public void createServiceQueue(String serviceName, String messageType) {
        createServiceQueue(serviceName, messageType, DeadLetterConfig.disabled());
    }

    @Override
    public void createServiceQueue(String serviceName, String messageType, DeadLetterConfig dlqConfig) {
        createdQueues.add(new RecordedQueue(
                serviceName,
                messageType,
                generateQueueName(serviceName, messageType),
                generateExchangeName(messageType),
                dlqConfig != null && dlqConfig.isEnabled(),
                dlqConfig == null ? 0 : dlqConfig.getMaxRetries(),
                dlqConfig == null ? 0 : dlqConfig.getRetryDelayMs()
        ));
    }

    public List<RecordedQueue> getCreatedQueues() {
        return Collections.unmodifiableList(createdQueues);
    }

    public boolean contains(String serviceName, String messageType) {
        return createdQueues.stream()
                .anyMatch(queue -> queue.getServiceName().equals(serviceName)
                        && queue.getMessageType().equals(messageType));
    }

    public static class RecordedQueue {
        private final String serviceName;
        private final String messageType;
        private final String queueName;
        private final String exchangeName;
        private final boolean dlqEnabled;
        private final int maxRetries;
        private final long retryDelayMs;

        public RecordedQueue(String serviceName,
                             String messageType,
                             String queueName,
                             String exchangeName,
                             boolean dlqEnabled,
                             int maxRetries,
                             long retryDelayMs) {
            this.serviceName = serviceName;
            this.messageType = messageType;
            this.queueName = queueName;
            this.exchangeName = exchangeName;
            this.dlqEnabled = dlqEnabled;
            this.maxRetries = maxRetries;
            this.retryDelayMs = retryDelayMs;
        }

        public String getServiceName() { return serviceName; }
        public String getMessageType() { return messageType; }
        public String getQueueName() { return queueName; }
        public String getExchangeName() { return exchangeName; }
        public boolean isDlqEnabled() { return dlqEnabled; }
        public int getMaxRetries() { return maxRetries; }
        public long getRetryDelayMs() { return retryDelayMs; }
    }
}
