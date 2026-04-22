package com.zula.queue.core;

import com.zula.queue.config.QueueProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Component
public class QueueManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueueManager.class);

    private final RabbitAdmin rabbitAdmin;
    private final QueueProperties properties;
    private final Set<String> createdQueues = new HashSet<>();
    private final Set<String> createdExchanges = new HashSet<>();
    private final Set<String> createdDlqQueues = new HashSet<>();
    private final Set<String> createdDlqExchanges = new HashSet<>();

    @Autowired
    public QueueManager(RabbitAdmin rabbitAdmin, QueueProperties properties) {
        this.rabbitAdmin = rabbitAdmin;
        this.properties = properties;
    }

    @PostConstruct
    public void init() {
        LOGGER.info("Zula Queue Manager initialized");
        LOGGER.info("Auto-create queues: {}", properties.isAutoCreateQueues());
    }

    public void createServiceQueue(String serviceName, String messageType) {
        createServiceQueue(serviceName, messageType, DeadLetterConfig.disabled());
    }

    public void createServiceQueue(String serviceName, String messageType, DeadLetterConfig dlqConfig) {
        if (!properties.isAutoCreateQueues()) {
            return;
        }

        String queueName = generateQueueName(serviceName, messageType);
        String exchangeName = generateExchangeName(messageType);
        createExchange(exchangeName);
        createQueue(queueName, exchangeName, messageType, dlqConfig);
    }

    private void createExchange(String exchangeName) {
        if (createdExchanges.contains(exchangeName)) {
            return;
        }
        rabbitAdmin.declareExchange(new TopicExchange(exchangeName, true, false));
        createdExchanges.add(exchangeName);
        LOGGER.info("Zula: Created exchange: {}", exchangeName);
    }

    private void createQueue(String queueName, String exchangeName, String messageType, DeadLetterConfig dlqConfig) {
        if (createdQueues.contains(queueName)) {
            return;
        }

        Queue queue;
        if (dlqConfig != null && dlqConfig.isEnabled()) {
            String dlxName = generateDeadLetterExchangeName(messageType);
            String dlqName = generateDeadLetterQueueName(queueName);
            createDeadLetterExchangeAndQueue(dlxName, dlqName);

            Map<String, Object> arguments = new HashMap<>();
            arguments.put("x-dead-letter-exchange", dlxName);
            queue = new Queue(queueName, true, false, false, arguments);
        } else {
            queue = new Queue(queueName, true, false, false);
        }

        try {
            rabbitAdmin.declareQueue(queue);
        } catch (AmqpIOException exception) {
            LOGGER.warn("Zula: Recreating queue {} due to {}", queueName, exception.getMessage());
            rabbitAdmin.deleteQueue(queueName);
            rabbitAdmin.declareQueue(queue);
        }

        Binding binding = BindingBuilder.bind(queue)
                .to(new TopicExchange(exchangeName))
                .with("#");
        rabbitAdmin.declareBinding(binding);
        createdQueues.add(queueName);
        LOGGER.info("Zula: Created queue: {}", queueName);
    }

    private void createDeadLetterExchangeAndQueue(String dlxName, String dlqName) {
        if (!createdDlqExchanges.contains(dlxName)) {
            rabbitAdmin.declareExchange(new TopicExchange(dlxName, true, false));
            createdDlqExchanges.add(dlxName);
            LOGGER.info("Zula: Created DLX: {}", dlxName);
        }
        if (!createdDlqQueues.contains(dlqName)) {
            Queue dlq = new Queue(dlqName, true, false, false);
            rabbitAdmin.declareQueue(dlq);
            rabbitAdmin.declareBinding(BindingBuilder.bind(dlq).to(new TopicExchange(dlxName)).with("#"));
            createdDlqQueues.add(dlqName);
            LOGGER.info("Zula: Created DLQ: {}", dlqName);
        }
    }

    public String generateQueueName(String serviceName, String messageType) {
        String prefix = properties.getQueuePrefix();
        return (prefix.isEmpty() ? "" : prefix + ".") + serviceName.toLowerCase() + "." + messageType.toLowerCase();
    }

    public String generateExchangeName(String messageType) {
        return messageType.toLowerCase() + properties.getExchangeSuffix();
    }

    public String generateDeadLetterExchangeName(String messageType) {
        return generateExchangeName(messageType) + ".dlx";
    }

    public String generateDeadLetterQueueName(String queueName) {
        return queueName + ".dlq";
    }
}
