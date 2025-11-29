package com.zula.queue.core;

import com.zula.queue.config.QueueProperties;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashSet;
import java.util.Set;

@Component
public class QueueManager {

    private final RabbitAdmin rabbitAdmin;
    private final QueueProperties properties;
    private final Set<String> createdQueues = new HashSet<>();
    private final Set<String> createdExchanges = new HashSet<>();

    @Autowired
    public QueueManager(RabbitAdmin rabbitAdmin, QueueProperties properties) {
        this.rabbitAdmin = rabbitAdmin;
        this.properties = properties;
    }

    @PostConstruct
    public void init() {
        System.out.println("🚀 Zula Queue Manager initialized");
        System.out.println("✅ Auto-create queues: " + properties.isAutoCreateQueues());
    }

    public void createServiceQueue(String serviceName, String messageType) {
        if (!properties.isAutoCreateQueues()) {
            return;
        }

        String queueName = generateQueueName(serviceName, messageType);
        String exchangeName = generateExchangeName(messageType);

        createExchange(exchangeName);
        createQueue(queueName, exchangeName);
    }

    private void createExchange(String exchangeName) {
        if (!createdExchanges.contains(exchangeName)) {
            TopicExchange exchange = new TopicExchange(exchangeName, true, false);
            rabbitAdmin.declareExchange(exchange);
            createdExchanges.add(exchangeName);
            System.out.println("📡 Zula: Created exchange: " + exchangeName);
        }
    }

    private void createQueue(String queueName, String exchangeName) {
        if (!createdQueues.contains(queueName)) {
            Queue queue = new Queue(queueName, true, false, false);
            rabbitAdmin.declareQueue(queue);

            Binding binding = BindingBuilder.bind(queue)
                    .to(new TopicExchange(exchangeName))
                    .with("#");
            rabbitAdmin.declareBinding(binding);

            createdQueues.add(queueName);
            System.out.println("📨 Zula: Created queue: " + queueName);
        }
    }

    public String generateQueueName(String serviceName, String messageType) {
        String prefix = properties.getQueuePrefix();
        return (prefix.isEmpty() ? "" : prefix + ".") +
                serviceName.toLowerCase() + "." +
                messageType.toLowerCase();
    }

    public String generateExchangeName(String messageType) {
        return messageType.toLowerCase() + properties.getExchangeSuffix();
    }
}