package com.zula.queue.config;

import com.zula.queue.core.QueueManager;
import com.zula.queue.core.MessagePublisher;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableRabbit
@ConditionalOnClass(ConnectionFactory.class)
@EnableConfigurationProperties(QueueProperties.class)
public class QueueAutoConfig {

    @Bean
    @ConditionalOnMissingBean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        return new RabbitAdmin(connectionFactory);
    }

    @Bean
    @ConditionalOnMissingBean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    @ConditionalOnMissingBean
    public QueueManager queueManager(RabbitAdmin rabbitAdmin, QueueProperties properties) {
        return new QueueManager(rabbitAdmin, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public MessagePublisher messagePublisher(QueueManager queueManager, RabbitTemplate rabbitTemplate) {
        return new MessagePublisher(queueManager, rabbitTemplate);
    }
}