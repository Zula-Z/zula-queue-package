package com.zula.queue.config;

import com.zula.queue.core.MessagePublisher;
import com.zula.queue.core.QueueManager;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
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

    @Bean
    @ConditionalOnMissingBean
    public com.zula.queue.core.CommandPublisher commandPublisher(MessagePublisher messagePublisher) {
        return new com.zula.queue.core.CommandPublisher(messagePublisher);
    }

    @Bean
    @ConditionalOnMissingBean
    public com.zula.queue.core.MessageHandlerRegistry messageHandlerRegistry(QueueManager queueManager,
                                                                             org.springframework.amqp.rabbit.connection.ConnectionFactory connectionFactory,
                                                                             com.fasterxml.jackson.databind.ObjectMapper objectMapper,
                                                                             org.springframework.core.env.Environment environment,
                                                                             @org.springframework.beans.factory.annotation.Autowired(required = false) com.zula.queue.core.QueuePersistenceService queuePersistenceService) {
        return new com.zula.queue.core.MessageHandlerRegistry(queueManager, connectionFactory, objectMapper, environment, queuePersistenceService);
    }

    @Bean
    @ConditionalOnMissingBean
    public MessageQueueInitializer messageQueueInitializer(QueueManager queueManager,
                                                           org.springframework.core.env.Environment environment,
                                                           org.springframework.beans.factory.BeanFactory beanFactory) {
        return new MessageQueueInitializer(queueManager, environment, beanFactory);
    }

    @Bean
    @ConditionalOnMissingBean
    public com.zula.queue.core.CommandHandlerRegistry commandHandlerRegistry(com.zula.queue.core.MessageHandlerRegistry messageHandlerRegistry) {
        return new com.zula.queue.core.CommandHandlerRegistry(messageHandlerRegistry);
    }

    @Bean
    @ConditionalOnClass({org.jdbi.v3.core.Jdbi.class, com.zula.database.core.DatabaseManager.class})
    @ConditionalOnBean({org.jdbi.v3.core.Jdbi.class, com.zula.database.core.DatabaseManager.class})
    public com.zula.queue.core.QueuePersistenceService queuePersistenceService(org.jdbi.v3.core.Jdbi jdbi,
                                                                               com.zula.database.core.DatabaseManager databaseManager,
                                                                               com.fasterxml.jackson.databind.ObjectMapper objectMapper,
                                                                               com.zula.database.config.DatabaseProperties databaseProperties) {
        return new com.zula.queue.core.QueuePersistenceService(jdbi, databaseManager, objectMapper, databaseProperties);
    }
}
