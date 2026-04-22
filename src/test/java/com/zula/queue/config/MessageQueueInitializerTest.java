package com.zula.queue.config;

import com.zula.queue.samples.ClassNameMessage;
import com.zula.queue.testkit.RecordingQueueManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThat;

class MessageQueueInitializerTest {

    @Test
    void createsQueueForCommandWithoutCommandType() {
        RecordingQueueManager queueManager = initialize();

        assertThat(queueManager.contains("svc-demo", "sample")).isTrue();
    }

    @Test
    void usesCommandTypeWhenPresent() {
        RecordingQueueManager queueManager = initialize();

        assertThat(queueManager.contains("svc-demo", "typed-command")).isTrue();
    }

    @Test
    void handlesZulaMessageWithExplicitType() {
        RecordingQueueManager queueManager = initialize();

        assertThat(queueManager.contains("svc-demo", "custom-message")).isTrue();
    }

    @Test
    void fallsBackToClassNameWhenMessageTypeMissing() {
        RecordingQueueManager queueManager = initialize();

        assertThat(queueManager.contains("svc-demo", "classname")).isTrue();
    }

    @Test
    void recordsDeadLetterConfigFromCommandRetry() {
        RecordingQueueManager queueManager = initialize();

        assertThat(queueManager.getCreatedQueues())
                .anySatisfy(queue -> {
                    assertThat(queue.getMessageType()).isEqualTo("typed-command");
                    assertThat(queue.isDlqEnabled()).isTrue();
                    assertThat(queue.getMaxRetries()).isEqualTo(5);
                    assertThat(queue.getRetryDelayMs()).isEqualTo(2500L);
                });
    }

    private RecordingQueueManager initialize() {
        QueueProperties properties = new QueueProperties();
        properties.getScanPackages().add(ClassNameMessage.class.getPackageName());

        RecordingQueueManager queueManager = new RecordingQueueManager();
        Environment environment = new MockEnvironment().withProperty("spring.application.name", "svc-demo");
        BeanFactory beanFactory = new StaticApplicationContext();

        MessageQueueInitializer initializer =
                new MessageQueueInitializer(queueManager, environment, beanFactory, properties, null);
        initializer.initializeQueues();
        return queueManager;
    }
}
