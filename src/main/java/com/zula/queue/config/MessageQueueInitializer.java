package com.zula.queue.config;

import com.zula.queue.core.QueueManager;
import com.zula.queue.core.QueueRegistryService;
import com.zula.queue.core.ZulaCommandRetry;
import com.zula.queue.core.ZulaHandlerRetry;
import com.zula.queue.core.ZulaCommand;
import com.zula.queue.core.ZulaMessage;
import com.zula.queue.core.ZulaPublish;
import com.zula.queue.core.DeadLetterConfig;
import com.zula.queue.core.model.QueueMetadata;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.env.Environment;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;

/**
 * Scans application base packages for classes annotated with @ZulaMessage and
 * pre-creates queues for each message type so applications don't need to wire
 * consumers just to materialize queues.
 */
@Component
public class MessageQueueInitializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageQueueInitializer.class);

    private final QueueManager queueManager;
    private final Environment environment;
    private final BeanFactory beanFactory;
    private final QueueProperties queueProperties;
    private final QueueRegistryService registryService;

    public MessageQueueInitializer(QueueManager queueManager,
                                   Environment environment,
                                   BeanFactory beanFactory,
                                   QueueProperties queueProperties,
                                   QueueRegistryService registryService) {
        this.queueManager = queueManager;
        this.environment = environment;
        this.beanFactory = beanFactory;
        this.queueProperties = queueProperties;
        this.registryService = registryService;
    }

    @PostConstruct
    public void initializeQueues() {
        java.util.Set<String> basePackages = new java.util.LinkedHashSet<>();
        if (AutoConfigurationPackages.has(beanFactory)) {
            basePackages.addAll(AutoConfigurationPackages.get(beanFactory));
        }
        basePackages.addAll(queueProperties.getScanPackages());

        java.util.Set<String> expandedPackages = new java.util.LinkedHashSet<>();
        for (String packageName : basePackages) {
            String current = packageName;
            while (current != null && !current.isBlank()) {
                expandedPackages.add(current);
                int index = current.lastIndexOf('.');
                if (index < 0) {
                    break;
                }
                current = current.substring(0, index);
            }
        }
        basePackages = expandedPackages;

        if (basePackages.isEmpty()) {
            basePackages.add("com");
            LOGGER.warn("Zula: No queue scan packages configured; falling back to scanning 'com'");
        }

        boolean scansCom = basePackages.stream().anyMatch(packageName -> packageName.equals("com") || packageName.startsWith("com."));
        if (!scansCom) {
            basePackages.add("com");
            LOGGER.warn("Zula: Added fallback package 'com' to scan for @ZulaCommand/@ZulaMessage");
        }

        String currentServiceName = environment.getProperty("spring.application.name", "unknown-service");

        ClassPathScanningCandidateComponentProvider scanner =
                new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(ZulaMessage.class));
        scanner.addIncludeFilter(new AnnotationTypeFilter(ZulaCommand.class));

        basePackages.forEach(basePackage -> scanner.findCandidateComponents(basePackage).forEach(beanDef -> {
                    String className = beanDef.getBeanClassName();
                    if (className == null) {
                        return;
                    }
                    try {
                        Class<?> clazz = ClassUtils.forName(className, null);
                        if (clazz.isInterface() || java.lang.reflect.Modifier.isAbstract(clazz.getModifiers())) {
                            return;
                        }
                        ZulaMessage messageAnnotation = clazz.getAnnotation(ZulaMessage.class);
                        ZulaCommand commandAnnotation = clazz.getAnnotation(ZulaCommand.class);
                        ZulaPublish publishAnnotation = clazz.getAnnotation(ZulaPublish.class);
                        ZulaCommandRetry retryAnnotation = clazz.getAnnotation(ZulaCommandRetry.class);
                        String messageType = deriveMessageType(clazz.getSimpleName(), messageAnnotation, commandAnnotation);
                        String targetService = deriveServiceName(publishAnnotation);
                        DeadLetterConfig deadLetterConfig = DeadLetterConfig.from(retryAnnotation);
                        queueManager.createServiceQueue(targetService, messageType, deadLetterConfig);
                        registerQueue(currentServiceName, targetService, messageType, deadLetterConfig);
                    } catch (Exception ex) {
                        LOGGER.warn("Zula: Skipping message class {} due to error: {}", className, ex.getMessage());
                    }
                }));
    }

    private String deriveServiceName(ZulaPublish publishAnnotation) {
        String serviceName = environment.getProperty("spring.application.name", "");
        if (serviceName != null && !serviceName.isBlank()) {
            return serviceName.toLowerCase();
        }
        if (publishAnnotation != null && publishAnnotation.service() != null && !publishAnnotation.service().isBlank()) {
            return publishAnnotation.service().toLowerCase();
        }
        return "unknown-service";
    }

    private String deriveMessageType(String className, ZulaMessage messageAnnotation, ZulaCommand commandAnnotation) {
        if (commandAnnotation != null && !commandAnnotation.commandType().isEmpty()) {
            return commandAnnotation.commandType().toLowerCase();
        }
        if (messageAnnotation != null && !messageAnnotation.messageType().isEmpty()) {
            return messageAnnotation.messageType().toLowerCase();
        }
        if (className.endsWith("Command")) {
            return className.substring(0, className.length() - "Command".length()).toLowerCase();
        }
        if (className.endsWith("Message")) {
            return className.substring(0, className.length() - "Message".length()).toLowerCase();
        }
        return className.toLowerCase();
    }

    private void registerQueue(String registeringServiceName, String queueServiceName, String messageType, DeadLetterConfig deadLetterConfig) {
        if (registryService == null) {
            return;
        }
        QueueMetadata metadata = new QueueMetadata();
        metadata.setServiceName(registeringServiceName);
        metadata.setQueueName(queueManager.generateQueueName(queueServiceName, messageType));
        metadata.setMessageType(messageType);
        metadata.setExchangeName(queueManager.generateExchangeName(messageType));
        metadata.setHasDlq(deadLetterConfig.isEnabled());
        metadata.setMaxRetries(deadLetterConfig.getMaxRetries());
        metadata.setRetryDelayMs(deadLetterConfig.getRetryDelayMs());
        registryService.registerQueue(metadata);
    }
}
