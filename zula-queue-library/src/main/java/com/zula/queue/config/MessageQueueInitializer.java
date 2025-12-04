package com.zula.queue.config;

import com.zula.queue.core.QueueManager;
import com.zula.queue.core.ZulaCommand;
import com.zula.queue.core.ZulaMessage;
import jakarta.annotation.PostConstruct;
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

    private final QueueManager queueManager;
    private final Environment environment;
    private final BeanFactory beanFactory;

    public MessageQueueInitializer(QueueManager queueManager,
                                   Environment environment,
                                   BeanFactory beanFactory) {
        this.queueManager = queueManager;
        this.environment = environment;
        this.beanFactory = beanFactory;
    }

    @PostConstruct
    public void initializeQueues() {
        java.util.List<String> basePackages = new java.util.ArrayList<>();
        if (AutoConfigurationPackages.has(beanFactory)) {
            basePackages.addAll(AutoConfigurationPackages.get(beanFactory));
        }
        if (basePackages.isEmpty()) {
            return; // nothing to scan
        }

        String serviceName = environment.getProperty("spring.application.name", "unknown-service");

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
                        String messageType = deriveMessageType(clazz.getSimpleName(), messageAnnotation, commandAnnotation);
                        queueManager.createServiceQueue(serviceName, messageType);
                    } catch (Exception ex) {
                        System.out.println("Zula: Skipping message class " + className + " due to error: " + ex.getMessage());
                    }
                }));
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
}
