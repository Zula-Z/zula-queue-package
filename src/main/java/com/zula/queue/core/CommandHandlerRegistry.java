package com.zula.queue.core;

import org.springframework.stereotype.Component;

import java.util.function.Consumer;

/**
 * Command-first registry wrapper that resolves command types using {@link ZulaCommand}
 * (with a fallback to {@link ZulaMessage}) and delegates registration to
 * {@link MessageHandlerRegistry}.
 */
@Component
public class CommandHandlerRegistry {

    private final MessageHandlerRegistry registry;

    public CommandHandlerRegistry(MessageHandlerRegistry registry) {
        this.registry = registry;
    }

    public <T> void register(Class<T> commandClass, Consumer<T> handler) {
        registry.register(deriveCommandType(commandClass), commandClass, handler);
    }

    public <T> void register(String commandType, Class<T> commandClass, Consumer<T> handler) {
        registry.register(commandType, commandClass, handler);
    }

    private String deriveCommandType(Class<?> clazz) {
        ZulaCommand commandAnnotation = clazz.getAnnotation(ZulaCommand.class);
        if (commandAnnotation != null && !commandAnnotation.commandType().isEmpty()) {
            return commandAnnotation.commandType().toLowerCase();
        }
        ZulaMessage messageAnnotation = clazz.getAnnotation(ZulaMessage.class);
        if (messageAnnotation != null && !messageAnnotation.messageType().isEmpty()) {
            return messageAnnotation.messageType().toLowerCase();
        }
        String simpleName = clazz.getSimpleName();
        if (simpleName.endsWith("Command")) {
            return simpleName.substring(0, simpleName.length() - "Command".length()).toLowerCase();
        }
        if (simpleName.endsWith("Message")) {
            return simpleName.substring(0, simpleName.length() - "Message".length()).toLowerCase();
        }
        return simpleName.toLowerCase();
    }
}
