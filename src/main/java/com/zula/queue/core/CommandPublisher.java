package com.zula.queue.core;

import org.springframework.stereotype.Component;

/**
 * Command-first facade that mirrors the Standard Bank style "sendCommand" API
 * while delegating to {@link MessagePublisher} under the hood.
 */
@Component
public class CommandPublisher {

    private final MessagePublisher messagePublisher;

    public CommandPublisher(MessagePublisher messagePublisher) {
        this.messagePublisher = messagePublisher;
    }

    public <T> void sendCommand(T command) {
        messagePublisher.publish(command);
    }

    public <T> void sendCommandToService(String serviceName, T command) {
        messagePublisher.publishToService(serviceName, command);
    }

    public <T> void sendCommandToService(String serviceName, String action, T command) {
        messagePublisher.publishToService(serviceName, action, command);
    }

    public <T> void sendCommandToService(String serviceName, String commandType, String action, T command) {
        messagePublisher.publishToService(serviceName, commandType, action, command);
    }
}
