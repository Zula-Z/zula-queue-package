package com.zula.queue.core;

/**
 * Alias for BaseMessageConsumer to better match "handler" terminology.
 * Extend this class in services to handle incoming messages.
 */
public abstract class BaseMessageHandler<T> extends BaseMessageConsumer<T> {

    protected BaseMessageHandler() {
        super();
    }

    protected BaseMessageHandler(String customMessageType) {
        super(customMessageType);
    }
}
