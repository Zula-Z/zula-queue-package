package com.zula.queue.core;

/**
 * Command-oriented alias for {@link BaseMessageConsumer}. Extend this in services
 * to handle incoming commands without wiring listeners manually.
 */
public abstract class BaseCommandHandler<T> extends BaseMessageConsumer<T> {

    protected BaseCommandHandler() {
        super();
    }

    protected BaseCommandHandler(String customCommandType) {
        super(customCommandType);
    }
}
