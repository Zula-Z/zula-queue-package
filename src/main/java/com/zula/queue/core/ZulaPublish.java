package com.zula.queue.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares default routing for a message class so callers can just
 * create the message and call MessagePublisher.publish(message).
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ZulaPublish {
    /** Target service/application name. */
    String service();

    /** Optional action/operation; defaults to "process". */
    String action() default "process";
}
