package com.zula.queue.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as a queue message and allows overriding the message type.
 * If messageType is empty, the class name minus the "Message" suffix is used.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ZulaMessage {
    String messageType() default "";
}
