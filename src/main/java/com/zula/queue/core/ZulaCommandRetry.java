package com.zula.queue.core;

import org.springframework.stereotype.Indexed;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Indexed
public @interface ZulaCommandRetry {
    boolean retryable() default true;
    int maxRetries() default 3;
    long retryDelayMs() default 0;
}
