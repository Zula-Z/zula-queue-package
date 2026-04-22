package com.zula.queue.core;

public class DeadLetterConfig {

    private final boolean enabled;
    private final boolean retryable;
    private final int maxRetries;
    private final long retryDelayMs;

    public DeadLetterConfig(boolean enabled, boolean retryable, int maxRetries, long retryDelayMs) {
        this.enabled = enabled;
        this.retryable = retryable;
        this.maxRetries = Math.max(0, maxRetries);
        this.retryDelayMs = Math.max(0, retryDelayMs);
    }

    public static DeadLetterConfig disabled() {
        return new DeadLetterConfig(false, false, 0, 0);
    }

    public static DeadLetterConfig from(ZulaCommandRetry annotation) {
        if (annotation == null) {
            return disabled();
        }
        return new DeadLetterConfig(true, annotation.retryable(), annotation.maxRetries(), annotation.retryDelayMs());
    }

    public static DeadLetterConfig merge(DeadLetterConfig messageConfig, ZulaHandlerRetry override) {
        if (override == null) {
            return messageConfig;
        }
        return new DeadLetterConfig(true, override.retryable(), override.maxRetries(), override.retryDelayMs());
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isRetryable() {
        return retryable;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getRetryDelayMs() {
        return retryDelayMs;
    }
}
