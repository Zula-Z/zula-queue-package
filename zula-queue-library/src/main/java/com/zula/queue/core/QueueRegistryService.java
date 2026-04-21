package com.zula.queue.core;

import com.zula.queue.core.model.QueueMetadata;
import org.jdbi.v3.core.Jdbi;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class QueueRegistryService {

    private final Jdbi jdbi;
    private final String queueSchema;

    public QueueRegistryService(Jdbi jdbi, com.zula.database.core.DatabaseManager databaseManager) {
        this.jdbi = jdbi;
        this.queueSchema = databaseManager.generateQueueSchemaName();
    }

    public void registerQueue(QueueMetadata metadata) {
        String table = queueSchema + ".queue_registry";
        jdbi.useHandle(handle -> {
            int updated = handle.createUpdate(
                            "UPDATE " + table + " SET last_seen_at=:lastSeenAt, is_active=true, exchange_name=:exchangeName, " +
                                    "has_dlq=:hasDlq, max_retries=:maxRetries, retry_delay_ms=:retryDelayMs " +
                                    "WHERE service_name=:serviceName AND queue_name=:queueName")
                    .bind("lastSeenAt", LocalDateTime.now())
                    .bind("exchangeName", metadata.getExchangeName())
                    .bind("hasDlq", metadata.isHasDlq())
                    .bind("maxRetries", metadata.getMaxRetries())
                    .bind("retryDelayMs", metadata.getRetryDelayMs())
                    .bind("serviceName", metadata.getServiceName())
                    .bind("queueName", metadata.getQueueName())
                    .execute();

            if (updated == 0) {
                handle.createUpdate(
                                "INSERT INTO " + table + " " +
                                        "(service_name, queue_name, message_type, exchange_name, has_dlq, max_retries, retry_delay_ms, " +
                                        "registered_at, last_seen_at, is_active) " +
                                        "VALUES (:serviceName, :queueName, :messageType, :exchangeName, :hasDlq, :maxRetries, :retryDelayMs, " +
                                        ":registeredAt, :lastSeenAt, true)")
                        .bind("serviceName", metadata.getServiceName())
                        .bind("queueName", metadata.getQueueName())
                        .bind("messageType", metadata.getMessageType())
                        .bind("exchangeName", metadata.getExchangeName())
                        .bind("hasDlq", metadata.isHasDlq())
                        .bind("maxRetries", metadata.getMaxRetries())
                        .bind("retryDelayMs", metadata.getRetryDelayMs())
                        .bind("registeredAt", LocalDateTime.now())
                        .bind("lastSeenAt", LocalDateTime.now())
                        .execute();
            }
        });
    }

    public List<QueueMetadata> getRegisteredQueues(String serviceName) {
        return jdbi.withHandle(handle -> handle.createQuery(
                        "SELECT id, service_name, queue_name, message_type, exchange_name, has_dlq, max_retries, retry_delay_ms, " +
                                "registered_at, last_seen_at, is_active FROM " + queueSchema + ".queue_registry " +
                                "WHERE service_name=:serviceName ORDER BY message_type")
                .bind("serviceName", serviceName)
                .mapToBean(QueueMetadata.class)
                .list());
    }

    public List<Map<String, Object>> getAllServices() {
        return jdbi.withHandle(handle -> handle.createQuery(
                        "SELECT service_name, COUNT(*) AS queue_count, SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_count " +
                                "FROM " + queueSchema + ".queue_registry GROUP BY service_name ORDER BY service_name")
                .mapToMap()
                .list());
    }

    public List<Map<String, Object>> getQueueStats() {
        return jdbi.withHandle(handle -> handle.createQuery(
                        "SELECT qr.*, COALESCE(COUNT(dlq.message_id), 0) AS dlq_message_count " +
                                "FROM " + queueSchema + ".queue_registry qr " +
                                "LEFT JOIN " + queueSchema + ".message_dlq dlq ON qr.queue_name = dlq.queue_name " +
                                "GROUP BY qr.id, qr.service_name, qr.queue_name, qr.message_type, qr.exchange_name, qr.has_dlq, " +
                                "qr.max_retries, qr.retry_delay_ms, qr.registered_at, qr.last_seen_at, qr.is_active " +
                                "ORDER BY qr.service_name, qr.message_type")
                .mapToMap()
                .list());
    }

    public Map<String, Object> getQueueDlqStats(String queueName) {
        return jdbi.withHandle(handle -> handle.createQuery(
                        "SELECT queue_name, COUNT(*) AS total_messages, " +
                                "SUM(CASE WHEN status = 'DLQ' THEN 1 ELSE 0 END) AS failed_count " +
                                "FROM " + queueSchema + ".message_dlq WHERE queue_name=:queueName GROUP BY queue_name")
                .bind("queueName", queueName)
                .mapToMap()
                .findOne()
                .orElse(Map.of("queue_name", queueName, "total_messages", 0L)));
    }

    public void unregisterQueue(String queueName) {
        jdbi.useHandle(handle -> handle.createUpdate(
                        "UPDATE " + queueSchema + ".queue_registry SET is_active=false WHERE queue_name=:queueName")
                .bind("queueName", queueName)
                .execute());
    }

    public void deleteQueue(String queueName) {
        jdbi.useHandle(handle -> handle.createUpdate(
                        "DELETE FROM " + queueSchema + ".queue_registry WHERE queue_name=:queueName")
                .bind("queueName", queueName)
                .execute());
    }
}
