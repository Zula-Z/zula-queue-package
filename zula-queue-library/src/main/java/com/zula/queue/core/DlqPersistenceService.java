package com.zula.queue.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zula.database.config.DatabaseProperties;
import com.zula.database.core.DatabaseManager;
import org.jdbi.v3.core.Jdbi;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

public class DlqPersistenceService {

    private final Jdbi jdbi;
    private final ObjectMapper objectMapper;
    private final String queueSchema;

    public DlqPersistenceService(
            Jdbi jdbi,
            DatabaseManager databaseManager,
            ObjectMapper objectMapper,
            DatabaseProperties properties
    ) {
        this.jdbi = jdbi;
        this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
        this.queueSchema = databaseManager.generateQueueSchemaName();

        if (properties.isAutoCreateQueueSchema()) {
            databaseManager.createQueueSchemaAndTables();
        }
        createTablesIfMissing();
    }

    private void createTablesIfMissing() {
        jdbi.useHandle(handle -> {
            String dbProduct = detectDatabaseProduct(handle);
            boolean isMySQL = dbProduct.contains("mysql");
            String autoIncrement = isMySQL ? "BIGINT AUTO_INCREMENT PRIMARY KEY" : "BIGSERIAL PRIMARY KEY";

            handle.execute("CREATE TABLE IF NOT EXISTS " + queueSchema + ".message_dlq (" +
                    "message_id VARCHAR(100) PRIMARY KEY," +
                    "message_type VARCHAR(255)," +
                    "queue_name VARCHAR(255)," +
                    "service_name VARCHAR(255)," +
                    "payload TEXT," +
                    "headers_json TEXT," +
                    "routing_key VARCHAR(255)," +
                    "error TEXT," +
                    "retry_count INT," +
                    "max_retries INT," +
                    "retry_delay_ms BIGINT," +
                    "status VARCHAR(32)," +
                    "created_at TIMESTAMP," +
                    "updated_at TIMESTAMP" +
                    ")");

            handle.execute("CREATE TABLE IF NOT EXISTS " + queueSchema + ".message_dlq_replay (" +
                    "replay_id VARCHAR(100) PRIMARY KEY," +
                    "message_id VARCHAR(100)," +
                    "message_type VARCHAR(255)," +
                    "queue_name VARCHAR(255)," +
                    "service_name VARCHAR(255)," +
                    "action VARCHAR(32)," +
                    "actor VARCHAR(255)," +
                    "created_at TIMESTAMP" +
                    ")");

            handle.execute("CREATE TABLE IF NOT EXISTS " + queueSchema + ".queue_registry (" +
                    "id " + autoIncrement + "," +
                    "service_name VARCHAR(255) NOT NULL," +
                    "queue_name VARCHAR(255) NOT NULL," +
                    "message_type VARCHAR(255) NOT NULL," +
                    "exchange_name VARCHAR(255)," +
                    "has_dlq BOOLEAN NOT NULL DEFAULT FALSE," +
                    "max_retries INT NOT NULL DEFAULT 0," +
                    "retry_delay_ms BIGINT NOT NULL DEFAULT 0," +
                    "registered_at TIMESTAMP NOT NULL," +
                    "last_seen_at TIMESTAMP NOT NULL," +
                    "is_active BOOLEAN NOT NULL DEFAULT TRUE," +
                    "UNIQUE(service_name, queue_name)" +
                    ")");
        });
    }

    private String detectDatabaseProduct(org.jdbi.v3.core.Handle handle) {
        try {
            return handle.getConnection().getMetaData().getDatabaseProductName().toLowerCase();
        } catch (Exception e) {
            return "postgresql";
        }
    }

    public void record(
            String messageId,
            String messageType,
            String queueName,
            String serviceName,
            Object payload,
            String headersJson,
            String routingKey,
            long retryDelayMs,
            String error,
            int retryCount,
            int maxRetries,
            String status
    ) {
        LocalDateTime now = LocalDateTime.now();
        String serializedPayload = toPayload(payload);
        jdbi.useHandle(handle -> {
            int updated = handle.createUpdate(
                            "UPDATE " + queueSchema + ".message_dlq SET " +
                                    "message_type=:messageType, queue_name=:queueName, service_name=:serviceName, payload=:payload, " +
                                    "headers_json=:headersJson, routing_key=:routingKey, error=:error, retry_count=:retryCount, " +
                                    "max_retries=:maxRetries, retry_delay_ms=:retryDelayMs, status=:status, updated_at=:updatedAt " +
                                    "WHERE message_id=:messageId")
                    .bind("messageType", messageType)
                    .bind("queueName", queueName)
                    .bind("serviceName", serviceName)
                    .bind("payload", serializedPayload)
                    .bind("headersJson", headersJson)
                    .bind("routingKey", routingKey)
                    .bind("error", error)
                    .bind("retryCount", retryCount)
                    .bind("maxRetries", maxRetries)
                    .bind("retryDelayMs", retryDelayMs)
                    .bind("status", status)
                    .bind("updatedAt", now)
                    .bind("messageId", messageId)
                    .execute();

            if (updated == 0) {
                handle.createUpdate(
                                "INSERT INTO " + queueSchema + ".message_dlq " +
                                        "(message_id, message_type, queue_name, service_name, payload, headers_json, routing_key, " +
                                        "error, retry_count, max_retries, retry_delay_ms, status, created_at, updated_at) " +
                                        "VALUES (:messageId, :messageType, :queueName, :serviceName, :payload, :headersJson, :routingKey, " +
                                        ":error, :retryCount, :maxRetries, :retryDelayMs, :status, :createdAt, :updatedAt)")
                        .bind("messageId", messageId)
                        .bind("messageType", messageType)
                        .bind("queueName", queueName)
                        .bind("serviceName", serviceName)
                        .bind("payload", serializedPayload)
                        .bind("headersJson", headersJson)
                        .bind("routingKey", routingKey)
                        .bind("error", error)
                        .bind("retryCount", retryCount)
                        .bind("maxRetries", maxRetries)
                        .bind("retryDelayMs", retryDelayMs)
                        .bind("status", status)
                        .bind("createdAt", now)
                        .bind("updatedAt", now)
                        .execute();
            }
        });
    }

    public void logReplay(String messageId, String messageType, String queueName, String serviceName, String action, String actor) {
        jdbi.useHandle(handle -> handle.createUpdate(
                        "INSERT INTO " + queueSchema + ".message_dlq_replay " +
                                "(replay_id, message_id, message_type, queue_name, service_name, action, actor, created_at) " +
                                "VALUES (:replayId, :messageId, :messageType, :queueName, :serviceName, :action, :actor, :createdAt)")
                .bind("replayId", java.util.UUID.randomUUID().toString())
                .bind("messageId", messageId)
                .bind("messageType", messageType)
                .bind("queueName", queueName)
                .bind("serviceName", serviceName)
                .bind("action", action)
                .bind("actor", actor)
                .bind("createdAt", LocalDateTime.now())
                .execute());
    }

    public List<DlqRecord> listByQueue(String queueName, int limit) {
        return jdbi.withHandle(handle -> handle.createQuery(
                        "SELECT * FROM " + queueSchema + ".message_dlq WHERE queue_name=:queueName ORDER BY created_at DESC LIMIT :limit")
                .bind("queueName", queueName)
                .bind("limit", limit)
                .mapToBean(DlqRecord.class)
                .list());
    }

    public List<DlqRecord> listAll(int limit) {
        return jdbi.withHandle(handle -> handle.createQuery(
                        "SELECT * FROM " + queueSchema + ".message_dlq ORDER BY created_at DESC LIMIT :limit")
                .bind("limit", limit)
                .mapToBean(DlqRecord.class)
                .list());
    }

    public DlqRecord find(String messageId) {
        return jdbi.withHandle(handle -> handle.createQuery(
                        "SELECT * FROM " + queueSchema + ".message_dlq WHERE message_id=:messageId")
                .bind("messageId", messageId)
                .mapToBean(DlqRecord.class)
                .findOne()
                .orElse(null));
    }

    public int delete(String messageId) {
        return jdbi.withHandle(handle -> handle.createUpdate(
                        "DELETE FROM " + queueSchema + ".message_dlq WHERE message_id=:messageId")
                .bind("messageId", messageId)
                .execute());
    }

    public int deleteByQueue(String queueName) {
        return jdbi.withHandle(handle -> handle.createUpdate(
                        "DELETE FROM " + queueSchema + ".message_dlq WHERE queue_name=:queueName")
                .bind("queueName", queueName)
                .execute());
    }

    public int deleteByService(String serviceName) {
        return jdbi.withHandle(handle -> handle.createUpdate(
                        "DELETE FROM " + queueSchema + ".message_dlq WHERE service_name=:serviceName")
                .bind("serviceName", serviceName)
                .execute());
    }

    public List<DlqRecord> listByQueueForReplay(String queueName, int limit) {
        return jdbi.withHandle(handle -> handle.createQuery(
                        "SELECT * FROM " + queueSchema + ".message_dlq WHERE queue_name=:queueName ORDER BY created_at ASC LIMIT :limit")
                .bind("queueName", queueName)
                .bind("limit", limit)
                .mapToBean(DlqRecord.class)
                .list());
    }

    public List<DlqRecord> listByServiceForReplay(String serviceName, int limit) {
        return jdbi.withHandle(handle -> handle.createQuery(
                        "SELECT * FROM " + queueSchema + ".message_dlq WHERE service_name=:serviceName ORDER BY created_at ASC LIMIT :limit")
                .bind("serviceName", serviceName)
                .bind("limit", limit)
                .mapToBean(DlqRecord.class)
                .list());
    }

    public List<Stat> countsByQueue() {
        return jdbi.withHandle(handle -> handle.createQuery(
                        "SELECT queue_name AS key, COUNT(*) AS count FROM " + queueSchema + ".message_dlq GROUP BY queue_name ORDER BY queue_name")
                .mapToBean(Stat.class)
                .list());
    }

    public List<Stat> countsByService() {
        return jdbi.withHandle(handle -> handle.createQuery(
                        "SELECT service_name AS key, COUNT(*) AS count FROM " + queueSchema + ".message_dlq GROUP BY service_name ORDER BY service_name")
                .mapToBean(Stat.class)
                .list());
    }

    public PagedResult search(SearchRequest request) {
        StringBuilder where = new StringBuilder(" WHERE 1=1");
        if (hasText(request.queueName)) {
            where.append(" AND LOWER(queue_name) LIKE LOWER(:queueName)");
        }
        if (hasText(request.serviceName)) {
            where.append(" AND LOWER(service_name) LIKE LOWER(:serviceName)");
        }
        if (hasText(request.messageType)) {
            where.append(" AND LOWER(message_type) LIKE LOWER(:messageType)");
        }
        if (hasText(request.status)) {
            where.append(" AND LOWER(status) LIKE LOWER(:status)");
        }
        if (request.from != null) {
            where.append(" AND created_at >= :fromTs");
        }
        if (request.to != null) {
            where.append(" AND created_at <= :toTs");
        }

        String base = " FROM " + queueSchema + ".message_dlq" + where;
        List<DlqRecord> rows = jdbi.withHandle(handle -> {
            var query = handle.createQuery("SELECT *" + base + " ORDER BY created_at DESC LIMIT :limit OFFSET :offset");
            bindSearch(query, request);
            return query.mapToBean(DlqRecord.class).list();
        });
        long total = jdbi.withHandle(handle -> {
            var query = handle.createQuery("SELECT COUNT(*)" + base);
            bindSearch(query, request);
            return query.mapTo(Long.class).one();
        });

        return new PagedResult(total, rows);
    }

    public List<DlqReplayLog> listReplayLog(String messageId, String queueName, String serviceName, int limit, int offset) {
        return jdbi.withHandle(handle -> handle.createQuery(
                        "SELECT * FROM " + queueSchema + ".message_dlq_replay " +
                                "WHERE (:messageId IS NULL OR message_id = :messageId) " +
                                "AND (:queueName IS NULL OR queue_name = :queueName) " +
                                "AND (:serviceName IS NULL OR service_name = :serviceName) " +
                                "ORDER BY created_at DESC LIMIT :limit OFFSET :offset")
                .bind("messageId", messageId)
                .bind("queueName", queueName)
                .bind("serviceName", serviceName)
                .bind("limit", limit)
                .bind("offset", offset)
                .mapToBean(DlqReplayLog.class)
                .list());
    }

    private void bindSearch(org.jdbi.v3.core.statement.Query query, SearchRequest request) {
        if (hasText(request.queueName)) {
            query.bind("queueName", "%" + request.queueName + "%");
        }
        if (hasText(request.serviceName)) {
            query.bind("serviceName", "%" + request.serviceName + "%");
        }
        if (hasText(request.messageType)) {
            query.bind("messageType", "%" + request.messageType + "%");
        }
        if (hasText(request.status)) {
            query.bind("status", "%" + request.status + "%");
        }
        if (request.from != null) {
            query.bind("fromTs", request.from);
        }
        if (request.to != null) {
            query.bind("toTs", request.to);
        }
        query.bind("limit", request.limit);
        query.bind("offset", request.offset);
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private String toPayload(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception exception) {
            return payload == null ? "" : payload.toString();
        }
    }

    public static class Stat {
        private String key;
        private long count;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }

    public static class SearchRequest {
        public String queueName;
        public String serviceName;
        public String messageType;
        public String status;
        public LocalDateTime from;
        public LocalDateTime to;
        public int limit = 100;
        public int offset = 0;
    }

    public static class PagedResult {
        private final long total;
        private final List<DlqRecord> items;

        public PagedResult(long total, List<DlqRecord> items) {
            this.total = total;
            this.items = items == null ? Collections.emptyList() : items;
        }

        public long getTotal() {
            return total;
        }

        public List<DlqRecord> getItems() {
            return items;
        }
    }
}
