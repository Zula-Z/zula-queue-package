package com.zula.queue.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zula.database.config.DatabaseProperties;
import com.zula.database.core.DatabaseManager;
import com.zula.database.dao.MessageDao;
import com.zula.database.entity.MessageInbox;
import com.zula.database.entity.MessageOutbox;
import org.jdbi.v3.core.Jdbi;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;

/**
 * Persists inbound/outbound queue traffic into the service-specific queue schema.
 */
public class QueuePersistenceService {

    private static final String STATUS_SENT = "SENT";
    private static final String STATUS_RECEIVED = "RECEIVED";
    private static final String STATUS_PROCESSED = "PROCESSED";

    private final Jdbi jdbi;
    private final DatabaseManager databaseManager;
    private final ObjectMapper objectMapper;
    private final String queueSchema;

    public QueuePersistenceService(Jdbi jdbi,
                                   DatabaseManager databaseManager,
                                   ObjectMapper objectMapper,
                                   DatabaseProperties properties) {
        this.jdbi = jdbi;
        this.databaseManager = databaseManager;
        this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
        this.queueSchema = databaseManager.generateQueueSchemaName();

        if (properties.isAutoCreateQueueSchema()) {
            databaseManager.createQueueSchemaAndTables();
        }
    }

    public String persistOutbox(Object message,
                                String messageType,
                                String targetService,
                                String messageId,
                                MessageInitiator initiator) {
        String payload = toPayload(message);
        LocalDateTime now = LocalDateTime.now();

        MessageOutbox outbox = new MessageOutbox();
        outbox.setMessageId(messageId);
        outbox.setMessageType(messageType);
        outbox.setTargetService(targetService);
        outbox.setPayload(payload);
        outbox.setStatus(STATUS_SENT);
        applyInitiator(outbox, initiator);
        outbox.setSentAt(now);
        outbox.setCreatedAt(now);
        outbox.setUpdatedAt(now);
        outbox.setRetryCount(0);

        jdbi.useExtension(MessageDao.class, dao -> dao.insertOutbox(outbox, queueSchema));
        return messageId;
    }

    public void recordInboxReceived(String messageId,
                                    String messageType,
                                    String sourceService,
                                    String payload,
                                    MessageInitiator initiator) {
        LocalDateTime now = LocalDateTime.now();

        MessageInbox inbox = new MessageInbox();
        inbox.setMessageId(messageId);
        inbox.setMessageType(messageType);
        inbox.setSourceService(StringUtils.hasText(sourceService) ? sourceService : "unknown-service");
        inbox.setPayload(payload);
        inbox.setStatus(STATUS_RECEIVED);
        applyInitiator(inbox, initiator);
        inbox.setCreatedAt(now);
        inbox.setUpdatedAt(now);

        jdbi.useExtension(MessageDao.class, dao -> dao.insertInbox(inbox, queueSchema));
    }

    public void markInboxProcessed(String messageId) {
        LocalDateTime now = LocalDateTime.now();
        jdbi.useExtension(MessageDao.class,
                dao -> dao.updateInboxStatus(messageId, STATUS_PROCESSED, now, now, queueSchema));
    }

    private String toPayload(Object message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (Exception ignored) {
            return message != null ? message.toString() : "";
        }
    }

    private void applyInitiator(MessageOutbox outbox, MessageInitiator initiator) {
        if (initiator == null) {
            return;
        }
        outbox.setInitiatorType(initiator.getType());
        outbox.setInitiatorId(initiator.getId());
        outbox.setInitiatorName(initiator.getName());
        outbox.setInitiatorPayload(toPayload(initiator));
    }

    private void applyInitiator(MessageInbox inbox, MessageInitiator initiator) {
        if (initiator == null) {
            return;
        }
        inbox.setInitiatorType(initiator.getType());
        inbox.setInitiatorId(initiator.getId());
        inbox.setInitiatorName(initiator.getName());
        inbox.setInitiatorPayload(toPayload(initiator));
    }
}
