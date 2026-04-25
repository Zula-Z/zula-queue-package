package com.zula.queue.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.Message;

import java.util.Map;
import java.util.UUID;

/**
 * Utilities for extracting message metadata from payloads or AMQP headers.
 */
public final class MessageMetadataHelper {

    public static final String HEADER_MESSAGE_ID = "x-message-id";
    public static final String HEADER_SOURCE_SERVICE = "x-source-service";
    public static final String HEADER_MESSAGE_TYPE = "x-message-type";
    public static final String HEADER_INITIATOR_TYPE = "x-initiator-type";
    public static final String HEADER_INITIATOR_ID = "x-initiator-id";
    public static final String HEADER_INITIATOR_NAME = "x-initiator-name";
    public static final String HEADER_INITIATOR_PAYLOAD = "x-initiator-payload";

    private MessageMetadataHelper() {
    }

    public static String extractSourceService(Message message) {
        if (message == null) {
            return null;
        }
        Object header = message.getMessageProperties().getHeaders().get(HEADER_SOURCE_SERVICE);
        return header != null ? header.toString() : null;
    }

    public static String extractMessageId(Message message, Object payload) {
        String fromHeader = extractMessageIdFromHeader(message);
        if (fromHeader != null && !fromHeader.isBlank()) {
            return fromHeader;
        }
        String fromPayload = extractMessageIdFromPayload(payload);
        if (fromPayload != null && !fromPayload.isBlank()) {
            return fromPayload;
        }
        return UUID.randomUUID().toString();
    }

    private static String extractMessageIdFromHeader(Message message) {
        if (message == null) {
            return null;
        }
        Map<String, Object> headers = message.getMessageProperties().getHeaders();
        Object headerValue = headers.get(HEADER_MESSAGE_ID);
        if (headerValue != null) {
            return headerValue.toString();
        }
        return null;
    }

    private static String extractMessageIdFromPayload(Object payload) {
        if (payload == null) {
            return null;
        }
        try {
            java.lang.reflect.Method getter = payload.getClass().getMethod("getRequestId");
            Object value = getter.invoke(payload);
            if (value != null) {
                return value.toString();
            }
        } catch (Exception ignored) { }
        try {
            java.lang.reflect.Field field = payload.getClass().getDeclaredField("requestId");
            field.setAccessible(true);
            Object value = field.get(payload);
            if (value != null) {
                return value.toString();
            }
        } catch (Exception ignored) { }
        return null;
    }

    public static MessageInitiator extractInitiator(Message message, ObjectMapper objectMapper) {
        if (message == null) {
            return null;
        }

        Map<String, Object> headers = message.getMessageProperties().getHeaders();
        Object payloadHeader = headers.get(HEADER_INITIATOR_PAYLOAD);
        if (payloadHeader != null && objectMapper != null) {
            try {
                return objectMapper.readValue(payloadHeader.toString(), MessageInitiator.class);
            } catch (Exception ignored) {
            }
        }

        MessageInitiator initiator = new MessageInitiator();
        boolean hasValue = false;

        Object typeHeader = headers.get(HEADER_INITIATOR_TYPE);
        if (typeHeader != null) {
            initiator.setType(typeHeader.toString());
            hasValue = true;
        }

        Object idHeader = headers.get(HEADER_INITIATOR_ID);
        if (idHeader != null) {
            initiator.setId(idHeader.toString());
            hasValue = true;
        }

        Object nameHeader = headers.get(HEADER_INITIATOR_NAME);
        if (nameHeader != null) {
            initiator.setName(nameHeader.toString());
            hasValue = true;
        }

        return hasValue ? initiator : null;
    }
}
