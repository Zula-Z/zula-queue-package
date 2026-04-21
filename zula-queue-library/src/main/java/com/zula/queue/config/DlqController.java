package com.zula.queue.config;

import com.zula.queue.core.DlqPersistenceService;
import com.zula.queue.core.DlqRecord;
import com.zula.queue.core.DlqReplayLog;
import com.zula.queue.core.DlqService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RestController
@ConditionalOnBean(DlqPersistenceService.class)
@ConditionalOnProperty(prefix = "zula.queue", name = "enable-dlq-controller", havingValue = "true", matchIfMissing = true)
@RequestMapping("/dlq")
public class DlqController {

    private final DlqPersistenceService dlqPersistenceService;
    private final DlqService dlqService;

    public DlqController(DlqPersistenceService dlqPersistenceService, DlqService dlqService) {
        this.dlqPersistenceService = dlqPersistenceService;
        this.dlqService = dlqService;
    }

    @GetMapping("/messages")
    public List<DlqRecord> list(@RequestParam(value = "queue", required = false) String queue,
                                @RequestParam(value = "limit", defaultValue = "100") int limit) {
        if (queue != null && !queue.isBlank()) {
            return dlqPersistenceService.listByQueue(queue, limit);
        }
        return dlqPersistenceService.listAll(limit);
    }

    @GetMapping("/messages/{id}")
    public DlqRecord get(@PathVariable String id) {
        return dlqPersistenceService.find(id);
    }

    @PostMapping("/messages/{id}/replay")
    public ResponseEntity<?> replay(@PathVariable String id) {
        if (!dlqService.replay(id)) {
            return ResponseEntity.status(404).body(error("not_found", "DLQ message not found"));
        }
        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/messages/{id}")
    public ResponseEntity<?> delete(@PathVariable String id) {
        int deleted = dlqService.delete(id);
        if (deleted == 0) {
            return ResponseEntity.status(404).body(error("not_found", "DLQ message not found"));
        }
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/queues/{queue}/replay")
    public ResponseEntity<?> replayQueue(@PathVariable String queue,
                                         @RequestParam(value = "limit", defaultValue = "100") int limit) {
        return ResponseEntity.ok(Map.of("replayed", dlqService.replayByQueue(queue, limit)));
    }

    @PostMapping("/services/{service}/replay")
    public ResponseEntity<?> replayService(@PathVariable String service,
                                           @RequestParam(value = "limit", defaultValue = "100") int limit) {
        return ResponseEntity.ok(Map.of("replayed", dlqService.replayByService(service, limit)));
    }

    @DeleteMapping("/queues/{queue}")
    public ResponseEntity<?> deleteQueue(@PathVariable String queue) {
        return ResponseEntity.ok(Map.of("deleted", dlqService.deleteByQueue(queue)));
    }

    @DeleteMapping("/services/{service}")
    public ResponseEntity<?> deleteService(@PathVariable String service) {
        return ResponseEntity.ok(Map.of("deleted", dlqService.deleteByService(service)));
    }

    @GetMapping("/stats/queues")
    public List<DlqPersistenceService.Stat> countsByQueue() {
        return dlqPersistenceService == null ? Collections.emptyList() : dlqPersistenceService.countsByQueue();
    }

    @GetMapping("/stats/services")
    public List<DlqPersistenceService.Stat> countsByService() {
        return dlqPersistenceService == null ? Collections.emptyList() : dlqPersistenceService.countsByService();
    }

    @GetMapping("/search")
    public DlqPersistenceService.PagedResult search(@RequestParam(value = "queue", required = false) String queue,
                                                    @RequestParam(value = "service", required = false) String service,
                                                    @RequestParam(value = "messageType", required = false) String messageType,
                                                    @RequestParam(value = "status", required = false) String status,
                                                    @RequestParam(value = "from", required = false) LocalDateTime from,
                                                    @RequestParam(value = "to", required = false) LocalDateTime to,
                                                    @RequestParam(value = "limit", defaultValue = "100") int limit,
                                                    @RequestParam(value = "offset", defaultValue = "0") int offset) {
        DlqPersistenceService.SearchRequest request = new DlqPersistenceService.SearchRequest();
        request.queueName = queue;
        request.serviceName = service;
        request.messageType = messageType;
        request.status = status;
        request.from = from;
        request.to = to;
        request.limit = limit;
        request.offset = offset;
        return dlqPersistenceService.search(request);
    }

    @GetMapping("/replay-log")
    public List<DlqReplayLog> replayLog(@RequestParam(value = "messageId", required = false) String messageId,
                                        @RequestParam(value = "queue", required = false) String queue,
                                        @RequestParam(value = "service", required = false) String service,
                                        @RequestParam(value = "limit", defaultValue = "100") int limit,
                                        @RequestParam(value = "offset", defaultValue = "0") int offset) {
        return dlqPersistenceService.listReplayLog(messageId, queue, service, limit, offset);
    }

    private Map<String, String> error(String code, String message) {
        return Map.of("error", code, "message", message == null ? "" : message);
    }
}
