package com.zula.queue.config;

import com.zula.queue.core.QueueRegistryService;
import com.zula.queue.core.model.QueueMetadata;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/dlq/registry")
@ConditionalOnProperty(prefix = "zula.queue", name = "enable-dlq-controller", havingValue = "true", matchIfMissing = true)
public class QueueRegistryController {

    private final QueueRegistryService registryService;

    public QueueRegistryController(QueueRegistryService registryService) {
        this.registryService = registryService;
    }

    @GetMapping("/services")
    public List<Map<String, Object>> getAllServices() {
        return registryService.getAllServices();
    }

    @GetMapping("/services/{serviceName}/queues")
    public List<QueueMetadata> getServiceQueues(@PathVariable String serviceName) {
        return registryService.getRegisteredQueues(serviceName);
    }

    @GetMapping("/queues")
    public List<Map<String, Object>> getAllQueuesWithStats() {
        return registryService.getQueueStats();
    }

    @GetMapping("/queues/{queueName}/stats")
    public Map<String, Object> getQueueStats(@PathVariable String queueName) {
        return registryService.getQueueDlqStats(queueName);
    }

    @DeleteMapping("/queues/{queueName}")
    public Map<String, String> unregisterQueue(@PathVariable String queueName) {
        registryService.unregisterQueue(queueName);
        return Map.of("status", "success", "message", "Queue unregistered: " + queueName);
    }

    @DeleteMapping("/queues/{queueName}/hard")
    public Map<String, String> deleteQueue(@PathVariable String queueName) {
        registryService.deleteQueue(queueName);
        return Map.of("status", "success", "message", "Queue deleted: " + queueName);
    }
}
