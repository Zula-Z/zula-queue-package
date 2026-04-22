package com.zula.queue.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "zula.queue")
public class QueueProperties {
    private boolean autoCreateQueues = true;
    private String queuePrefix = "zula";
    private String exchangeSuffix = "-exchange";
    private boolean durableQueues = true;
    private boolean exclusiveQueues = false;
    private boolean autoDeleteQueues = false;
    private boolean enableDlqController = true;
    private List<String> scanPackages = new ArrayList<>();

    public boolean isAutoCreateQueues() { return autoCreateQueues; }
    public void setAutoCreateQueues(boolean autoCreateQueues) { this.autoCreateQueues = autoCreateQueues; }

    public String getQueuePrefix() { return queuePrefix; }
    public void setQueuePrefix(String queuePrefix) { this.queuePrefix = queuePrefix; }

    public String getExchangeSuffix() { return exchangeSuffix; }
    public void setExchangeSuffix(String exchangeSuffix) { this.exchangeSuffix = exchangeSuffix; }

    public boolean isDurableQueues() { return durableQueues; }
    public void setDurableQueues(boolean durableQueues) { this.durableQueues = durableQueues; }

    public boolean isExclusiveQueues() { return exclusiveQueues; }
    public void setExclusiveQueues(boolean exclusiveQueues) { this.exclusiveQueues = exclusiveQueues; }

    public boolean isAutoDeleteQueues() { return autoDeleteQueues; }
    public void setAutoDeleteQueues(boolean autoDeleteQueues) { this.autoDeleteQueues = autoDeleteQueues; }

    public boolean isEnableDlqController() { return enableDlqController; }
    public void setEnableDlqController(boolean enableDlqController) { this.enableDlqController = enableDlqController; }

    public List<String> getScanPackages() { return scanPackages; }
    public void setScanPackages(List<String> scanPackages) {
        this.scanPackages = scanPackages != null ? scanPackages : new ArrayList<>();
    }

}
