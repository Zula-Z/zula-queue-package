package com.zula.queue.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DlqPersistenceServiceTest {

    @Test
    void usesMysqlAutoIncrementForMsProvider() {
        assertThat(DlqPersistenceService.autoIncrementPrimaryKey("MySQL"))
                .isEqualTo("BIGINT AUTO_INCREMENT PRIMARY KEY");
    }

    @Test
    void usesPostgresBigserialByDefault() {
        assertThat(DlqPersistenceService.autoIncrementPrimaryKey("PostgreSQL"))
                .isEqualTo("BIGSERIAL PRIMARY KEY");
    }
}
