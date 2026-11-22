CREATE TABLE IF NOT EXISTS task (
    task_id        VARCHAR(64)  PRIMARY KEY,
    biz_key        VARCHAR(255) NOT NULL,
    task_type      VARCHAR(64)  NOT NULL,
    priority       INT          NOT NULL DEFAULT 5,
    payload        CLOB,
    status         VARCHAR(32)  NOT NULL,
    fail_reason    VARCHAR(1024),
    retry_count    INT          NOT NULL DEFAULT 0,
    max_retry      INT          NOT NULL DEFAULT 3,
    next_retry_at  TIMESTAMP,
    heartbeat_at   TIMESTAMP,
    enqueued_at    TIMESTAMP,
    started_at     TIMESTAMP,
    finished_at    TIMESTAMP,
    created_at     TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_task_biz_key UNIQUE (biz_key)
);
CREATE INDEX IF NOT EXISTS idx_task_status ON task (status);
CREATE INDEX IF NOT EXISTS idx_task_next_retry ON task (status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_task_heartbeat ON task (status, heartbeat_at);

CREATE TABLE IF NOT EXISTS task_result (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    task_id       VARCHAR(64)   NOT NULL,
    result_data   CLOB,
    success       BOOLEAN       NOT NULL,
    created_at    TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_result_task_id UNIQUE (task_id)
);
CREATE INDEX IF NOT EXISTS idx_result_task ON task_result (task_id);