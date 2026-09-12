CREATE DATABASE IF NOT EXISTS notify_business CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
USE notify_business;

CREATE TABLE IF NOT EXISTS notification_batch (
 id BINARY(16) PRIMARY KEY, client_id VARCHAR(64) NOT NULL,
 item_count INT UNSIGNED NOT NULL, created_at DATETIME(6) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS notification_task (
 id BINARY(16) PRIMARY KEY,
 client_id VARCHAR(64) NOT NULL, target_id VARCHAR(64) NOT NULL, batch_id BINARY(16) NULL,
 client_idem_key VARCHAR(128) NOT NULL, request_hash BINARY(32) NOT NULL,
 request_json JSON NOT NULL,
 delivery_mode VARCHAR(16) NOT NULL, status VARCHAR(16) NOT NULL,
 attempt_count INT UNSIGNED NOT NULL DEFAULT 0,
 cycle_attempt_count INT UNSIGNED NOT NULL DEFAULT 0,
 dispatch_generation BIGINT UNSIGNED NOT NULL DEFAULT 1,
 next_attempt_at DATETIME(6) NOT NULL, retry_deadline_at DATETIME(6) NOT NULL,
 lease_token BINARY(16) NULL, lease_until DATETIME(6) NULL,
 last_http_status INT NOT NULL DEFAULT 0, last_error_code VARCHAR(64) NOT NULL DEFAULT '',
 target_revision BIGINT UNSIGNED NOT NULL, retry_revision BIGINT UNSIGNED NOT NULL,
 hook_revision BIGINT UNSIGNED NOT NULL, config_revision BIGINT UNSIGNED NOT NULL,
 created_at DATETIME(6) NOT NULL, updated_at DATETIME(6) NOT NULL, delivered_at DATETIME(6) NULL,
 UNIQUE KEY uk_client_idem (client_id, client_idem_key),
 KEY idx_status_next (status, next_attempt_at),
 KEY idx_target_status (target_id,status,next_attempt_at), KEY idx_batch(batch_id),
 KEY idx_lease(status,lease_until)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS delivery_attempt (
 id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, notification_id BINARY(16) NOT NULL,
 attempt_no INT UNSIGNED NOT NULL, dispatch_generation BIGINT UNSIGNED NOT NULL,
 lease_token BINARY(16) NOT NULL, started_at DATETIME(6) NOT NULL, finished_at DATETIME(6) NULL,
 http_status INT NOT NULL DEFAULT 0, result VARCHAR(16) NOT NULL,
 error_code VARCHAR(64) NOT NULL DEFAULT '', latency_ms BIGINT UNSIGNED NOT NULL DEFAULT 0,
 response_headers_json JSON NULL, response_body_preview VARBINARY(8192) NULL,
 response_body_hash BINARY(32) NULL, hook_result_json JSON NULL,
 UNIQUE KEY uk_attempt(notification_id,attempt_no)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS mq_outbox (
 id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, event_id BINARY(16) NOT NULL,
 notification_id BINARY(16) NOT NULL, generation BIGINT UNSIGNED NOT NULL,
 event_type VARCHAR(32) NOT NULL, routing_key VARCHAR(128) NOT NULL,
 available_at DATETIME(6) NOT NULL, payload_json JSON NOT NULL,
 status VARCHAR(16) NOT NULL, publish_token BINARY(16) NULL, lease_until DATETIME(6) NULL,
 published_at DATETIME(6) NULL, created_at DATETIME(6) NOT NULL,
 UNIQUE KEY uk_event_id(event_id), KEY idx_outbox_pending(status,available_at,id),
 KEY idx_notification_generation(notification_id,generation,status), KEY idx_publish_lease(status,lease_until)
) ENGINE=InnoDB;
