CREATE DATABASE IF NOT EXISTS notify_control CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
USE notify_control;
CREATE TABLE IF NOT EXISTS config_revision (
 scope VARCHAR(64) PRIMARY KEY, revision BIGINT UNSIGNED NOT NULL, updated_at DATETIME(6) NOT NULL
) ENGINE=InnoDB;
INSERT IGNORE INTO config_revision VALUES ('global',0,UTC_TIMESTAMP(6));
-- Each document is a complete, immutable version of all management entities.
-- Foreign-reference and CEL validation is performed by notify-admin before commit.
CREATE TABLE IF NOT EXISTS config_snapshot (
 revision BIGINT UNSIGNED PRIMARY KEY, document JSON NOT NULL,
 created_at DATETIME(6) NOT NULL, actor VARCHAR(128) NOT NULL
) ENGINE=InnoDB;
