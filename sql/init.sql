USE `deploy`;

-- 迁移任务表：记录每个服务的迁移任务状态
CREATE TABLE IF NOT EXISTS `schema_migration_task` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `service_name` VARCHAR(255) NOT NULL COMMENT '微服务名',
  `installed_version` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '已安装版本',
  `target_version` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '目标版本',
  `script_file_name` VARCHAR(512) NOT NULL DEFAULT '' COMMENT '当前/最后执行的脚本',
  `status` VARCHAR(32) NOT NULL DEFAULT 'pending' COMMENT 'pending/running/success/failed',
  `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '最后更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_service_name` (`service_name`)
);

-- 迁移历史表：记录每次脚本执行的详细历史（含 checksum）
CREATE TABLE IF NOT EXISTS `schema_migration_history` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `service_name` VARCHAR(255) NOT NULL COMMENT '微服务名',
  `version` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '脚本所属版本',
  `script_file_name` VARCHAR(512) NOT NULL DEFAULT '' COMMENT '脚本文件名',
  `checksum` VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'SHA-256 校验和',
  `status` VARCHAR(32) NOT NULL DEFAULT 'success' COMMENT 'success/failed',
  `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '执行时间',
  PRIMARY KEY (`id`),
  KEY `idx_service_version` (`service_name`, `version`)
);
