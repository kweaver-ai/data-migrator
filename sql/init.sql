USE `deploy`;

CREATE TABLE IF NOT EXISTS `schema_upgrade_table` (
  `id` BIGINT(11) NOT NULL AUTO_INCREMENT,
  `service_name` VARCHAR(50) NOT NULL COMMENT '微服务名',
  `script_file_name` VARCHAR(255) NOT NULL DEFAULT '' COMMENT '已执行的脚本名',
  `installed_version` VARCHAR(255) NOT NULL COMMENT '已安装版本',
  `target_version` VARCHAR(50) NOT NULL DEFAULT '' COMMENT '目标版本',
  `status` VARCHAR(50) NOT NULL COMMENT 'pre-start/pre-success/pre-fail/post-fail/success',
  `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '完成时间',
  `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '最后更新时间',
  PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `schema_records` (
  `id` BIGINT(11) NOT NULL AUTO_INCREMENT,
  `service_name` VARCHAR(50) NOT NULL COMMENT '微服务名',
  `installed_version` VARCHAR(50) NOT NULL COMMENT '已安装版本',
  `target_version` VARCHAR(50) NOT NULL COMMENT '目标版本',
  `script_file_name` VARCHAR(255) NOT NULL COMMENT '已执行的脚本名',
  `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `verification_data_records` (
  `d_id` BIGINT(11) NOT NULL AUTO_INCREMENT,
  `ai_id` BIGINT(11) NOT NULL,
  `verify_result` VARCHAR(20) NOT NULL,
  `verify_end_time` INT NOT NULL,
  PRIMARY KEY (`d_id`)
);

CREATE TABLE IF NOT EXISTS `data_test_entries` (
  `t_id` BIGINT(11) NOT NULL AUTO_INCREMENT,
  `d_id` BIGINT(11) NOT NULL,
  `test_result` VARCHAR(20) NOT NULL,
  `test_result_details` VARCHAR(4096) NOT NULL DEFAULT '',
  `service_name` VARCHAR(50) NOT NULL,
  PRIMARY KEY (`t_id`),
  KEY `idx_d_id` (`d_id`)
);