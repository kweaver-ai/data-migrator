USE deploy;
CREATE TABLE IF NOT EXISTS schema_upgrade_table (
id bigint(11) NOT NULL AUTO_INCREMENT,
service_name varchar(50) NOT NULL COMMENT '微服务名',
script_file_name varchar(255) NOT NULL DEFAULT '' COMMENT '已执行的脚本名',
installed_version varchar(255) NOT NULL COMMENT '已安装版本',
target_version varchar(50) NOT NULL DEFAULT '' COMMENT '目标版本',
status varchar(50) NOT NULL COMMENT 'start/running/failed/success, start标识为pre开始，且在执行过程中。running标识为pre阶段已经执行完成。fail标识为上一个脚本执行失败，程序读到这个状态时，再次执行上一次执行过的脚本。success标识为升级成功。',
create_time datetime DEFAULT CURRENT_TIMESTAMP COMMENT '完成时间',
update_time datetime DEFAULT CURRENT_TIMESTAMP COMMENT '最后更新时间',
PRIMARY KEY (id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS schema_records(
id bigint(11) NOT NULL AUTO_INCREMENT,
service_name varchar(50) NOT NULL COMMENT '微服务名',
installed_version varchar(50) NOT NULL COMMENT '已安装版本',
target_version varchar(50) NOT NULL COMMENT '目标版本',
script_file_name varchar(255) NOT NULL COMMENT '已执行的脚本名',
create_time datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
PRIMARY KEY (id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- CREATE TABLE IF NOT EXISTS schema_change(
-- id bigint(11) NOT NULL AUTO_INCREMENT,
-- module_name varchar(50) NOT NULL COMMENT '模块化服务名',
-- status varchar(50) NOT NULL COMMENT 'start/running/failed/success',
-- create_time datetime DEFAULT CURRENT_TIMESTAMP COMMENT '完成时间',
-- PRIMARY KEY (id),
-- UNIQUE KEY `module_name` (`module_name`)
-- )ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;