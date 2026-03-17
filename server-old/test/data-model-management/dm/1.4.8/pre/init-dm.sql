CREATE TABLE IF NOT EXISTS deploy.schema_upgrade_table (
id bigint NOT NULL AUTO_INCREMENT,
service_name varchar(50) NOT NULL ,
script_file_name varchar(255) NOT NULL DEFAULT '' ,
installed_version varchar(255) NOT NULL ,
target_version varchar(50) NOT NULL DEFAULT '' ,
status varchar(50) NOT NULL ,
create_time timestamp NOT NULL ,
update_time timestamp NOT NULL ,
PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS deploy.schema_records(
id bigint NOT NULL AUTO_INCREMENT,
service_name varchar(50) NOT NULL ,
installed_version varchar(50) NOT NULL ,
target_version varchar(50) NOT NULL ,
script_file_name varchar(255) NOT NULL ,
create_time timestamp NOT NULL ,
PRIMARY KEY (id)
);