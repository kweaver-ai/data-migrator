CREATE TABLE t_user33 (
  f_user_id bigint NOT NULL AUTO_INCREMENT COMMENT '用户id',
  f_username varchar(45) NOT NULL COMMENT '真实姓名',
  f_email varchar(30) NOT NULL COMMENT '用户邮箱',
  f_nickname varchar(45) NOT NULL COMMENT '昵称',
  f_avatar int NOT NULL COMMENT '头像',
  f_birthday date NOT NULL COMMENT '生日',
  f_sex tinyint COMMENT '性别',
  f_short_introduce varchar(150) DEFAULT NULL COMMENT '一句话介绍自己，最多50个汉字',
  f_user_resume varchar(300) NOT NULL COMMENT '用户提交的简历存放地址',
  f_user_register_ip int NOT NULL COMMENT '用户注册时的源ip',
  f_create_time timestamp NOT NULL COMMENT '用户记录创建的时间',
  f_update_time timestamp NOT NULL COMMENT '用户资料修改的时间',
  f_user_review_status tinyint NOT NULL COMMENT '用户资料审核状态，1为通过，2为审核中，3为未通过，4为还未提交审核',
  PRIMARY KEY (f_user_id),
  UNIQUE (f_email)
);