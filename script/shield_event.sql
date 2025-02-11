SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for shield_event
-- ----------------------------
DROP TABLE IF EXISTS `shield_event`;
CREATE TABLE `shield_event` (
    `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `event_id` varchar(64) CHARACTER SET utf8 NOT NULL DEFAULT '-1' COMMENT '事件id',
    `success` bool NOT NULL DEFAULT false COMMENT '是否成功',
    `tx_type` varchar(32) CHARACTER SET utf8 NOT NULL DEFAULT 'OTHER' COMMENT '事务类型 COMMIT ROLLBACK',
    `event_status` varchar(32) CHARACTER SET utf8 NOT NULL DEFAULT 'INIT' COMMENT '事件处理状态, INIT PUBLISHING PUBLISHED PROCESSING PROCESSED',
    `content` text CHARACTER SET utf8 COMMENT '业务参数',
    `app_id` varchar(128) CHARACTER SET utf8 DEFAULT 'DEFAULT_APP' COMMENT '应用名',
    `record_status` tinyint(1) NOT NULL DEFAULT '0' COMMENT '数据状态，0-有效，1-逻辑删除',
    `before_update_event_status` varchar(32) CHARACTER SET utf8 NOT NULL DEFAULT '' COMMENT '先前事件处理状态, INIT PUBLISHING PUBLISHED PROCESSING PROCESSED',
    `gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `gmt_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unk_message_exists` (`tx_type`, `event_id`, `app_id`) USING BTREE,
    INDEX `idx_event_status` (`event_status`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=51 DEFAULT CHARSET=utf8 COMMENT='事件表';