CREATE DATABASE IF NOT EXISTS media_db
    DEFAULT CHARACTER SET utf8
    DEFAULT COLLATE utf8_general_ci;

USE media_db;

DROP TABLE IF EXISTS `tb_media_prompt`;

CREATE TABLE `tb_media_prompt` (
    `id`                 BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键',
    `task_id`            BIGINT       NOT NULL                COMMENT '任务ID',
    `scene_index`        INT          NOT NULL                COMMENT '场次序号（从1开始）',
    `storyboard_index`   INT          DEFAULT NULL            COMMENT '分镜序号（场级提示词为NULL）',
    `prompt_type`        VARCHAR(50)  NOT NULL                COMMENT '提示词类型：video_prompt/multi_view_prompt/text_to_image/spatial_composition_drawing/fusion_image/sound_effect/special_effect',
    `prompt_content`     TEXT                                 COMMENT '生成的提示词内容',
    `status`             TINYINT      NOT NULL DEFAULT 0      COMMENT '0=成功 1=失败',
    `error_message`      TEXT         DEFAULT NULL            COMMENT '失败原因',
    `llm_error_code`     VARCHAR(64)  DEFAULT NULL            COMMENT 'LLM失败错误码（如HTTP_429/NETWORK_ERROR）',
    `llm_response_snippet` TEXT       DEFAULT NULL            COMMENT 'LLM失败响应片段（截断）',
    `llm_duration_ms`    BIGINT       DEFAULT NULL            COMMENT 'LLM调用耗时（毫秒）',
    `token_usage`        INT          DEFAULT NULL            COMMENT 'Token消耗量',
    `llm_retries`        INT          DEFAULT NULL            COMMENT 'LLM调用重试次数',
    `created_at`         DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (`id`),
    INDEX `idx_task_id` (`task_id`),
    INDEX `idx_task_scene` (`task_id`, `scene_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='LLM提示词生成结果表';
