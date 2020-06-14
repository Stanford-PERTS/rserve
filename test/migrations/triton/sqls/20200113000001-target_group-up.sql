/* triton #1527 preview survey url */

ALTER TABLE `program`
  ADD COLUMN `target_group_enabled` tinyint(1) NOT NULL DEFAULT 1
  AFTER `survey_config_enabled`;
