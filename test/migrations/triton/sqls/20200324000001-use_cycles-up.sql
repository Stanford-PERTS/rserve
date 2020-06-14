/* triton #1632 cycleless mode */

ALTER TABLE `program`
  ADD COLUMN `use_cycles` tinyint(1) NOT NULL DEFAULT 1
  AFTER `target_group_enabled`;
