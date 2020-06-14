# https://github.com/PERTS/analysis/issues/214 add `issue_date`

ALTER TABLE `report` MODIFY COLUMN `content_type` varchar(200) NOT NULL;
ALTER TABLE `report` DROP COLUMN `issue_date`;
ALTER TABLE `report` DROP COLUMN `notes`;
