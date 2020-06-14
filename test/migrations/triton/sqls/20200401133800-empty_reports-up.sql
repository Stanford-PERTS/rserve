# https://github.com/PERTS/analysis/issues/214 add `issue_date`

ALTER TABLE `report` MODIFY COLUMN `content_type` varchar(200) DEFAULT NULL;
ALTER TABLE `report` ADD COLUMN `issue_date` date DEFAULT NULL AFTER `filename`;
ALTER TABLE `report` ADD COLUMN `notes` text AFTER `preview`;

UPDATE `report` SET `issue_date` = SUBSTRING_INDEX(`filename`, '.', 1);
