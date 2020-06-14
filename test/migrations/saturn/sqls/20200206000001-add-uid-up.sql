ALTER TABLE `response`
  ADD COLUMN `uid` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin
  NOT NULL DEFAULT '' AFTER `firestore_id`;

ALTER TABLE `response_backup`
  ADD COLUMN `uid` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin
  NOT NULL DEFAULT '' AFTER `firestore_id`;

UPDATE `response`
SET `uid` = CONCAT('Response_', `firestore_id`);

UPDATE `response_backup`
SET `uid` = CONCAT('Response_', `firestore_id`);
