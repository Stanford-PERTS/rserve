CREATE TABLE `checkpoint` (
  `uid` varchar(50) NOT NULL,
  `short_uid` varchar(50) NOT NULL,
  `label` varchar(50) NOT NULL,
  `parent_id` varchar(50) NOT NULL,
  `parent_kind` varchar(50) NOT NULL,
  `name` text NOT NULL,
  `ordinal` tinyint(3) unsigned NOT NULL,
  `status` varchar(50) NOT NULL DEFAULT 'incomplete',
  `survey_id` varchar(50) DEFAULT NULL,
  `project_cohort_id` varchar(50) DEFAULT NULL,
  `cohort_label` varchar(100) DEFAULT NULL,
  `project_id` varchar(50) DEFAULT NULL,
  `program_label` varchar(50) DEFAULT NULL,
  `organization_id` varchar(50) DEFAULT NULL,
  `task_ids` text,
  PRIMARY KEY (`uid`),
  KEY `ordinal` (`ordinal`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `participant` (
  `uid` varchar(50) NOT NULL,
  `short_uid` varchar(50) NOT NULL,
  `created` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `name` varchar(128) DEFAULT NULL,
  `organization_id` varchar(50) NOT NULL,
  PRIMARY KEY (`uid`),
  UNIQUE KEY `name_organization` (`name`,`organization_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `participant_data` (
  `uid` varchar(50) NOT NULL,
  `short_uid` varchar(50) NOT NULL,
  `created` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `key` varchar(50) NOT NULL,
  `value` text NOT NULL,
  `participant_id` varchar(50) NOT NULL,
  `program_label` varchar(50) NOT NULL,
  `project_id` varchar(50) DEFAULT NULL,
  `cohort_label` varchar(50) DEFAULT NULL,
  `project_cohort_id` varchar(50) NOT NULL,
  `code` varchar(50) NOT NULL,
  `survey_id` varchar(50) DEFAULT NULL,
  `survey_ordinal` tinyint(3) unsigned DEFAULT NULL,
  `testing` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`uid`),
  UNIQUE KEY `participant-survey-key` (`participant_id`,`survey_id`,`key`),
  KEY `participant_id` (`participant_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
