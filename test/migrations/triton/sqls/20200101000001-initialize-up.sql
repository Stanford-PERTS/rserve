CREATE TABLE `classroom` (
  `uid` varchar(50) NOT NULL,
  `short_uid` varchar(50) NOT NULL,
  `created` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `name` varchar(200) DEFAULT NULL,
  `team_id` varchar(50) NOT NULL,
  `code` varchar(50) NOT NULL,
  `contact_id` varchar(50) NOT NULL,
  `num_students` smallint(5) unsigned NOT NULL DEFAULT '0',
  `grade_level` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `cycle` (
  `uid` varchar(50) NOT NULL,
  `short_uid` varchar(50) NOT NULL,
  `created` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `team_id` varchar(50) NOT NULL,
  `ordinal` tinyint(4) unsigned NOT NULL DEFAULT '1',
  `start_date` date DEFAULT NULL,
  `end_date` date DEFAULT NULL,
  `meeting_datetime` datetime DEFAULT NULL,
  `meeting_location` varchar(200) DEFAULT NULL,
  `resolution_date` date DEFAULT NULL,
  `students_completed` smallint(5) unsigned DEFAULT NULL,
  PRIMARY KEY (`uid`),
  KEY `team` (`team_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `digest` (
  `uid` varchar(50) NOT NULL,
  `short_uid` varchar(50) NOT NULL,
  `created` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `user_id` varchar(50) NOT NULL,
  `type` varchar(50) NOT NULL,
  `body` text NOT NULL,
  `read` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`uid`),
  KEY `user` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `metric` (
  `uid` varchar(50) NOT NULL,
  `short_uid` varchar(50) NOT NULL,
  `created` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `name` varchar(200) NOT NULL,
  `label` varchar(50) NOT NULL,
  `description` text,
  `links` varchar(2000) NOT NULL DEFAULT '[]',
  PRIMARY KEY (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `notification` (
  `uid` varchar(50) NOT NULL,
  `short_uid` varchar(50) NOT NULL,
  `created` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `user_id` varchar(50) NOT NULL,
  `type` varchar(50) NOT NULL,
  `template_params` varchar(2000) NOT NULL DEFAULT '{}',
  PRIMARY KEY (`uid`),
  KEY `user-created` (`user_id`,`created`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `organization` (
  `uid` varchar(50) NOT NULL,
  `short_uid` varchar(50) NOT NULL,
  `created` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `name` varchar(200) DEFAULT NULL,
  `code` varchar(50) NOT NULL,
  `phone_number` varchar(50) DEFAULT NULL,
  `mailing_address` varchar(500) DEFAULT NULL,
  `program_id` varchar(50) NOT NULL,
  PRIMARY KEY (`uid`),
  UNIQUE KEY `code` (`code`),
  KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `participant` (
  `uid` varchar(50) NOT NULL,
  `short_uid` varchar(50) NOT NULL,
  `created` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `student_id` varchar(100) NOT NULL,
  `stripped_student_id` varchar(100) NOT NULL,
  `team_id` varchar(50) NOT NULL,
  `classroom_ids` varchar(3500) NOT NULL,
  `in_target_group` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`uid`),
  UNIQUE KEY `team-student` (`team_id`,`stripped_student_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `program` (
  `uid` varchar(50) NOT NULL,
  `short_uid` varchar(50) NOT NULL,
  `created` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `name` varchar(200) NOT NULL,
  `label` varchar(50) NOT NULL,
  `metrics` varchar(3500) NOT NULL DEFAULT '[]',
  `survey_config_enabled` tinyint(1) NOT NULL DEFAULT '1',
  `max_cycles` tinyint(3) NOT NULL DEFAULT '-1',
  `min_cycles` tinyint(3) unsigned NOT NULL DEFAULT '3',
  `min_cycle_weekdays` tinyint(3) unsigned NOT NULL DEFAULT '5',
  `send_cycle_email` tinyint(1) NOT NULL DEFAULT '1',
  `max_team_members` tinyint(3) NOT NULL DEFAULT '-1',
  `team_term` varchar(50) NOT NULL DEFAULT 'Team',
  `classroom_term` varchar(50) NOT NULL DEFAULT 'Class',
  `captain_term` varchar(50) NOT NULL DEFAULT 'Captain',
  `contact_term` varchar(50) NOT NULL DEFAULT 'Main Contact',
  `member_term` varchar(50) NOT NULL DEFAULT 'Teacher',
  `active` tinyint(1) NOT NULL DEFAULT '1',
  PRIMARY KEY (`uid`),
  UNIQUE KEY `label` (`label`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `report` (
  `uid` varchar(50) NOT NULL,
  `short_uid` varchar(50) NOT NULL,
  `created` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `parent_id` varchar(50) NOT NULL,
  `organization_id` varchar(50) DEFAULT NULL,
  `team_id` varchar(50) DEFAULT NULL,
  `classroom_id` varchar(50) DEFAULT NULL,
  `dataset_id` varchar(50) DEFAULT NULL,
  `template` varchar(200) DEFAULT NULL,
  `filename` varchar(200) NOT NULL,
  `gcs_path` varchar(200) DEFAULT NULL,
  `size` int(4) unsigned DEFAULT NULL,
  `content_type` varchar(200) NOT NULL,
  `preview` tinyint(1) NOT NULL DEFAULT '1',
  PRIMARY KEY (`uid`),
  UNIQUE KEY `parent-file` (`parent_id`,`filename`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `response` (
  `uid` varchar(50) NOT NULL,
  `short_uid` varchar(50) NOT NULL,
  `created` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `type` varchar(20) NOT NULL,
  `user_id` varchar(50) NOT NULL,
  `team_id` varchar(50) NOT NULL,
  `parent_id` varchar(50) DEFAULT NULL,
  `module_label` varchar(50) NOT NULL,
  `progress` tinyint(4) unsigned NOT NULL DEFAULT '0',
  `page` tinyint(4) unsigned DEFAULT NULL,
  `body` text,
  PRIMARY KEY (`uid`),
  UNIQUE KEY `type-user-team-parent-module` (`type`,`user_id`,`team_id`,`parent_id`,`module_label`),
  KEY `parent` (`parent_id`),
  KEY `user` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `response_backup` (
  `backup_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `uid` varchar(50) NOT NULL,
  `short_uid` varchar(50) NOT NULL,
  `created` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `type` varchar(20) NOT NULL,
  `user_id` varchar(50) NOT NULL,
  `team_id` varchar(50) NOT NULL,
  `parent_id` varchar(50) DEFAULT NULL,
  `module_label` varchar(50) NOT NULL,
  `progress` tinyint(4) unsigned NOT NULL DEFAULT '0',
  `page` tinyint(4) unsigned DEFAULT NULL,
  `body` text,
  PRIMARY KEY (`backup_id`),
  KEY `uid` (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `survey` (
  `uid` varchar(50) NOT NULL,
  `short_uid` varchar(50) NOT NULL,
  `created` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `team_id` varchar(50) NOT NULL,
  `portal_message` varchar(2000) DEFAULT NULL,
  `metrics` varchar(3500) NOT NULL DEFAULT '[]',
  `open_responses` varchar(3500) NOT NULL DEFAULT '[]',
  `interval` int(1) unsigned NOT NULL DEFAULT '2',
  `notified` datetime DEFAULT NULL,
  PRIMARY KEY (`uid`),
  UNIQUE KEY `team` (`team_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `team` (
  `uid` varchar(50) NOT NULL,
  `short_uid` varchar(50) NOT NULL,
  `created` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `name` varchar(200) DEFAULT NULL,
  `captain_id` varchar(50) NOT NULL,
  `organization_ids` varchar(3500) NOT NULL DEFAULT '[]',
  `program_id` varchar(50) NOT NULL,
  `survey_reminders` tinyint(1) NOT NULL DEFAULT '1',
  `report_reminders` tinyint(1) NOT NULL DEFAULT '1',
  `target_group_name` varchar(200) DEFAULT NULL,
  `task_data` text NOT NULL,
  PRIMARY KEY (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `user` (
  `uid` varchar(50) NOT NULL,
  `short_uid` varchar(50) NOT NULL,
  `created` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `name` varchar(200) DEFAULT NULL,
  `email` varchar(200) NOT NULL,
  `phone_number` varchar(50) DEFAULT NULL,
  `user_type` varchar(50) NOT NULL DEFAULT 'user',
  `owned_teams` varchar(3500) NOT NULL DEFAULT '[]',
  `owned_organizations` varchar(3500) NOT NULL DEFAULT '[]',
  `receive_email` tinyint(1) NOT NULL DEFAULT '1',
  `receive_sms` tinyint(1) NOT NULL DEFAULT '1',
  `consent` tinyint(1) unsigned DEFAULT NULL,
  `recent_program_id` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`uid`),
  UNIQUE KEY `email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
