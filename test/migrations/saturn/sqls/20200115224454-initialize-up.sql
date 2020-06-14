  CREATE TABLE response (
    # Firestore IDs should always be 20 characters. 50 for safety.
    # https://github.com/firebase/firebase-js-sdk/blob/73a586c92afe3f39a844b2be86086fddb6877bb7/packages/firestore/src/util/misc.ts#L36
    firestore_id varchar(50)
      CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT '',

    created datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified datetime NOT NULL DEFAULT CURRENT_TIMESTAMP
      ON UPDATE CURRENT_TIMESTAMP,

    # While it might make sense to want to look at stored responses even when
    # they are missing either code or participant, we can use Firestore for
    # that. Only keep fully qualified responses in this table.
    code varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    participant_id varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,

    survey_label varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    meta text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
    answers text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
    progress tinyint(4) unsigned NOT NULL DEFAULT 0,
    page varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
    PRIMARY KEY (firestore_id),
    INDEX survey_code_created (survey_label, code, created),
    INDEX survey_code (survey_label, code),
    INDEX survey (survey_label)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

  CREATE TABLE response_backup (
    id int(11) unsigned NOT NULL AUTO_INCREMENT,

    # Firestore IDs should always be 20 characters. 50 for safety.
    # https://github.com/firebase/firebase-js-sdk/blob/73a586c92afe3f39a844b2be86086fddb6877bb7/packages/firestore/src/util/misc.ts#L36
    firestore_id varchar(50)
      CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT '',

    created datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
    # No 'modified' here b/c we'll never run an UPDATE on this table.

    # This table attempts to back up everything in the firestore, whether or
    # not it has these conventional properties.
    code varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
    participant_id varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,

    survey_label varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    meta text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
    answers text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
    progress tinyint(4) unsigned NOT NULL DEFAULT 0,
    page varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
    PRIMARY KEY (id),
    INDEX firestore_id (firestore_id)  # NOT unique!
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
