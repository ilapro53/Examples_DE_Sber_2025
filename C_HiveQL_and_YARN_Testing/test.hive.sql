SELECT VERSION();

CREATE TABLE detn.ilka_newtable (
	field1 INTEGER,
	field2 DECIMAL(18,5),
	field3 STRING,
	field4 TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

INSERT INTO detn.ilka_newtable VALUES
    (1, 3.14159, 'Hello World!', current_timestamp())
;

INSERT INTO detn.ilka_newtable VALUES
    (2, 2.71828, 'Foo-Bar', current_timestamp())
;

SELECT * FROM detn.ilka_newtable;

SHOW CREATE TABLE detn.ilka_newtable;

DROP TABLE detn.ilka_newtable;

CREATE EXTERNAL TABLE detn.ilka_top_secret (
	ID integer comment 'Ключ объекта',
	Name string comment 'Наименование объекта',
	AdmArea string comment 'Административный округ',
	District string comment 'Район',
	Location string comment 'Адрес',
	LocationClarification string comment 'Уточнение адреса',
	CloseFlag string comment 'Закрыт',
	CloseReason string comment 'Причина закрытия',
	WorkingHoursMonday string comment 'Рабочие часы в понедельник',
	WorkingHoursTuesday string comment 'Рабочие часы во вторник',
	WorkingHoursWednesday string comment 'Рабочие часы в среду',
	WorkingHoursThursday string comment 'Рабочие часы в четверг',
	WorkingHoursFriday string comment 'Рабочие часы в пятницу',
	WorkingHoursSaturday string comment 'Рабочие часы в субботу',
	WorkingHoursSunday string comment 'Рабочие часы в воскресенье',
	ClarificationOfWorkingHours string comment 'Уточнение рабочего графика',
	DisabilityFriendly string comment 'Приспособлен для инвалидов',
	BalanceHolderName string comment 'Владелец'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/detn/ilka/my_top_secret_directory';

SELECT * FROM detn.ilka_top_secret;

DROP TABLE detn.ilka_top_secret;

CREATE EXTERNAL TABLE detn.ilka_top_secret (
	ID integer comment 'Ключ объекта',
	Name string comment 'Наименование объекта',
	AdmArea string comment 'Административный округ',
	District string comment 'Район',
	Location string comment 'Адрес',
	LocationClarification string comment 'Уточнение адреса',
	CloseFlag string comment 'Закрыт',
	CloseReason string comment 'Причина закрытия',
	WorkingHoursMonday string comment 'Рабочие часы в понедельник',
	WorkingHoursTuesday string comment 'Рабочие часы во вторник',
	WorkingHoursWednesday string comment 'Рабочие часы в среду',
	WorkingHoursThursday string comment 'Рабочие часы в четверг',
	WorkingHoursFriday string comment 'Рабочие часы в пятницу',
	WorkingHoursSaturday string comment 'Рабочие часы в субботу',
	WorkingHoursSunday string comment 'Рабочие часы в воскресенье',
	ClarificationOfWorkingHours string comment 'Уточнение рабочего графика',
	DisabilityFriendly string comment 'Приспособлен для инвалидов',
	BalanceHolderName string comment 'Владелец'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/detn/ilka/my_top_secret_directory';

CREATE TABLE detn.ilka_top_secret_target (
	ID integer comment 'Ключ объекта',
	Name string comment 'Наименование объекта',
	AdmArea string comment 'Административный округ',
	District string comment 'Район',
	Location string comment 'Адрес',
	LocationClarification string comment 'Уточнение адреса',
	CloseFlag string comment 'Закрыт',
	CloseReason string comment 'Причина закрытия',
	WorkingHoursMonday string comment 'Рабочие часы в понедельник',
	WorkingHoursTuesday string comment 'Рабочие часы во вторник',
	WorkingHoursWednesday string comment 'Рабочие часы в среду',
	WorkingHoursThursday string comment 'Рабочие часы в четверг',
	WorkingHoursFriday string comment 'Рабочие часы в пятницу',
	WorkingHoursSaturday string comment 'Рабочие часы в субботу',
	WorkingHoursSunday string comment 'Рабочие часы в воскресенье',
	ClarificationOfWorkingHours string comment 'Уточнение рабочего графика',
	DisabilityFriendly string comment 'Приспособлен для инвалидов',
	BalanceHolderName string comment 'Владелец'
)
STORED AS PARQUET;

INSERT INTO detn.ilka_top_secret_target
SELECT * FROM detn.ilka_top_secret;

SELECT * FROM detn.ilka_top_secret_target;

DROP TABLE detn.ilka_zaryadye;
CREATE TABLE detn.ilka_zaryadye (
  id double,
  name STRING,
  name_latin STRING,
  landscape STRING,
  bloom_period STRING,
  description STRING
)
STORED AS PARQUET;

-- /user/detn/ilka/zaryadye.parquet
LOAD DATA INPATH '/user/detn/ilka/zaryadye.parquet' INTO TABLE detn.ilka_zaryadye;

SELECT * FROM detn.ilka_zaryadye LIMIT 10;