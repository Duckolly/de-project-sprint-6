CREATE TABLE STV2024050744__STAGING.users (
    id INT PRIMARY KEY,
    chat_name VARCHAR(200),
    registration_dt DATETIME,
    country VARCHAR(200),
    age INT CHECK (age >= 0)
)
ORDER BY id;

CREATE TABLE STV2024050744__STAGING.groups (
    id INT PRIMARY KEY,
    admin_id INT,
    group_name VARCHAR(100),
    registration_dt DATETIME,
    is_private BOOLEAN
)
order by id, admin_id
PARTITION BY registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);

CREATE TABLE STV2024050744__STAGING.dialogs (
    message_id INT PRIMARY KEY,
    message_ts TIMESTAMP,
    message_from INT,
    message_to INT,
    message VARCHAR(1000),
    message_group INT
)ORDER BY message_id
PARTITION BY message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);

drop table if exists STV202312114__STAGING.group_log;
create table STV202312114__STAGING.group_log(
    group_id int,
    user_id int,
    user_id_from int,
    event varchar(20),
    datetime timestamp
)
ORDER BY group_id
SEGMENTED BY HASH(group_id) ALL NODES
PARTITION BY datetime::date
GROUP BY calendar_hierarchy_day(datetime::date,3,2)
;

ALTER TABLE STV2024050744__STAGING.users
ADD CONSTRAINT chat_name_length CHECK (LENGTH(chat_name) <= 200);

ALTER TABLE STV2024050744__STAGING.users
ADD CONSTRAINT country_length CHECK (LENGTH(country) <= 200);

ALTER TABLE STV2024050744__STAGING.groups
ADD CONSTRAINT group_name_length CHECK (LENGTH(group_name) <= 100);

ALTER TABLE STV2024050744__STAGING.dialogs
ADD CONSTRAINT message_length CHECK (LENGTH(message) <= 1000);

ALTER TABLE STV2024050744__STAGING.dialogs
ADD CONSTRAINT fk_message_from FOREIGN KEY (message_from) REFERENCES STV2024050744__STAGING.users(id);

ALTER TABLE STV2024050744__STAGING.dialogs
ADD CONSTRAINT fk_message_to FOREIGN KEY (message_to) REFERENCES STV2024050744__STAGING.users(id);

ALTER TABLE STV2024050744__STAGING.groups
ADD CONSTRAINT fk_admin_id FOREIGN KEY (admin_id) REFERENCES STV2024050744__STAGING.users(id);

ALTER TABLE STV2024050744__STAGING.dialogs
ADD CONSTRAINT fk_group_id FOREIGN KEY (message_group) REFERENCES STV2024050744__STAGING.groups(id);

ALTER TABLE STV2024050744__STAGING.groups
ADD CONSTRAINT is_private_check CHECK (is_private IN (FALSE, TRUE));