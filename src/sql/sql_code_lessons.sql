drop table if exists STV2024050744__DWH.s_admins;

create table STV2024050744__DWH.s_admins
(
hk_admin_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES STV2024050744__DWH.l_admins (hk_l_admin_id),
is_admin boolean,
admin_from datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV2024050744__DWH.s_admins(hk_admin_id, is_admin,admin_from,load_dt,load_src)
select la.hk_l_admin_id,
True as is_admin,
hg.registration_dt,
now() as load_dt,
's3' as load_src
from STV2024050744__DWH.l_admins as la
left join STV2024050744__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;

drop table if exists STV2024050744__DWH.s_group_name;

create table STV2024050744__DWH.s_group_name
(
hk_group_id bigint not null CONSTRAINT fk_s_group_name_h_groups REFERENCES STV2024050744__DWH.h_groups (hk_group_id),
group_name varchar(128),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV2024050744__DWH.s_group_name(hk_group_id, group_name,load_dt,load_src)
select hg.hk_group_id,
g.group_name,
now() as load_dt,
's3' as load_src
from STV2024050744__DWH.h_groups as hg
left join STV2024050744__STAGING.groups as g on g.id = hg.group_id;

drop table if exists STV2024050744__DWH.s_group_private_status;

create table STV2024050744__DWH.s_group_private_status
(
hk_group_id bigint not null CONSTRAINT fk_s_group_name_h_groups REFERENCES STV2024050744__DWH.h_groups (hk_group_id),
is_private boolean,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV2024050744__DWH.s_group_private_status(hk_group_id, is_private,load_dt,load_src)
select hg.hk_group_id,
g.is_private,
now() as load_dt,
's3' as load_src
from STV2024050744__DWH.h_groups as hg
left join STV2024050744__STAGING.groups as g on g.id = hg.group_id;

drop table if exists STV2024050744__DWH.s_dialog_info;

create table STV2024050744__DWH.s_dialog_info
(
hk_message_id bigint not null CONSTRAINT fk_s_dialog_info_h_dialogs REFERENCES STV2024050744__DWH.h_dialogs (hk_message_id),
message varchar(1024),
message_from int,
message_to int,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV2024050744__DWH.s_dialog_info(hk_message_id, message,message_from,message_to,load_dt,load_src)
select hd.hk_message_id,
d.message,
d.message_from,
d.message_to,
now() as load_dt,
's3' as load_src
from STV2024050744__DWH.h_dialogs as hd
left join STV2024050744__STAGING.dialogs as d on hd.message_id = d.message_id;

drop table if exists STV2024050744__DWH.s_user_socdem;

create table STV2024050744__DWH.s_user_socdem
(
hk_user_id bigint not null CONSTRAINT fk_s_user_socdem_h_users REFERENCES STV2024050744__DWH.h_users (hk_user_id),
country varchar(256),
age int,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV2024050744__DWH.s_user_socdem(hk_user_id,country,age,load_dt,load_src)
select hu.hk_user_id,
u.country,
u.age,
now() as load_dt,
's3' as load_src
from STV2024050744__DWH.h_users as hu
left join STV2024050744__STAGING.users as u on hu.user_id = u.id;

drop table if exists STV2024050744__DWH.s_user_chatinfo;

create table STV2024050744__DWH.s_user_chatinfo
(
hk_user_id bigint not null CONSTRAINT fk_s_user_socdem_h_users REFERENCES STV2024050744__DWH.h_users (hk_user_id),
chat_name varchar(256),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV2024050744__DWH.s_user_chatinfo(hk_user_id, chat_name,load_dt,load_src)
select hu.hk_user_id,
u.chat_name,
now() as load_dt,
's3' as load_src
from STV2024050744__DWH.h_users as hu
left join STV2024050744__STAGING.users as u on hu.user_id = u.id;

drop table if exists STV2024050744__DWH.s_auth_history;

CREATE TABLE STV2024050744__DWH.l_user_group_activity
(
    hk_l_user_group_activity INT PRIMARY KEY,  -- Основной ключ
    hk_user_id INT NOT NULL CONSTRAINT fk_l_user_group_activity_user_id REFERENCES STV2024050744__DWH.h_users (hk_user_id),
    hk_group_id INT CONSTRAINT fk_l_user_group_activity_group_id REFERENCES STV2024050744__DWH.h_groups (hk_group_id),
    load_dt DATETIME,                          -- Временная отметка о том, когда были загружены данные
    load_src VARCHAR(20)                       -- Данные об источнике
)
ORDER BY
    load_dt
SEGMENTED BY
    hash(hk_l_user_group_activity) ALL NODES
PARTITION BY
    load_dt::date
GROUP BY
    calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV2024050744__DWH.l_user_group_activity (hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)

SELECT DISTINCT
    gl.event_id AS hk_l_user_group_activity,
    hu.hk_user_id AS hk_user_id,
    hg.hk_group_id AS hk_group_id,
    NOW() AS load_dt,
    's3' AS load_src

FROM STV2024050744__STAGING.group_log AS gl
LEFT JOIN STV2024050744__DWH.h_users AS hu ON gl.user_id = hu.user_id
LEFT JOIN STV2024050744__DWH.h_groups AS hg ON gl.group_id = hg.group_id;

create table STV2024050744__DWH.s_auth_history
(
hk_l_user_group_activity bigint not null CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES STV2024050744__DWH.l_user_group_activity (hk_l_user_group_activity),
user_id_from int,
event varchar(256),
event_dt datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV2024050744__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)

SELECT DISTINCT
    luga.hk_l_user_group_activity,   -
    gl.user_id_from,                 
    gl.event,                        
    gl.event_datetime AS event_dt,   
    NOW() as load_dt,                      
    's3' AS load_src 

FROM STV2024050744__STAGING.group_log AS gl
LEFT JOIN STV2024050744__DWH.h_groups AS hg ON gl.group_id = hg.group_id
LEFT JOIN STV2024050744__DWH.h_users AS hu ON gl.user_id = hu.user_id
LEFT JOIN STV2024050744__DWH.l_user_group_activity AS luga ON hg.hk_group_id = luga.hk_group_id AND hu.hk_user_id = luga.hk_user_id;



INSERT INTO STV2024050744__DWH.l_user_message (hk_l_user_message, hk_user_id, hk_message_id, load_dt, load_src)
SELECT
  HASH(hu.hk_user_id, hd.hk_message_id) AS hk_l_user_message,
  hu.hk_user_id,
  hd.hk_message_id,
  NOW() AS load_dt,
  's3' AS load_src
FROM STV2024050744__STAGING.dialogs AS d
LEFT JOIN STV2024050744__DWH.h_users AS hu ON d.message_from = hu.user_id
LEFT JOIN STV2024050744__DWH.h_dialogs AS hd ON d.message_id = hd.message_id
WHERE hu.hk_user_id IS NOT NULL
  AND hd.hk_message_id IS NOT NULL
  AND HASH(hu.hk_user_id, hd.hk_message_id) NOT IN (SELECT hk_l_user_message FROM STV2024050744__DWH.l_user_message);


INSERT INTO STV2024050744__DWH.l_groups_dialogs (hk_l_groups_dialogs, hk_group_id, hk_message_id, load_dt, load_src)
SELECT
  HASH(hg.hk_group_id, hd.hk_message_id) AS hk_l_groups_dialogs,
  hg.hk_group_id,
  hd.hk_message_id,
  NOW() AS load_dt,
  's3' AS load_src
FROM STV2024050744__STAGING.dialogs AS d
LEFT JOIN STV2024050744__DWH.h_dialogs AS hd ON d.message_id = hd.message_id
LEFT JOIN STV2024050744__DWH.h_groups AS hg ON d.message_group = hg.group_id
WHERE HASH(hg.hk_group_id, hd.hk_message_id) NOT IN (SELECT hk_l_groups_dialogs FROM STV2024050744__DWH.l_groups_dialogs)
AND hg.hk_group_id IS NOT NULL;




INSERT INTO STV2024050744__DWH.l_admins(hk_l_admin_id, hk_group_id,hk_user_id,load_dt,load_src)
select
hash(hg.hk_group_id,hu.hk_user_id),
hg.hk_group_id,
hu.hk_user_id,
now() as load_dt,
's3' as load_src
from STV2024050744__STAGING.groups as g
left join STV2024050744__DWH.h_users as hu on g.admin_id = hu.user_id
left join STV2024050744__DWH.h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_admin_id from STV2024050744__DWH.l_admins); 



CREATE TABLE STV2024050744__DWH.l_admins
(
    hk_l_admin_id BIGINT PRIMARY KEY,
    hk_user_id BIGINT NOT NULL CONSTRAINT fk_l_admins_user REFERENCES STV2024050744__DWH.h_users (hk_user_id),
    hk_group_id BIGINT NOT NULL CONSTRAINT fk_l_admins_group REFERENCES STV2024050744__DWH.h_groups (hk_group_id),
    load_dt DATETIME,
    load_src VARCHAR(20)
)
ORDER BY 
  load_dt
SEGMENTED BY 
  hk_l_admin_id ALL NODES
PARTITION BY 
  load_dt::date
GROUP BY 
  calendar_hierarchy_day(load_dt::date, 3, 2);


CREATE TABLE STV2024050744__DWH.l_groups_dialogs
(
    hk_l_groups_dialogs BIGINT PRIMARY KEY,
    hk_group_id BIGINT NOT NULL CONSTRAINT fk_l_groups_dialogs_group REFERENCES STV2024050744__DWH.h_groups (hk_group_id),
    hk_message_id BIGINT NOT NULL CONSTRAINT fk_l_groups_dialogs_message REFERENCES STV2024050744__DWH.h_dialogs (hk_message_id),
    load_dt DATETIME,
    load_src VARCHAR(20)
)
ORDER BY 
  load_dt
SEGMENTED BY 
  hk_l_groups_dialogs ALL NODES
PARTITION BY 
  load_dt::date
GROUP BY 
  calendar_hierarchy_day(load_dt::date, 3, 2);


CREATE TABLE STV2024050744__DWH.l_user_message
(
  hk_l_user_message BIGINT PRIMARY KEY,
  hk_user_id BIGINT NOT NULL CONSTRAINT fk_l_user_message_user REFERENCES STV2024050744__DWH.h_users (hk_user_id),
  hk_message_id BIGINT NOT NULL CONSTRAINT fk_l_user_message_message REFERENCES STV2024050744__DWH.h_dialogs (hk_message_id),
  load_dt DATETIME,
  load_src VARCHAR(20)
)
ORDER BY 
  load_dt
SEGMENTED BY 
  hk_user_id ALL NODES
PARTITION BY 
  load_dt::date
GROUP BY 
  calendar_hierarchy_day(load_dt::date, 3, 2);


DROP TABLE IF EXISTS STV2024050744__DWH.h_dialogs;

CREATE TABLE STV2024050744__DWH.h_dialogs
(
    hk_message_id INT PRIMARY KEY NOT NULL,
    message_id INT NULL,
    message_ts TIMESTAMP NULL,
    load_dt TIMESTAMP NULL,
    load_src VARCHAR(20) NULL
)
ORDER BY load_dt
SEGMENTED BY hk_message_id ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);



INSERT INTO STV2024050744__DWH.h_dialogs (hk_message_id, message_id, message_ts, load_dt, load_src)
SELECT hk_message_id, message_id, message_ts, load_dt, load_src
FROM STV2024050744__STAGING.dialogs
WHERE message_id IS NOT NULL AND message_id <> 0;










INSERT INTO STV2024050744__DWH.h_users(hk_user_id, user_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_user_id,
       id as user_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV2024050744__STAGING.users
where hash(id) not in (select hk_user_id from STV2024050744__DWH.h_users);
INSERT INTO STV2024050744__DWH.h_groups(hk_group_id, group_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_group_id,
       id as group_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV2024050744__STAGING.groups
where hash(id) not in (select hk_group_id from STV2024050744__DWH.h_groups);

INSERT INTO STV2024050744__DWH.h_dialogs(hk_message_id, message_id,message_ts,load_dt,load_src)
select
       hash(message_id) as  hk_message_id,
       message_id,
       message_ts,
       now() as load_dt,
       's3' as load_src
       from STV2024050744__STAGING.dialogs
where hash(message_id) not in (select hk_message_id from STV2024050744__DWH.h_dialogs) and message_id IS NOT NULL AND message_id <> 0;






(SELECT 
    MAX(u.registration_dt) < NOW() AS 'no future dates',
    MIN(u.registration_dt) >= '2020-09-03' AS 'no false-start dates',
    'users' AS dataset
FROM STV2024050744__STAGING.users u)
UNION ALL
(SELECT
    MAX(g.registration_dt) < NOW(),
    MIN(g.registration_dt) >= '2020-09-03',
    'groups'
FROM STV2024050744__STAGING.groups g)
UNION ALL
(SELECT
    MAX(d.message_ts) < NOW(),
    MIN(d.message_ts) >= '2020-09-03',
    'dialogs'
FROM STV2024050744__STAGING.dialogs d);


SELECT COUNT(g.id) AS missing_admins_count
FROM STV2024050744__STAGING.groups AS g 
LEFT JOIN STV2024050744__STAGING.users AS u 
ON g.admin_id = u.id 
WHERE u.id IS NULL;

(SELECT COUNT(1) AS missing_info_count, 'missing group admin info' AS info
FROM STV2024050744__STAGING.groups g 
WHERE g.admin_id IS NULL)
UNION ALL
(SELECT COUNT(1) AS missing_info_count, 'missing sender info' AS info
FROM STV2024050744__STAGING.dialogs d 
WHERE d.message_from NOT IN (SELECT id FROM STV2024050744__STAGING.users))
UNION ALL
(SELECT COUNT(1) AS missing_info_count, 'missing receiver info' AS info
FROM STV2024050744__STAGING.dialogs d 
WHERE d.message_to NOT IN (SELECT id FROM STV2024050744__STAGING.users))
UNION ALL 
(SELECT COUNT(1) AS missing_info_count, 'norm receiver info' AS info
FROM STV2024050744__STAGING.dialogs d 
WHERE d.message_to IN (SELECT id FROM STV2024050744__STAGING.users));


DROP TABLE IF EXISTS STV2024050744__DWH.h_dialogs;

CREATE TABLE STV2024050744__DWH.h_dialogs
(
    hk_message_id INT PRIMARY KEY NOT NULL,
    message_id INT NULL,
    message_ts TIMESTAMP NULL,
    load_dt TIMESTAMP NULL,
    load_src VARCHAR(20) NULL
)
ORDER BY load_dt
SEGMENTED BY hk_message_id ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);



DROP TABLE IF EXISTS STV2024050744__DWH.h_groups;

DROP TABLE IF EXISTS STV2024050744__DWH.h_groups;

CREATE TABLE STV2024050744__DWH.h_groups
(
    hk_group_id INT PRIMARY KEY NOT NULL,
    group_id INT NULL,
    registration_dt TIMESTAMP NULL,
    load_dt TIMESTAMP NULL,
    load_src VARCHAR(20) NULL
)
ORDER BY load_dt
SEGMENTED BY hk_group_id ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);





drop table if exists STV2024050744__DWH.h_users;


create table STV2024050744__DWH.h_users
(
    hk_user_id bigint primary key,
    user_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
;


SELECT COUNT(*), COUNT(DISTINCT id) FROM STV2024050744__STAGING.users;

SELECT COUNT(*), COUNT(DISTINCT id) FROM STV2024050744__STAGING.groups;

SELECT COUNT(*), COUNT(DISTINCT message_id) FROM STV2024050744__STAGING.dialogs;

copy STV2024050744__STAGING.dialogs (
    message_id,message_ts,message_from,message_to,message,message_group) 
from 
    local 'C:\\Users\\user\\s6-lessons\\dialogs.csv'
delimiter ',';


-- Удаление старой таблицы dialogs_temp, если она существует
DROP TABLE IF EXISTS dialogs_temp;

-- Создание клона таблицы
CREATE TABLE stv2024050744.dialogs_temp
LIKE dialogs
INCLUDING PROJECTIONS;

-- Копирование данных с преобразованием message_type
INSERT INTO stv2024050744.dialogs_temp
SELECT 
    message_id,
    message_ts,
    message_from,
    message_to,
    message,
    COALESCE(message_type, '0') AS message_type
FROM
    dialogs
WHERE
    datediff('month', message_ts, now()) < 4;

-- Проверка на наличие NULL в клонированной таблице
SELECT
    THROW_ERROR('Остались NULL в клоне!') AS test_nulls
FROM
    stv2024050744.dialogs_temp
WHERE
    message_type IS NULL;

-- Определение диапазона дат для swap_partitions_between_tables
-- Пример диапазона: с 2024-01-01 по 2024-04-30 (это нужно будет уточнить по вашим данным)
SELECT swap_partitions_between_tables(
    'dialogs_temp',  /* клон */
    '2024-01-01',    /* начальная партиция диапазона */
    '2024-04-30',    /* конечная партиция диапазона */
    'dialogs'        /* оригинал */
);

-- Проверка на наличие NULL в оригинальной таблице за последние 4 месяца
SELECT
    THROW_ERROR('Остались NULL в основной таблице!') AS test_nulls
FROM
    stv2024050744.dialogs
WHERE
    message_type IS NULL
    AND datediff('month', message_ts, now()) < 4;




COPY stv2024050744.dialogs (
    message_id,
    /* FILLER - означает, что в этот атрибут вектора загрузки
    мы читаем значение из файла. Но на выход (в таблицу) оно
    уже не поступит. Зато можно использовать message_ts_orig
    для трансформации прямо в процессе загрузки */
    message_ts_orig FILLER timestamp(6), 
    message_ts as 
        /* Поскольку данные статичны, а время идёт вперёд, 
        для демонстрации мы сдвинем 2 последних года поближе
        к реальному времени */
        add_months(
            message_ts_orig, 
            case 
                when year(message_ts_orig) >= 2020 
                then datediff('month', '2021-06-21'::timestamp, now()::timestamp) - 1
                else 0
            end
        ),
    message_from,
    message_to,
    message,
    message_type
)
/* 
    NOTE: здесь надо указать путь к файлу на вашем компьютере.
    Для UNC путей (Windows), на забываем экранировать '\' 
    с помощью двойного указания. Например 'C:\\Files\\dialogs.csv'
*/
FROM LOCAL 'C:\\Users\\user\\s6-lessons\\dialogs.csv' 
DELIMITER ','
ENCLOSED BY '"'; 


DROP TABLE IF EXISTS stv2024050744.dialogs;


CREATE TABLE STV2024050744__DWH.h_dialogs
(
    message_id   INT PRIMARY KEY,
    message_ts   TIMESTAMP(6) NOT NULL,
    message_from INT REFERENCES members(id) NOT NULL,
    message_to   INT REFERENCES members(id) NOT NULL,
    message      VARCHAR(1000),
    message_type VARCHAR(100)
)
ORDER BY message_from, message_ts
SEGMENTED BY hash(message_id) ALL NODES
PARTITION BY message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);




MERGE INTO members
USING members_inc
ON members.id = members_inc.id
WHEN MATCHED AND (
    members.age <> members_inc.age OR
    members.gender <> members_inc.gender OR
    members.email <> members_inc.email
)
THEN UPDATE SET 
    age = members_inc.age,
    gender = members_inc.gender,
    email = members_inc.email
WHEN NOT MATCHED
THEN INSERT (
    id,
    age, 
    gender, 
    email
)
VALUES (
    members_inc.id,
    members_inc.age,
    members_inc.gender,
    members_inc.email
);






SELECT * FROM stv2024050744.members WHERE ID in (1,4,28, 600000, 600001);




CREATE TABLE STV2024050744.members_inc LIKE STV2024050744.members INCLUDING PROJECTIONS;
/* 
INCLUDING PROJECTIONS - создаст полную копию структур для всех проекций
родительской таблицы
*/

COPY members(id, age, gender, email ENFORCELENGTH )
FROM LOCAL 'C:\\Users\\user\\s6-lessons\\members.csv'
DELIMITER ','
REJECTED DATA AS TABLE members_rej;

DELETE FROM STV2024050744__STAGING.users;
DELETE FROM STV2024050744__STAGING.groups;
DELETE FROM STV2024050744__STAGING.dialogs;

DELETE FROM STV2024050744__DWH.h_users;
DELETE FROM STV2024050744__DWH.h_groups;
DELETE FROM STV2024050744__DWH.h_dialogs;


COPY STV2024050744.members(id, age, gender, email ENFORCELENGTH )
FROM LOCAL 'C:\\Users\\user\\s6-lessons\\members.csv'
DELIMITER ','
REJECTED DATA AS TABLE members_rej;


CREATE TABLE STV2024050744.members_inc LIKE STV2024050744.members INCLUDING PROJECTIONS;
/* 
INCLUDING PROJECTIONS - создаст полную копию структур для всех проекций
родительской таблицы
*/

create table STV2024050744.members_inc
(
        id int NOT NULL,
        age int,
        gender char,
        email varchar(50),
        CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED
)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES
;


DROP TABLE IF EXISTS STV2024050744.members;

CREATE TABLE STV2024050744.members
(
    id INT PRIMARY KEY,
    age INT,
    gender VARCHAR(8),
    email VARCHAR(256)
)
ORDER BY id
SEGMENTED BY hash(id) ALL NODES;

drop table if exists dialogs;

create table STV2024050744.dialogs
(
    message_id   int PRIMARY KEY,
    message_ts   timestamp(6) NOT NULL,
    message_from int REFERENCES members(id) NOT NULL,
    message_to   int REFERENCES members(id) NOT NULL,
    message      varchar(1000),
    message_type varchar(100)
)
order by message_from, message_ts
SEGMENTED BY hash(message_id) all nodes
PARTITION BY message_ts::DATE
;


COPY STV2024050744.dialogs (
        message_id, message_ts, message_from, message_to, message, message_group
) 
FROM 
        LOCAL 'C:\\Users\\user\\s6-lessons\\dialogs_part.csv'
DELIMITER ',';






SELECT COUNT(*) FROM STV2024050744.members;
SELECT COUNT(*) FROM STV2024050744.dialogs;

SELECT
    U.gender,
    U.age,
    COUNT(M.message_id) AS sent
FROM
    STV2024050744.members AS U
    INNER JOIN STV2024050744.dialogs AS M
    ON U.id = M.message_from
WHERE
    M.message_ts >= TRUNC(NOW(), 'DD') - INTERVAL '3 YEAR'
GROUP BY
    U.gender, U.age;


COPY STV2024050744.dialogs (
        message_id, message_ts, message_from, message_to, message, message_group
) 
FROM 
        LOCAL 'C:\\Users\\user\\s6-lessons\\dialogs_part.csv'
DELIMITER ',';







DROP TABLE IF EXISTS STV2024050744.STV2024050744_TABLE CASCADE

CREATE TABLE STV2024050744.STV2024050744_TABLE
(
    i INT,
    ts TIMESTAMP(6),
    v VARCHAR(255),
    PRIMARY KEY (i, ts)
)
ORDER BY i, ts, v
SEGMENTED BY HASH (i, ts) ALL NODES;

CREATE TABLE STV2024050744.STV2024050744_TABLE
(
    i INT,
    ts TIMESTAMP(6),
    v VARCHAR(255),
    PRIMARY KEY (i, ts)
)
ORDER BY i, ts, v
SEGMENTED BY HASH (i, ts) ALL NODES;


select export_objects('','COPY_EX1')






CREATE TABLE STV2024050744.COPY_EX1
(
    i int NOT NULL,
    v varchar(16),
    CONSTRAINT C_PRIMARY PRIMARY KEY (i) DISABLED
);

CREATE PROJECTION STV2024050744.COPY_EX1 /*+createtype(L)*/ 
(
 i,
 v
)
AS
 SELECT COPY_EX1.i,
        COPY_EX1.v
 FROM STV2024050744.COPY_EX1
 ORDER BY COPY_EX1.i
SEGMENTED BY hash(COPY_EX1.i) ALL NODES KSAFE 1;

SELECT MARK_DESIGN_KSAFE(1); 

select export_objects('','COPY_EX1')


CREATE TABLE STV2024050744.COPY_EX1 (
    i int primary key,
    v varchar(16)
);

/* 
Вставка хотя бы одной записи обязательна для нашего примера
*/
INSERT INTO STV2024050744.COPY_EX1 VALUES (1, 'wow'); 


-- Создаем первую таблицу orders
CREATE TABLE STV2024050744.orders
(
    id INT PRIMARY KEY,
    registration_ts TIMESTAMP(6),
    user_id INT,
    is_confirmed BOOLEAN
)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES;

-- Создаем вторую таблицу orders_v2 с внешним ключом, ссылающимся на id из orders
CREATE TABLE STV2024050744.orders_v2
(
    id INT PRIMARY KEY,
    registration_ts TIMESTAMP WITH TIME ZONE,
    user_id INT REFERENCES STV2024050744.orders(id),
    is_confirmed BOOLEAN
)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES;




CREATE TABLE STV2024050744.BAD_IDEA (
    i int primary key,
    v varchar(16)
);

ALTER TABLE STV2024050744.BAD_IDEA SET MERGEOUT 0;


-- Создание таблицы members
CREATE TABLE STV2024050744.members (
    id INT NOT NULL,
    age INT,
    gender CHAR,
    email VARCHAR(50),
    CONSTRAINT C_PRIMARY PRIMARY KEY (id)
);

-- Загрузка данных из файла members.csv
COPY STV2024050744.members_rej (id, age, gender, email)
FROM LOCAL 'C:\Users\user\s6-lessons\members.csv'
DELIMITER ';';

drop table members_rej




