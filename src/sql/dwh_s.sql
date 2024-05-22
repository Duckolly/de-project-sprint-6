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
select luga.hk_l_user_group_activity,
gl.user_id_from,
gl.event,
gl.datetime as event_dt,
now() as load_dt,
's3' as load_src
from STV2024050744__STAGING.group_log as gl
left join STV2024050744__DWH.h_groups as hg on gl.group_id = hg.group_id
left join STV2024050744__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV2024050744__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id