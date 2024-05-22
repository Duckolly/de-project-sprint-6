drop table if exists STV202312114__DWH.l_user_message;
create table STV202312114__DWH.l_user_message
(
hk_l_user_message bigint primary key,
hk_user_id bigint not null CONSTRAINT fk_l_user_message_user REFERENCES STV202312114__DWH.h_users (hk_user_id),
hk_message_id bigint not null CONSTRAINT fk_l_user_message_message REFERENCES STV202312114__DWH.h_dialogs (hk_message_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV202312114__DWH.l_user_message(hk_l_user_message, hk_user_id,hk_message_id,load_dt,load_src)
select
hash(hu.hk_user_id, hg.hk_message_id),
hu.hk_user_id,
hg.hk_message_id,
now() as load_dt,
's3' as load_src
from STV202312114__STAGING.dialogs d
join STV202312114__DWH.h_dialogs as hg on hg.message_id = d.message_id
join STV202312114__DWH.h_users as hu on hu.user_id = d.message_from
where hash(hu.hk_user_id, hg.hk_message_id) not in (select hk_l_user_message from STV202312114__DWH.l_user_message);

drop table if exists STV202312114__DWH.l_groups_dialogs;
create table STV202312114__DWH.l_groups_dialogs
(
hk_l_groups_dialogs bigint primary key,
hk_message_id bigint not null CONSTRAINT fk_l_groups_dialogs_message REFERENCES STV202312114__DWH.h_dialogs (hk_message_id),
hk_group_id bigint not null CONSTRAINT fk_l_groups_dialogs_group REFERENCES STV202312114__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_groups_dialogs all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV202312114__DWH.l_groups_dialogs(hk_l_groups_dialogs, hk_message_id,hk_group_id,load_dt,load_src)
select
hash(hd.hk_message_id, hg.hk_group_id),
hd.hk_message_id,
hg.hk_group_id,
now() as load_dt,
's3' as load_src
from STV202312114__STAGING.dialogs d
join STV202312114__DWH.h_groups as hg on d.message_group = hg.group_id
join STV202312114__DWH.h_dialogs as hd on d.message_id = hd.message_id
where hash(hd.hk_message_id, hg.hk_group_id) not in (select hk_l_groups_dialogs from STV202312114__DWH.l_groups_dialogs);

drop table if exists STV202312114__DWH.l_admins;
create table STV202312114__DWH.l_admins
(
hk_l_admin_id bigint primary key,
hk_group_id bigint not null CONSTRAINT fk_l_admins_groups REFERENCES STV202312114__DWH.h_groups (hk_group_id),
hk_user_id bigint not null CONSTRAINT fk_l_admins_user REFERENCES STV202312114__DWH.h_users (hk_user_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV202312114__DWH.l_admins(hk_l_admin_id, hk_group_id,hk_user_id,load_dt,load_src)
select
hash(hg.hk_group_id,hu.hk_user_id),
hg.hk_group_id,
hu.hk_user_id,
now() as load_dt,
's3' as load_src
from STV202312114__STAGING.groups as g
left join STV202312114__DWH.h_users as hu on g.admin_id = hu.user_id
left join STV202312114__DWH.h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_admin_id from STV202312114__DWH.l_admins);

drop table if exists STV202312114__DWH.l_user_group_activity;
create table STV202312114__DWH.l_user_group_activity
(
hk_l_user_group_activity bigint primary key,
hk_user_id bigint not null CONSTRAINT fk_l_user_group_activity_user REFERENCES STV202312114__DWH.h_users (hk_user_id),
hk_group_id bigint not null CONSTRAINT fk_l_user_group_activity_groups REFERENCES STV202312114__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV202312114__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
select distinct
hash(hu.hk_user_id, hg.hk_group_id),
hk_user_id,
hk_group_id,
now() as load_dt,
's3' as load_src
from STV202312114__STAGING.group_log as gl
left join STV202312114__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV202312114__DWH.h_groups as hg on gl.group_id = hg.group_id
where hash(hu.hk_user_id, hg.hk_group_id) not in (select hk_l_user_group_activity from STV202312114__DWH.l_user_group_activity);