WITH user_group_log AS (
    SELECT 
        hg.hk_group_id,
        COUNT(DISTINCT sa.user_id_from) AS cnt_added_users
    FROM STV2024050744__DWH.s_auth_history AS sa
    LEFT JOIN STV2024050744__DWH.h_groups AS hg ON sa.hk_l_user_group_activity = hg.hk_group_id
    WHERE sa.event = 'add'
    GROUP BY hg.hk_group_id
    ORDER BY MIN(hg.registration_dt)
    LIMIT 10
),
user_group_messages AS (
    SELECT 
        hg.hk_group_id,
        COUNT(DISTINCT sa.hk_user_id) AS cnt_users_in_group_with_messages
    FROM STV2024050744__DWH.l_user_group_activity AS sa
    LEFT JOIN STV2024050744__DWH.h_groups AS hg ON sa.hk_l_user_group_activity = hg.hk_group_id
    GROUP BY hg.hk_group_id
)

SELECT 
    ugl.hk_group_id,
    ugl.cnt_added_users,
    COALESCE(ugm.cnt_users_in_group_with_messages, 0) AS cnt_users_in_group_with_messages,
    (COALESCE(ugm.cnt_users_in_group_with_messages, 0) / NULLIF(ugl.cnt_added_users, 0)) AS group_conversion
FROM user_group_log AS ugl
LEFT JOIN user_group_messages AS ugm ON ugl.hk_group_id = ugm.hk_group_id
ORDER BY group_conversion DESC;