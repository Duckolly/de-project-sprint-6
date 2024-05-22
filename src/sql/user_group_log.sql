WITH user_group_log AS (
    SELECT 
        hg.hk_group_id,
        COUNT(DISTINCT sa.user_id_from) AS cnt_added_users
    FROM MY__DWH.s_auth_history AS sa
    LEFT JOIN MY__DWH.h_groups AS hg ON sa.hk_l_user_group_activity = hg.hk_group_id
    WHERE sa.event = 'add'
    GROUP BY hg.hk_group_id
    ORDER BY MIN(hg.registration_dt)
    LIMIT 10
)
SELECT hk_group_id,
       cnt_added_users
FROM user_group_log
ORDER BY cnt_added_users DESC
LIMIT 10;
