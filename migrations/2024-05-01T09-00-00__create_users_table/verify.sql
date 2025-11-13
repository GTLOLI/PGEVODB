-- 校验 app_users 表是否存在并包含 email 唯一约束
SELECT
    to_regclass('public.app_users') IS NOT NULL AS has_table,
    (SELECT COUNT(*) FROM pg_constraint WHERE conname = 'app_users_email_key') > 0 AS has_unique_constraint;
