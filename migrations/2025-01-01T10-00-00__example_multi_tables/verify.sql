-- 验证表是否创建成功
SELECT 'products' AS table_name, COUNT(*) AS exists FROM information_schema.tables WHERE table_name = 'products'
UNION ALL
SELECT 'orders', COUNT(*) FROM information_schema.tables WHERE table_name = 'orders'
UNION ALL
SELECT 'order_items', COUNT(*) FROM information_schema.tables WHERE table_name = 'order_items';
