-- 示例：使用 @include 指令引用其他 SQL 文件
-- 这样可以将大型迁移拆分成多个文件，便于管理和维护

-- 1. 首先创建产品表
-- @include sql/01_create_products.sql

-- 2. 然后创建订单相关表（依赖产品表）
-- @include sql/02_create_orders.sql

-- 3. 可以在这里添加其他初始化语句
COMMENT ON TABLE products IS '产品信息表';
COMMENT ON TABLE orders IS '订单表';
COMMENT ON TABLE order_items IS '订单明细表';
