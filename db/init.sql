CREATE TABLE IF NOT EXISTS items (
    id    serial PRIMARY KEY,
    name  text NOT NULL,
    price numeric(10, 2) NOT NULL
);

INSERT INTO items (name, price)
SELECT 'item-' || g, round((random() * 100)::numeric, 2)
FROM generate_series(1, 20) AS g
WHERE NOT EXISTS (SELECT 1 FROM items);

-- 巡检用：pg_stat_statements 已通过 shared_preload_libraries 预加载，
-- 这里在 demo 库内创建扩展。PG17 的视图包含每条目独立的 stats_since
-- （起算时间），配合 queryid 可识别单条重置与淘汰后重现。
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
