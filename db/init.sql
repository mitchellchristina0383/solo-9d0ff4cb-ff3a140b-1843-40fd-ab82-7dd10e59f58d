CREATE TABLE IF NOT EXISTS items (
    id    serial PRIMARY KEY,
    name  text NOT NULL,
    price numeric(10, 2) NOT NULL
);

INSERT INTO items (name, price)
SELECT 'item-' || g, round((random() * 100)::numeric, 2)
FROM generate_series(1, 20) AS g
WHERE NOT EXISTS (SELECT 1 FROM items);
