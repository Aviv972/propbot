-- Import sales data from CSV chunks
BEGIN;

CREATE TEMP TABLE temp_sales (
    url TEXT,
    title TEXT,
    location TEXT,
    price NUMERIC,
    size NUMERIC,
    num_rooms INTEGER,
    price_per_sqm NUMERIC,
    room_type TEXT,
    snapshot_date TEXT,
    details TEXT,
    first_seen_date TEXT
);

\COPY temp_sales(url, title, location, price, size, num_rooms, price_per_sqm, room_type, snapshot_date) FROM '/app/sales_chunks/sales_chunk_0_50.csv' WITH (FORMAT csv, HEADER true);

\COPY temp_sales(url, title, location, price, size, num_rooms, price_per_sqm, room_type, snapshot_date) FROM '/app/sales_chunks/sales_chunk_50_100.csv' WITH (FORMAT csv, HEADER true);

\COPY temp_sales(url, title, location, price, size, num_rooms, price_per_sqm, room_type, snapshot_date) FROM '/app/sales_chunks/sales_chunk_100_150.csv' WITH (FORMAT csv, HEADER true);

\COPY temp_sales(url, title, location, price, size, num_rooms, price_per_sqm, room_type, snapshot_date) FROM '/app/sales_chunks/sales_chunk_150_200.csv' WITH (FORMAT csv, HEADER true);

\COPY temp_sales(url, title, location, price, size, num_rooms, price_per_sqm, room_type, snapshot_date) FROM '/app/sales_chunks/sales_chunk_200_250.csv' WITH (FORMAT csv, HEADER true);

\COPY temp_sales(url, title, location, price, size, num_rooms, price_per_sqm, room_type, snapshot_date) FROM '/app/sales_chunks/sales_chunk_250_300.csv' WITH (FORMAT csv, HEADER true);

\COPY temp_sales(url, title, location, price, size, num_rooms, price_per_sqm, room_type, snapshot_date) FROM '/app/sales_chunks/sales_chunk_300_350.csv' WITH (FORMAT csv, HEADER true);

\COPY temp_sales(url, title, location, price, size, num_rooms, price_per_sqm, room_type, snapshot_date) FROM '/app/sales_chunks/sales_chunk_350_400.csv' WITH (FORMAT csv, HEADER true);

\COPY temp_sales(url, title, location, price, size, num_rooms, price_per_sqm, room_type, snapshot_date) FROM '/app/sales_chunks/sales_chunk_400_420.csv' WITH (FORMAT csv, HEADER true);

INSERT INTO properties_sales (
    url, title, price, size, rooms, price_per_sqm, 
    location, neighborhood, details, snapshot_date, first_seen_date
)
SELECT 
    url, 
    title, 
    price, 
    size, 
    num_rooms, 
    price_per_sqm, 
    location, 
    CASE WHEN location LIKE '%,%' THEN split_part(location, ', ', -1) ELSE NULL END as neighborhood,
    room_type as details, 
    CASE 
        WHEN snapshot_date ~ E'^\\d{4}-\\d{2}-\\d{2}$' THEN snapshot_date::timestamp 
        ELSE NOW() 
    END as snapshot_date,
    CASE 
        WHEN snapshot_date ~ E'^\\d{4}-\\d{2}-\\d{2}$' THEN snapshot_date::timestamp 
        ELSE NOW() 
    END as first_seen_date
FROM temp_sales
WHERE url IS NOT NULL
ON CONFLICT (url) DO UPDATE SET
    title = EXCLUDED.title,
    price = EXCLUDED.price,
    size = EXCLUDED.size,
    rooms = EXCLUDED.rooms,
    price_per_sqm = EXCLUDED.price_per_sqm,
    location = EXCLUDED.location,
    neighborhood = EXCLUDED.neighborhood,
    details = EXCLUDED.details,
    snapshot_date = EXCLUDED.snapshot_date,
    updated_at = NOW();
SELECT COUNT(*) FROM properties_sales;
COMMIT;
