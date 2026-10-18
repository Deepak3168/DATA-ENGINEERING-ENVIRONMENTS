#!/bin/bash
set -e

# Create databases if they don't already exist
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE airflow;
    CREATE DATABASE oltp_db;
    CREATE DATABASE warehouse_db;
    
    GRANT ALL PRIVILEGES ON DATABASE airflow TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON DATABASE oltp_db TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON DATABASE warehouse_db TO $POSTGRES_USER;
EOSQL

# Seed OLTP schema in oltp_db
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "oltp_db" <<-EOSQL
    CREATE TABLE IF NOT EXISTS users (
        user_id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(150) UNIQUE NOT NULL,
        country VARCHAR(50) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS products (
        product_id SERIAL PRIMARY KEY,
        title VARCHAR(150) NOT NULL,
        category VARCHAR(50) NOT NULL,
        price NUMERIC(10, 2) NOT NULL,
        stock_qty INT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS orders (
        order_id SERIAL PRIMARY KEY,
        user_id INT REFERENCES users(user_id),
        total_amount NUMERIC(10, 2) NOT NULL,
        order_status VARCHAR(30) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS order_items (
        item_id SERIAL PRIMARY KEY,
        order_id INT REFERENCES orders(order_id),
        product_id INT REFERENCES products(product_id),
        quantity INT NOT NULL,
        unit_price NUMERIC(10, 2) NOT NULL
    );

    -- Seed sample initial records
    INSERT INTO users (name, email, country, created_at) VALUES
    ('Alice Smith', 'alice@example.com', 'US', NOW() - INTERVAL '10 days'),
    ('Bob Jones', 'bob@example.com', 'UK', NOW() - INTERVAL '8 days'),
    ('Charlie Brown', 'charlie@example.com', 'DE', NOW() - INTERVAL '5 days'),
    ('Diana Prince', 'diana@example.com', 'FR', NOW() - INTERVAL '3 days'),
    ('Evan Wright', 'evan@example.com', 'US', NOW() - INTERVAL '1 days')
    ON CONFLICT DO NOTHING;

    INSERT INTO products (title, category, price, stock_qty) VALUES
    ('Mechanical Keyboard', 'Electronics', 129.99, 50),
    ('Wireless Mouse', 'Electronics', 49.99, 120),
    ('Noise-Canceling Headphones', 'Audio', 199.99, 30),
    ('Ergonomic Office Chair', 'Furniture', 299.50, 15),
    ('USB-C 4K Monitor', 'Electronics', 399.00, 25),
    ('Standing Desk Mat', 'Accessories', 39.99, 80)
    ON CONFLICT DO NOTHING;

    INSERT INTO orders (user_id, total_amount, order_status, created_at) VALUES
    (1, 179.98, 'COMPLETED', NOW() - INTERVAL '5 days'),
    (2, 299.50, 'COMPLETED', NOW() - INTERVAL '4 days'),
    (3, 448.99, 'SHIPPED', NOW() - INTERVAL '2 days'),
    (4, 129.99, 'PROCESSING', NOW() - INTERVAL '1 days'),
    (1, 399.00, 'COMPLETED', NOW() - INTERVAL '6 hours')
    ON CONFLICT DO NOTHING;

    INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 129.99),
    (1, 2, 1, 49.99),
    (2, 4, 1, 299.50),
    (3, 1, 1, 129.99),
    (3, 5, 1, 319.00),
    (4, 1, 1, 129.99),
    (5, 5, 1, 399.00)
    ON CONFLICT DO NOTHING;
EOSQL

# Seed analytical Warehouse schema in warehouse_db (Dimensional Star Schema)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "warehouse_db" <<-EOSQL
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id INT PRIMARY KEY,
        full_date DATE NOT NULL,
        day_of_week VARCHAR(15) NOT NULL,
        month_name VARCHAR(15) NOT NULL,
        quarter INT NOT NULL,
        year INT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dim_user (
        user_key SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(150) NOT NULL,
        country VARCHAR(50) NOT NULL,
        is_active BOOLEAN DEFAULT TRUE,
        valid_from TIMESTAMP NOT NULL,
        valid_to TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS dim_product (
        product_key SERIAL PRIMARY KEY,
        product_id INT NOT NULL,
        title VARCHAR(150) NOT NULL,
        category VARCHAR(50) NOT NULL,
        price NUMERIC(10, 2) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS fact_orders (
        fact_order_id SERIAL PRIMARY KEY,
        order_id INT NOT NULL,
        user_key INT REFERENCES dim_user(user_key),
        date_id INT REFERENCES dim_date(date_id),
        order_status VARCHAR(30) NOT NULL,
        total_amount NUMERIC(10, 2) NOT NULL,
        item_count INT NOT NULL,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS agg_daily_sales (
        date_id INT PRIMARY KEY,
        total_revenue NUMERIC(14, 2) NOT NULL,
        total_orders INT NOT NULL,
        avg_order_value NUMERIC(10, 2) NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
EOSQL

echo "Databases 'airflow', 'oltp_db', and 'warehouse_db' initialized successfully."
