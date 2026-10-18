#!/usr/bin/env python3
"""
generate_oltp_data.py
Simulates a live backend e-commerce application database.
Generates users, products, orders, and order items into the OLTP Postgres database.
"""

import os
import random
import psycopg2
from datetime import datetime, timedelta

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_NAME = "oltp_db"

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_NAME
    )

FIRST_NAMES = ["Alex", "Jordan", "Taylor", "Morgan", "Sam", "Chris", "Pat", "Riley", "Cameron", "Casey"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
COUNTRIES = ["US", "CA", "UK", "DE", "FR", "JP", "AU", "NL"]
STATUSES = ["COMPLETED", "COMPLETED", "COMPLETED", "SHIPPED", "PROCESSING", "CANCELLED"]

def seed_more_data(batch_size=20):
    conn = get_connection()
    cur = conn.cursor()
    print(f"[*] Connecting to {DB_NAME} at {DB_HOST}:{DB_PORT}...")

    # Insert new users
    new_users = []
    for _ in range(batch_size // 2):
        name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        email = f"{name.lower().replace(' ', '.')}_{random.randint(100, 9999)}@mockmail.io"
        country = random.choice(COUNTRIES)
        cur.execute(
            "INSERT INTO users (name, email, country, created_at) VALUES (%s, %s, %s, %s) RETURNING user_id;",
            (name, email, country, datetime.utcnow() - timedelta(days=random.randint(1, 30)))
        )
        new_users.append(cur.fetchone()[0])

    conn.commit()
    print(f"[+] Created {len(new_users)} new mock users.")

    # Fetch available users & products
    cur.execute("SELECT user_id FROM users;")
    all_user_ids = [row[0] for row in cur.fetchall()]

    cur.execute("SELECT product_id, price FROM products;")
    products = cur.fetchall()

    if not products:
        print("[!] No products found! Run database init first.")
        return

    # Insert new orders
    orders_created = 0
    for _ in range(batch_size):
        user_id = random.choice(all_user_ids)
        order_status = random.choice(STATUSES)
        days_ago = random.randint(0, 7)
        hours_ago = random.randint(0, 23)
        order_time = datetime.utcnow() - timedelta(days=days_ago, hours=hours_ago)

        # Pick 1 to 4 items
        num_items = random.randint(1, 4)
        selected_items = random.sample(products, k=min(num_items, len(products)))

        total_amount = 0
        items_payload = []
        for prod_id, price in selected_items:
            qty = random.randint(1, 3)
            subtotal = float(price) * qty
            total_amount += subtotal
            items_payload.append((prod_id, qty, price))

        cur.execute(
            """INSERT INTO orders (user_id, total_amount, order_status, created_at, updated_at)
               VALUES (%s, %s, %s, %s, %s) RETURNING order_id;""",
            (user_id, round(total_amount, 2), order_status, order_time, order_time)
        )
        order_id = cur.fetchone()[0]

        for prod_id, qty, unit_price in items_payload:
            cur.execute(
                """INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                   VALUES (%s, %s, %s, %s);""",
                (order_id, prod_id, qty, unit_price)
            )
        orders_created += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"[+] Successfully generated {orders_created} mock orders with items.")

if __name__ == "__main__":
    seed_more_data(batch_size=25)
