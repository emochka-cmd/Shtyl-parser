import sqlite3
from typing import List, Dict, Any


class Database:
    def __init__(self, db_name: str = 'shtyl_product.db'):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self._create_table()

    def _create_table(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT,
                name TEXT,
                description TEXT,
                brand TEXT,
                category TEXT,
                price REAL,
                currency TEXT
            )
        """)
        self.conn.commit()

    def insert_product(self, product: Dict[str, Any]):
        """Добавляет один товар в таблицу"""
        self.cursor.execute("""
            INSERT INTO products (product_id, name, description, brand, category, price, currency)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            product.get("id"),
            product.get("name"),
            product.get("description"),
            product.get("brand"),
            product.get("category"),
            float(product["price"]) if product.get("price") else None,
            product.get("currency")
        ))
        self.conn.commit()

    def insert_many(self, products: List[Dict[str, Any]]):
        """Добавляет сразу список товаров"""
        self.cursor.executemany("""
            INSERT INTO products (product_id, name, description, brand, category, price, currency)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            (
                p.get("id"),
                p.get("name"),
                p.get("description"),
                p.get("brand"),
                p.get("category"),
                float(p["price"]) if p.get("price") else None,
                p.get("currency")
            )
            for p in products
        ])
        self.conn.commit()

    def fetch_all(self) -> List[Dict[str, Any]]:
        """Возвращает все товары"""
        self.cursor.execute("SELECT * FROM products")
        rows = self.cursor.fetchall()
        return [
            {
                "id": row[0],
                "product_id": row[1],
                "name": row[2],
                "description": row[3],
                "brand": row[4],
                "category": row[5],
                "price": row[6],
                "currency": row[7]
            }
            for row in rows
        ]

    def clear_table(self):
        """Удаляет все записи"""
        self.cursor.execute("DELETE FROM products")
        self.conn.commit()

    def close(self):
        """Закрывает соединение"""
        self.conn.close()
