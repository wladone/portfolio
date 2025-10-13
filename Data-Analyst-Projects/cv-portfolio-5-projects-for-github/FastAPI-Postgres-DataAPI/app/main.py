from fastapi import FastAPI
from pydantic import BaseModel
import psycopg2

app = FastAPI()

class Item(BaseModel):
    name: str
    value: float

@app.get("/")
def read_root():
    return {"message": "API is running"}

@app.post("/items/")
def create_item(item: Item):
    conn = psycopg2.connect(
        host="postgres",
        database="dataapi",
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()
    cur.execute("INSERT INTO items (name, value) VALUES (%s, %s)", (item.name, item.value))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "Item created"}

@app.get("/items/")
def list_items():
    conn = psycopg2.connect(
        host="postgres",
        database="dataapi",
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()
    cur.execute("SELECT id, name, value FROM items ORDER BY id DESC")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"items": [{"id": r[0], "name": r[1], "value": float(r[2])} for r in rows]}
