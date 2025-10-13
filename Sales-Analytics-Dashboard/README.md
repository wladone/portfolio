# 📊 Sales Analytics Dashboard – Power BI & SQL
**Author:** Vlad Gabriel Popescu  
**Role:** Data Analyst / BI Developer  

![Status](https://img.shields.io/badge/status-active-brightgreen)
![Power BI](https://img.shields.io/badge/Power%20BI-ready-yellow)
![SQL](https://img.shields.io/badge/SQL-PostgreSQL-blue)

## 📌 Description
This project demonstrates how to connect a SQL database to Power BI and build an interactive sales analytics dashboard with KPIs, filters, and regional performance insights.

---

## 🛠 Technologies Used
- **PostgreSQL**
- **Power BI Desktop**
- **DAX** for calculated measures

---

## 📂 Project Structure
```
Sales-Analytics-Dashboard/
│── README.md
│── sql/
│   ├── create_tables.sql
│── dashboard/
│   ├── README_PLACEHOLDER.txt
│── docker-compose.yml
│── screenshots/
│   ├── placeholder.png
```

---

## 📜 SQL Script Explanation (`sql/create_tables.sql`)

```sql
CREATE TABLE IF NOT EXISTS sales (
    sale_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    region VARCHAR(50),
    sale_date DATE,
    amount DECIMAL(10,2)
);
```
- **CREATE TABLE** – defines table `sales` to feed the dashboard
- **Columns** – product, region, date, amount

```sql
INSERT INTO sales (sale_id, product_name, region, sale_date, amount) VALUES
(1, 'Laptop', 'North', '2025-01-01', 1200.50),
(2, 'Smartphone', 'East', '2025-01-02', 850.00),
(3, 'Monitor', 'South', '2025-01-03', 250.75);
```
- **INSERT** – seed sample data

---

## 🚀 Run in Docker (PostgreSQL)

```bash
docker compose up -d
# Connect Power BI: PostgreSQL → host localhost, db salesdb, user postgres, pass postgres
# After testing:
docker compose down
```

---

## 📸 Screenshots
_Add images to `screenshots/` and reference here._

---

## 🔮 Possible Improvements
- Automate refresh with Power BI Gateway
- Add profit & category analysis
- Schedule ETL feeding this DB

---

## 📜 License
MIT License (see `LICENSE.md`)
