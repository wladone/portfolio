CREATE TABLE IF NOT EXISTS sales (
    sale_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    region VARCHAR(50),
    sale_date DATE,
    amount DECIMAL(10,2)
);

INSERT INTO sales (sale_id, product_name, region, sale_date, amount) VALUES
(1, 'Laptop', 'North', '2025-01-01', 1200.50),
(2, 'Smartphone', 'East', '2025-01-02', 850.00),
(3, 'Monitor', 'South', '2025-01-03', 250.75),
(4, 'Headphones', 'West', '2025-02-10', 120.00),
(5, 'Tablet', 'North', '2025-02-14', 500.00);
