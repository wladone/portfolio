import argparse
import csv
import json
import gzip
import os
import random
from datetime import datetime

# Constants for realistic data
CATEGORIES = [
    'T-Shirts', 'Jeans', 'Dresses', 'Shoes', 'Jackets', 'Sweaters', 'Skirts', 'Shorts',
    'Hats', 'Accessories', 'Pants', 'Blouses', 'Coats', 'Socks', 'Underwear'
]

BRANDS = [
    'Nike', 'Adidas', 'Zara', 'H&M', 'Gucci', 'Levi\'s', 'Puma', 'Uniqlo',
    'Forever 21', 'Ralph Lauren', 'Gap', 'Old Navy', 'Banana Republic', 'Tommy Hilfiger'
]

REGIONS = [
    'New York', 'California', 'Texas', 'Florida', 'Illinois', 'Pennsylvania',
    'Ohio', 'Georgia', 'North Carolina', 'Michigan', 'New Jersey', 'Virginia',
    'Washington', 'Arizona', 'Massachusetts'
]

PRODUCT_ADJECTIVES = [
    'Classic', 'Vintage', 'Modern', 'Casual', 'Elegant', 'Sporty', 'Comfortable',
    'Stylish', 'Trendy', 'Premium', 'Basic', 'Designer'
]

PRODUCT_TYPES = [
    'Tee', 'Jeans', 'Dress', 'Sneakers', 'Jacket', 'Sweater', 'Skirt', 'Shorts',
    'Cap', 'Necklace', 'Pants', 'Blouse', 'Coat', 'Socks', 'Bra'
]


def generate_product_name():
    adj = random.choice(PRODUCT_ADJECTIVES)
    typ = random.choice(PRODUCT_TYPES)
    color = random.choice(
        ['White', 'Black', 'Blue', 'Red', 'Green', 'Gray', 'Navy', 'Pink'])
    return f'{adj} {color} {typ}'


def generate_products(num=5000):
    products = []
    for i in range(1, num + 1):
        name = generate_product_name()
        category = random.choice(CATEGORIES)
        subcategory = random.choice(['Men', 'Women', 'Kids', 'Unisex'])
        brand = random.choice(BRANDS)
        price = round(random.uniform(15, 800), 2)
        # Cost is 40-80% of price
        cost = round(price * random.uniform(0.4, 0.8), 2)
        supplier_id = random.randint(1, 100)
        region = random.choice(REGIONS)
        created_date = datetime.now().strftime('%Y-%m-%d')
        updated_date = datetime.now().strftime('%Y-%m-%d')

        product = {
            'product_id': i,
            'product_name': name,
            'category': category,
            'subcategory': subcategory,
            'brand': brand,
            'price': price,
            'cost': cost,
            'supplier_id': supplier_id,
            'region': region,
            'created_date': created_date,
            'updated_date': updated_date
        }
        products.append(product)
    return products


def generate_transactions(products, date_str, num=50000):
    transactions = []
    product_ids = [p['product_id'] for p in products]
    for i in range(1, num + 1):
        product_id = random.choice(product_ids)
        product = next(p for p in products if p['product_id'] == product_id)
        quantity = random.randint(1, 10)
        unit_price = product['price']
        total = round(quantity * unit_price, 2)
        region = random.choice(REGIONS)
        store_id = random.randint(1, 50)
        customer_id = random.randint(100000, 999999)
        payment_method = random.choice(
            ['Credit Card', 'Debit Card', 'Cash', 'PayPal', 'Apple Pay'])

        transaction = {
            'sale_id': f'SALE_{i:08d}',
            'product_id': product_id,
            'sale_date': f'{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}',
            'region': region,
            'store_id': store_id,
            'quantity': quantity,
            'unit_price': unit_price,
            'total_amount': total,
            'customer_id': customer_id,
            'payment_method': payment_method
        }
        transactions.append(transaction)
    return transactions


def write_csv(data, filename, gzip_flag):
    if gzip_flag:
        filename += '.gz'
        with gzip.open(filename, 'wt', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
    else:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)


def write_ndjson(data, filename, gzip_flag):
    if gzip_flag:
        filename += '.gz'
        with gzip.open(filename, 'wt', encoding='utf-8') as f:
            for item in data:
                f.write(json.dumps(item) + '\n')
    else:
        with open(filename, 'w', encoding='utf-8') as f:
            for item in data:
                f.write(json.dumps(item) + '\n')


def main():
    parser = argparse.ArgumentParser(
        description='Generate sample data for fashion retail pipeline')
    parser.add_argument('--date', default=datetime.now().strftime('%Y%m%d'),
                        help='Date in YYYYMMDD format (default: today)')
    parser.add_argument('--gzip', action='store_true',
                        help='Compress output files with gzip')
    parser.add_argument('--output_dir', default='.',
                        help='Output directory (default: current directory)')
    args = parser.parse_args()

    date_str = args.date
    output_dir = args.output_dir
    gzip_flag = args.gzip

    # Create directories
    products_dir = os.path.join(output_dir, 'raw', 'products', date_str)
    transactions_dir = os.path.join(
        output_dir, 'raw', 'transactions', date_str)
    os.makedirs(products_dir, exist_ok=True)
    os.makedirs(transactions_dir, exist_ok=True)

    print(f"Generating sample data for date: {date_str}")
    print(f"Output directory: {output_dir}")
    print(f"Gzip compression: {gzip_flag}")

    # Generate data
    print("Generating 5000 products...")
    products = generate_products(5000)

    print("Generating 50000 transactions...")
    transactions = generate_transactions(products, date_str, 50000)

    # Write products
    products_filename = os.path.join(products_dir, f'products_{date_str}.csv')
    print(
        f"Writing products to {products_filename}{'(.gz)' if gzip_flag else ''}")
    write_csv(products, products_filename, gzip_flag)

    # Write transactions
    transactions_filename = os.path.join(
        transactions_dir, f'transactions_{date_str}.json')
    print(
        f"Writing transactions to {transactions_filename}{'(.gz)' if gzip_flag else ''}")
    write_ndjson(transactions, transactions_filename, gzip_flag)

    print("Sample data generation completed successfully!")


if __name__ == '__main__':
    main()
