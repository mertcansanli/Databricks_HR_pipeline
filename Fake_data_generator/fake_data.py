import pandas as pd
import random
from faker import Faker
from datetime import timedelta

fake = Faker()

segments = ["Home", "Corporate", "Consumer"]
categories = {
    "Furniture": ["Sofas", "Chairs", "Tables", "Beds"],
    "Technology": ["Phones", "Laptops", "Accessories"],
    "Office Supplies": ["Paper", "Binders", "Storage"]
}

countries = ["Australia", "USA", "Germany", "Canada", "UK"]

def generate_order_id():
    return f"ORD-{random.randint(1000000, 9999999)}"

def generate_customer_id():
    return f"CUST-{random.randint(1000, 9999)}"

def generate_data(n=10000):
    data = []
    
    for _ in range(n):
        order_date = fake.date_time_between(start_date='-1y', end_date='now')
        ship_date = order_date + timedelta(days=random.randint(1, 10))
        
        category = random.choice(list(categories.keys()))
        subcategory = random.choice(categories[category])
        
        returns = random.choice([0, 1])
        status = random.choice(["Complete", "Pending", "Cancelled"])
        repeat_customer = random.choice([0, 1])
        on_time = 1 if (ship_date - order_date).days <= 7 else 0
        
        data.append({
            "Order ID": generate_order_id(),
            "Order Date": order_date.strftime("%m/%d/%y %H:%M"),
            "Ship Date": ship_date.strftime("%m/%d/%y %H:%M"),
            "Segment": random.choice(segments),
            "Category": category,
            "Subcategory": subcategory,
            "Sales": round(random.uniform(20, 2000), 4),
            "Customer ID": generate_customer_id(),
            "Customer Name": fake.name(),
            "Country": random.choice(countries),
            "Returns": returns,
            "Status": status,
            "Repeat Customer": repeat_customer,
            "On Time": on_time
        })
    
    return pd.DataFrame(data)

# Veri üret
df = generate_data(3000)

# CSV olarak kaydet
df.to_csv("fake_orders.csv", index=False)

print(df.head())