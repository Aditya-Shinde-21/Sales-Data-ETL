import csv
import os
import random
from datetime import datetime, timedelta

from scripts.main.utility.my_sql_session import get_mysql_connection

customer_ids = list(range(1, 501))
store_ids = list(range(121, 125))

products = {"Rice": 65.0, "Wheat Flour": 52.0, "Sugar": 50.0, "Salt": 22.0, "Milk": 37.5, "Tea": 180.0, "Coffee": 220.0, "Cooking Oil": 165.0, "Ghee": 620.0, "Butter": 275.0,
    "Pulses": 140.0, "Chole": 120.0, "Rajma": 135.0, "Toor Dal": 155.0, "Moong Dal": 145.0,"Masoor Dal": 130.0, "Eggs": 7.5, "Bread": 45.0, "Cheese": 310.0, "Curd": 65.0,
    "Paneer": 420.0, "Frozen Vegetables": 180.0, "Fresh Vegetables": 60.0, "Fresh Fruits": 90.0, "Potatoes": 30.0, "Onions": 35.0, "Tomatoes": 40.0, "Garlic": 220.0, "Ginger": 180.0, "Apples": 160.0,
    "Bananas": 55.0, "Oranges": 80.0, "Grapes": 120.0, "Watermelon": 45.0, "Biscuits": 35.0, "Chocolates": 110.0, "Chips": 50.0, "Namkeen": 95.0, "Soft Drinks": 40.0, "Packaged Juices": 110.0,
    "Mineral Water": 25.0, "Instant Noodles": 70.0, "Pasta": 85.0, "Sauces": 105.0, "Ketchup": 120.0, "Spices": 95.0, "Pickles": 130.0, "Jaggery": 75.0, "Breakfast Cereals": 210.0, "Corn Flakes": 190.0,
    "Peanut Butter": 240.0, "Jam": 155.0, "Honey": 280.0, "Ice Cream": 260.0, "Chocolate Spread": 250.0, "Baby Food": 360.0, "Diapers": 520.0, "Baby Wipes": 180.0, "Toilet Paper": 95.0, "Tissues": 60.0,
    "Dishwashing Liquid": 110.0, "Floor Cleaner": 140.0, "Laundry Detergent": 180.0, "Fabric Softener": 165.0, "Shampoo": 190.0, "Hair Oil": 155.0, "Toothpaste": 110.0, "Toothbrush": 55.0, "Soap": 40.0, "Body Wash": 160.0,
    "Deodorant": 210.0, "Face Wash": 175.0, "Moisturizer": 230.0, "Sunscreen": 310.0, "Shaving Cream": 95.0, "Razor": 120.0, "First Aid Kit": 180.0, "Pain Relief Tablets": 45.0, "Cold and Flu Tablets": 85.0, "Vitamins": 260.0,
    "Batteries": 95.0, "Light Bulbs": 75.0, "Plastic Containers": 130.0, "Aluminum Foil": 110.0, "Garbage Bags": 90.0, "Air Freshener": 160.0, "Ready to Eat Meals": 140.0, "Frozen Parathas": 120.0
}

sales_persons = {
    121: [1, 2, 3, 4],
    122: [6, 7, 8, 9],
    123: [11, 12, 13, 14],
    124: [16, 17, 18, 19]
}

start_date = datetime(2021, 1, 1)
end_date = datetime(2025, 12, 31)

file_location = "D:\\DE_Project_Files\\sales_data_to_s3\\"
csv_file_path = os.path.join(file_location, "sales_data.csv")

connection = get_mysql_connection()
cursor = connection.cursor()

with open(csv_file_path, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["customer_id", "store_id", "product_id", "sales_date", "sales_person_id", "price", "quantity", "total_cost"])

    for _ in range(100000):
        customer_id = random.randint(1, 500)
        store_id = random.randint(121, 124)

        product_id = random.randint(1, 88)
        statement = f"SELECT current_price FROM product WHERE id = {product_id};"
        cursor.execute(statement)
        data = cursor.fetchall()
        price = data[0][0]
        #product_name = data[0][0]
        sales_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        sales_person_id = random.choice(sales_persons[store_id])
        quantity = random.randint(1, 10)
        #price = products[product_name]
        total_cost = price * quantity

        csvwriter.writerow([customer_id, store_id, product_id, sales_date.strftime("%Y-%m-%d"), sales_person_id, price, quantity, total_cost])

cursor.close()
connection.close()
print("CSV file generated successfully.")