import random
from faker import Faker
from datetime import datetime, timedelta

def get_insert_statements():
    products = {
        "Rice": 65.0, "Wheat Flour": 52.0, "Sugar": 50.0, "Salt": 22.0, "Milk": 37.5, "Tea": 180.0, "Coffee": 220.0,
        "Cooking Oil": 165.0, "Ghee": 620.0, "Butter": 275.0,
        "Pulses": 140.0, "Chole": 120.0, "Rajma": 135.0, "Toor Dal": 155.0, "Moong Dal": 145.0, "Masoor Dal": 130.0,
        "Eggs": 7.5, "Bread": 45.0, "Cheese": 310.0, "Curd": 65.0,
        "Paneer": 420.0, "Frozen Vegetables": 180.0, "Fresh Vegetables": 60.0, "Fresh Fruits": 90.0, "Potatoes": 30.0,
        "Onions": 35.0, "Tomatoes": 40.0, "Garlic": 220.0, "Ginger": 180.0, "Apples": 160.0,
        "Bananas": 55.0, "Oranges": 80.0, "Grapes": 120.0, "Watermelon": 45.0, "Biscuits": 35.0, "Chocolates": 110.0,
        "Chips": 50.0, "Namkeen": 95.0, "Soft Drinks": 40.0, "Packaged Juices": 110.0,
        "Mineral Water": 25.0, "Instant Noodles": 70.0, "Pasta": 85.0, "Sauces": 105.0, "Ketchup": 120.0,
        "Spices": 95.0, "Pickles": 130.0, "Jaggery": 75.0, "Breakfast Cereals": 210.0, "Corn Flakes": 190.0,
        "Peanut Butter": 240.0, "Jam": 155.0, "Honey": 280.0, "Ice Cream": 260.0, "Chocolate Spread": 250.0,
        "Baby Food": 360.0, "Diapers": 520.0, "Baby Wipes": 180.0, "Toilet Paper": 95.0, "Tissues": 60.0,
        "Dishwashing Liquid": 110.0, "Floor Cleaner": 140.0, "Laundry Detergent": 180.0, "Fabric Softener": 165.0,
        "Shampoo": 190.0, "Hair Oil": 155.0, "Toothpaste": 110.0, "Toothbrush": 55.0, "Soap": 40.0, "Body Wash": 160.0,
        "Deodorant": 210.0, "Face Wash": 175.0, "Moisturizer": 230.0, "Sunscreen": 310.0, "Shaving Cream": 95.0,
        "Razor": 120.0, "First Aid Kit": 180.0, "Pain Relief Tablets": 45.0, "Cold and Flu Tablets": 85.0,
        "Vitamins": 260.0,
        "Batteries": 95.0, "Light Bulbs": 75.0, "Plastic Containers": 130.0, "Aluminum Foil": 110.0,
        "Garbage Bags": 90.0, "Air Freshener": 160.0, "Ready to Eat Meals": 140.0, "Frozen Parathas": 120.0
    }

    fake = Faker('en_IN')
    insert_statements = []
    for product, price in products.items():
        name = product
        current_price = price
        old_price = round(random.uniform(0.9 * current_price, 1.1 * current_price), 2)

        created_date = fake.date_time_between(
            start_date="-2y", end_date="now"
        ).strftime("%Y-%m-%d %H:%M:%S")

        updated_date = fake.date_time_between(
            start_date=datetime.strptime(created_date, "%Y-%m-%d %H:%M:%S"),
            end_date="now"
        ).strftime("%Y-%m-%d %H:%M:%S")

        expiry_date = fake.date_between(
            start_date="+30d", end_date="+2y"
        ).strftime("%Y-%m-%d")
        insert_statements.append(f"INSERT INTO product (name, current_price, old_price, created_date, updated_date, expiry_date) VALUES ('{name}', '{current_price}', '{old_price}', '{created_date}', '{updated_date}', '{expiry_date}');")


    return insert_statements

