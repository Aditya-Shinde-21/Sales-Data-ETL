from scripts.main.utility.my_sql_session import *
from scripts.generate_data import generate_product_table_data
from scripts.generate_data import generate_customer_table_data

connection = get_mysql_connection()
cursor = connection.cursor()

# Write generated customer data to MySQL database
statements = generate_customer_table_data.get_insert_statements()
for statement in statements:
    cursor.execute(statement)
    connection.commit()

# Write generated product data to MySQL database
statements = generate_product_table_data.get_insert_statements()
for statement in statements:
    cursor.execute(statement)
    connection.commit()


print("Customer table:")
cursor.execute("SELECT * FROM customer LIMIT 21;")
query = cursor.fetchall()
for row in query:
    print(*row, sep=' | | ')

print("Product table:")
cursor.execute("SELECT * FROM product LIMIT 21;")
query = cursor.fetchall()
for row in query:
    print(*row, sep=' | | ')

cursor.close()
connection.close()

