import sqlite3

# Connect to the SQLite database file
db_path = "test.db"  # Path to your SQLite database file
conn = sqlite3.connect(db_path)

# Create a cursor object
cursor = conn.cursor()

# Define the email to query
email = "doe123@gmail.com"

try:
    # Execute the query with a WHERE clause to filter by email
    cursor.execute("SELECT * FROM user ")
    
    # Fetch the result
    result = cursor.fetchall()
    
    if result:
        print("User found:", result)
    else:
        print("No user found with email:", email)

except sqlite3.Error as e:
    print("Database error:", e)

finally:
    # Close the connection
    conn.close()
