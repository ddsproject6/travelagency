import sqlite3

# Connect to the SQLite database file
db_path = "test.db"  # Path to your SQLite database file
conn = sqlite3.connect(db_path)

# Create a cursor object
cursor = conn.cursor()

# Define the email to query
email = "doe@gmail.com"

try:
    # Execute the query
    cursor.execute("SELECT * FROM user")
    #  WHERE email = ? LIMIT 1", (email,)
    
    # Fetch the result
    result = cursor.fetchone()
    
    if result:
        print("User found:", result)
    else:
        print("No user found with email:", email)

except sqlite3.Error as e:
    print("Database error:", e)

finally:
    # Close the connection
    conn.close()
