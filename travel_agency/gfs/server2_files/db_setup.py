from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Step 1: Create an engine that connects to SQLite
engine = create_engine('sqlite:///server2.db', echo=True)

# Step 2: Define a base class for your models
Base = declarative_base()

# Step 3: Define the 'users' table
class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    email = Column(String)
    age = Column(Integer)

Base.metadata.drop_all(engine)

# Step 5: Create all tables (if they don't exist)
Base.metadata.create_all(engine)

# Step 6: Set up a session to interact with the database
Session = sessionmaker(bind=engine)
session = Session()

# Step 7: Insert dummy data into 'users' table
user1 = User(name="Alice Johnson", email="alice@example.com", age=30)
user2 = User(name="Bob Smith", email="bob@example.com", age=40)
user3 = User(name="Charlie Brown", email="charlie@example.com", age=25)



# Step 9: Add the records to the session and commit
session.add_all([user1, user2, user3])
session.commit()

users = session.query(User).all()

print("\nUsers:")
for user in users:
    print(f"ID: {user.id}, Name: {user.name}, Email: {user.email}, Age: {user.age}")


