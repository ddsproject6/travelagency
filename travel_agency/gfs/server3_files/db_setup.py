from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Step 1: Create an engine that connects to SQLite
engine = create_engine('sqlite:///server3.db', echo=True)

# Step 2: Define a base class for your models
Base = declarative_base()


# Step 4: Define the 'hotel_packages' table
class HotelPackage(Base):
    __tablename__ = 'hotel_packages'

    id = Column(Integer, primary_key=True, autoincrement=True)
    package_name = Column(String)
    price_per_night = Column(Float)
    location = Column(String)
    duration_nights = Column(Integer)
Base.metadata.drop_all(engine)

# Step 5: Create all tables (if they don't exist)
Base.metadata.create_all(engine)

# Step 6: Set up a session to interact with the database
Session = sessionmaker(bind=engine)
session = Session()



# Step 8: Insert dummy data into 'hotel_packages' table
package1 = HotelPackage(package_name="Luxury Beach Resort", price_per_night=250.00, location="Maldives", duration_nights=7)
package2 = HotelPackage(package_name="Mountain Retreat", price_per_night=180.00, location="Switzerland", duration_nights=5)
package3 = HotelPackage(package_name="City Lights Experience", price_per_night=150.00, location="New York", duration_nights=3)

# Step 9: Add the records to the session and commit
session.add_all([package1, package2, package3])
session.commit()

# Step 10: Query the tables and print the results

hotel_packages = session.query(HotelPackage).all()


print("\nHotel Packages:")
for package in hotel_packages:
    print(f"ID: {package.id}, Package Name: {package.package_name}, Location: {package.location}, Price: {package.price_per_night}, Duration: {package.duration_nights} nights")
