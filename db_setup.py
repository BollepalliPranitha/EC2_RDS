import pyodbc
import json
import yaml
from datetime import datetime

# Read database config from the config.yaml
def load_config():
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    return config

def connect_to_rds(config, database='master', autocommit=False):
    driver_path = "/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.10.so.6.1"
    connection_string = f'DRIVER={driver_path};SERVER={config["RDS_HOST"]},{config["DB_PORT"]};DATABASE={database};UID={config["DB_USER"]};PWD={config["DB_PASSWORD"]}'
    return pyodbc.connect(connection_string, autocommit=autocommit)

def create_database(cursor, db_name):
    cursor.execute(f"IF DB_ID('{db_name}') IS NULL CREATE DATABASE {db_name};")
    cursor.execute(f"USE {db_name};")

def create_hotel_table(cursor):
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Hotels')
        BEGIN
            CREATE TABLE Hotels (
                HotelID INT PRIMARY KEY IDENTITY(1,1),
                HotelName VARCHAR(255) NOT NULL,
                Location VARCHAR(255) NOT NULL,
                TotalRooms INT NOT NULL
            );
        END
    """)

def create_rooms_table(cursor):
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Rooms')
        BEGIN
            CREATE TABLE Rooms (
                RoomID INT PRIMARY KEY IDENTITY(1,1),
                HotelID INT,
                RoomNumber INT NOT NULL,
                RoomType VARCHAR(255) NOT NULL,
                PricePerNight DECIMAL(10, 2) NOT NULL,
                FOREIGN KEY (HotelID) REFERENCES Hotels(HotelID),
                CONSTRAINT UC_Room UNIQUE(HotelID, RoomNumber)
            );
        END
    """)

def create_guests_table(cursor):
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Guests')
        BEGIN
            CREATE TABLE Guests (
                GuestID INT PRIMARY KEY IDENTITY(1,1),
                FirstName VARCHAR(255) NOT NULL,
                LastName VARCHAR(255) NOT NULL,
                Email VARCHAR(255) NOT NULL UNIQUE,
                Phone VARCHAR(20)
            );
        END
    """)

def create_bookings_table(cursor):
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Bookings')
        BEGIN
            CREATE TABLE Bookings (
                BookingID INT PRIMARY KEY IDENTITY(1,1),
                GuestID INT,
                RoomID INT,
                CheckInDate DATE NOT NULL,
                CheckOutDate DATE NOT NULL,
                TotalCost DECIMAL(10, 2),
                FOREIGN KEY (GuestID) REFERENCES Guests(GuestID),
                FOREIGN KEY (RoomID) REFERENCES Rooms(RoomID)
            );
        END
    """)

def calculate_total_cost(cursor, room_id, check_in_date, check_out_date):
    cursor.execute("""
        SELECT PricePerNight FROM Rooms WHERE RoomID = ?
    """, (room_id,))
    room_price = cursor.fetchone()
    
    if room_price:
        room_price = room_price[0]
        check_in = datetime.strptime(check_in_date, '%Y-%m-%d')
        check_out = datetime.strptime(check_out_date, '%Y-%m-%d')
        duration = (check_out - check_in).days
        total_cost = room_price * duration
        return total_cost
    return 0

def insert_sample_data(cursor, data):
    # Insert hotels data
    for hotel in data["Hotels"]:
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM Hotels WHERE HotelName = ?)
            BEGIN
                INSERT INTO Hotels (HotelName, Location, TotalRooms)
                VALUES (?, ?, ?);
            END
        """, (hotel["HotelName"], hotel["HotelName"], hotel["Location"], hotel["TotalRooms"]))

    # Insert rooms data
    for room in data["Rooms"]:
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM Rooms WHERE RoomNumber = ? AND HotelID = ?)
            BEGIN
                INSERT INTO Rooms (HotelID, RoomNumber, RoomType, PricePerNight)
                VALUES (?, ?, ?, ?);
            END
        """, (room["HotelID"], room["RoomNumber"], room["RoomType"], room["PricePerNight"]))

    # Insert guests data
    for guest in data["Guests"]:
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM Guests WHERE Email = ?)
            BEGIN
                INSERT INTO Guests (FirstName, LastName, Email, Phone)
                VALUES (?, ?, ?, ?);
            END
        """, (guest["FirstName"], guest["LastName"], guest["Email"], guest["Phone"]))

    # Insert bookings data
    for booking in data["Bookings"]:
        cursor.execute("""
            SELECT GuestID FROM Guests WHERE GuestID = ?
        """, (booking['GuestID'],))
        guest_exists = cursor.fetchone()

        cursor.execute("""
            SELECT RoomID FROM Rooms WHERE RoomID = ?
        """, (booking['RoomID'],))
        room_exists = cursor.fetchone()

        if guest_exists and room_exists:
            cursor.execute("""
                INSERT INTO Bookings (GuestID, RoomID, CheckInDate, CheckOutDate, TotalCost)
                VALUES (?, ?, ?, ?, ?)
            """, (booking['GuestID'], booking['RoomID'], booking['CheckInDate'], booking['CheckOutDate'], booking['TotalCost']))
        else:
            print(f"Skipping booking due to missing guest or room. GuestID: {booking['GuestID']}, RoomID: {booking['RoomID']}")

def main():
    config = load_config()
    
    
    with open("datasets/data.json", "r") as file:
        data = json.load(file)
    
    # Step 1: Connect to RDS database (ensure you are connecting to the right DB)
    connection = connect_to_rds(config)
    cursor = connection.cursor()
    
    # Step 2: Create the database if not exists
    create_database(cursor, config['DB_NAME'])
    
    # Step 3: Create tables if they don't exist
    create_hotel_table(cursor)
    create_rooms_table(cursor)
    create_guests_table(cursor)
    create_bookings_table(cursor)
    
    # Step 4: Insert sample data
    insert_sample_data(cursor, data)
    
    # Step 5: Commit the transaction
    connection.commit()

    # Step 6: Close the connection
    connection.close()

if __name__ == "__main__":
    main()
