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

def insert_sample_data(cursor):
    # --- Insert Hotels ---
    hotels = [
        {"HotelName": "Sunset Resort", "Location": "Miami Beach, FL", "TotalRooms": 50},
        {"HotelName": "Mountain View Hotel", "Location": "Aspen, CO", "TotalRooms": 30},
        {"HotelName": "Ocean Breeze Hotel", "Location": "San Diego, CA", "TotalRooms": 40}
    ]

    hotel_name_to_id = {}

    for hotel in hotels:
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM Hotels WHERE HotelName = ?)
            BEGIN
                INSERT INTO Hotels (HotelName, Location, TotalRooms)
                VALUES (?, ?, ?);
            END
        """, (hotel["HotelName"], hotel["HotelName"], hotel["Location"], hotel["TotalRooms"]))

        cursor.execute("SELECT HotelID FROM Hotels WHERE HotelName = ?", (hotel["HotelName"],))
        hotel_id = cursor.fetchone()[0]
        hotel_name_to_id[hotel["HotelName"]] = hotel_id

    # --- Insert Guests ---
    guests = [
        {"FirstName": "John", "LastName": "Doe", "Email": "john.doe@example.com", "Phone": "555-1234"},
        {"FirstName": "Jane", "LastName": "Smith", "Email": "jane.smith@example.com", "Phone": "555-5678"},
        {"FirstName": "Alice", "LastName": "Johnson", "Email": "alice.johnson@example.com", "Phone": "555-8765"},
        {"FirstName": "Bob", "LastName": "Williams", "Email": "bob.williams@example.com", "Phone": "555-4321"},
        {"FirstName": "Eve", "LastName": "Davis", "Email": "eve.davis@example.com", "Phone": "555-9876"}
    ]

    guest_email_to_id = {}

    for guest in guests:
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM Guests WHERE Email = ?)
            BEGIN
                INSERT INTO Guests (FirstName, LastName, Email, Phone)
                VALUES (?, ?, ?, ?);
            END
        """, (guest["Email"], guest["FirstName"], guest["LastName"], guest["Email"], guest["Phone"]))

        cursor.execute("SELECT GuestID FROM Guests WHERE Email = ?", (guest["Email"],))
        guest_id = cursor.fetchone()[0]
        guest_email_to_id[guest["Email"]] = guest_id

    # --- Insert Rooms ---
    rooms = [
        {"HotelName": "Sunset Resort", "RoomNumber": 101, "RoomType": "Single", "PricePerNight": 120.00},
        {"HotelName": "Sunset Resort", "RoomNumber": 102, "RoomType": "Double", "PricePerNight": 200.00},
        {"HotelName": "Sunset Resort", "RoomNumber": 103, "RoomType": "Suite", "PricePerNight": 300.00},
        {"HotelName": "Sunset Resort", "RoomNumber": 104, "RoomType": "Single", "PricePerNight": 110.00},
        {"HotelName": "Mountain View Hotel", "RoomNumber": 201, "RoomType": "Double", "PricePerNight": 150.00},
        {"HotelName": "Mountain View Hotel", "RoomNumber": 202, "RoomType": "Single", "PricePerNight": 100.00},
        {"HotelName": "Mountain View Hotel", "RoomNumber": 203, "RoomType": "Suite", "PricePerNight": 250.00},
        {"HotelName": "Ocean Breeze Hotel", "RoomNumber": 301, "RoomType": "Double", "PricePerNight": 180.00}
    ]

    room_lookup = {}

    for room in rooms:
        hotel_id = hotel_name_to_id[room["HotelName"]]

        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM Rooms WHERE RoomNumber = ? AND HotelID = ?)
            BEGIN
                INSERT INTO Rooms (HotelID, RoomNumber, RoomType, PricePerNight)
                VALUES (?, ?, ?, ?);
            END
        """, (room["RoomNumber"], hotel_id, hotel_id, room["RoomNumber"], room["RoomType"], room["PricePerNight"]))

        cursor.execute("SELECT RoomID FROM Rooms WHERE HotelID = ? AND RoomNumber = ?", (hotel_id, room["RoomNumber"]))
        room_id = cursor.fetchone()[0]
        room_lookup[(hotel_id, room["RoomNumber"])] = room_id

    # --- Insert Bookings ---
    bookings = [
        {"GuestEmail": "john.doe@example.com", "HotelName": "Sunset Resort", "RoomNumber": 101,
         "CheckInDate": "2025-03-01", "CheckOutDate": "2025-03-05", "TotalCost": 480.00},

        {"GuestEmail": "jane.smith@example.com", "HotelName": "Mountain View Hotel", "RoomNumber": 202,
         "CheckInDate": "2025-04-01", "CheckOutDate": "2025-04-05", "TotalCost": 400.00},

        {"GuestEmail": "alice.johnson@example.com", "HotelName": "Ocean Breeze Hotel", "RoomNumber": 301,
         "CheckInDate": "2025-05-01", "CheckOutDate": "2025-05-03", "TotalCost": 360.00},

        {"GuestEmail": "bob.williams@example.com", "HotelName": "Sunset Resort", "RoomNumber": 103,
         "CheckInDate": "2025-02-10", "CheckOutDate": "2025-02-12", "TotalCost": 600.00},

        {"GuestEmail": "eve.davis@example.com", "HotelName": "Mountain View Hotel", "RoomNumber": 203,
         "CheckInDate": "2025-06-10", "CheckOutDate": "2025-06-15", "TotalCost": 1250.00}
    ]

    for booking in bookings:
        guest_id = guest_email_to_id.get(booking["GuestEmail"])
        hotel_id = hotel_name_to_id.get(booking["HotelName"])
        room_id = room_lookup.get((hotel_id, booking["RoomNumber"]))

        if guest_id and room_id:
            cursor.execute("""
                INSERT INTO Bookings (GuestID, RoomID, CheckInDate, CheckOutDate, TotalCost)
                VALUES (?, ?, ?, ?, ?)
            """, (guest_id, room_id, booking["CheckInDate"], booking["CheckOutDate"], booking["TotalCost"]))
        else:
            print(f"Skipping booking: GuestID or RoomID not found for {booking}")


def main():
    config = load_config()
    
    with open("datasets/data.json", "r") as file:
        data = json.load(file)
    
    #  STEP 1: Connect to master first and create DB
    master_connection = connect_to_rds(config, database='master')
    master_cursor = master_connection.cursor()
    create_database(master_cursor, config['DB_NAME'])
    master_connection.commit()
    master_connection.close()

    #  STEP 2: Now connect to your actual target DB
    connection = connect_to_rds(config, database=config['DB_NAME'])
    cursor = connection.cursor()

    # STEP 3: Continue with table creation and data insertion
    create_hotel_table(cursor)
    create_rooms_table(cursor)
    create_guests_table(cursor)
    create_bookings_table(cursor)
    
    insert_sample_data(cursor, data)
    connection.commit()
    connection.close()