import pyodbc
import json
import yaml

def load_config():
    """Load configuration from config.yaml file."""
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    return config

def connect_to_rds(config, database='master', autocommit=False):
    """Connect to the RDS instance."""
    driver_path = "/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.10.so.6.1"
    connection_string = f'DRIVER={driver_path};SERVER={config["RDS_HOST"]},{config["DB_PORT"]};DATABASE={database};UID={config["DB_USER"]};PWD={config["DB_PASSWORD"]}'
    return pyodbc.connect(connection_string, autocommit=autocommit)

def create_database(cursor, db_name):
    """Create the database if it does not exist."""
    cursor.execute(f"IF DB_ID('{db_name}') IS NULL CREATE DATABASE {db_name};")
    #cursor.commit()  # Commit the database creation

def table_exists(cursor, table_name):
    """Check if a table exists in the database."""
    cursor.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = ?
    """, (table_name,))
    return cursor.fetchone()[0] > 0

def create_hotel_table(cursor):
    """Create the Hotels table if it does not exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Hotels (
            HotelID INT PRIMARY KEY IDENTITY(1,1),
            HotelName VARCHAR(255) NOT NULL,
            Location VARCHAR(255) NOT NULL,
            TotalRooms INT NOT NULL
        );
    """)

def create_room_table(cursor):
    """Create the Rooms table if it does not exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Rooms (
            RoomID INT PRIMARY KEY IDENTITY(1,1),
            HotelID INT,
            RoomNumber INT NOT NULL,
            RoomType VARCHAR(255) NOT NULL,
            PricePerNight DECIMAL(10, 2) NOT NULL,
            FOREIGN KEY (HotelID) REFERENCES Hotels(HotelID),
            CONSTRAINT UC_Room UNIQUE(HotelID, RoomNumber)
        );
    """)

def create_guest_table(cursor):
    """Create the Guests table if it does not exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Guests (
            GuestID INT PRIMARY KEY IDENTITY(1,1),
            FirstName VARCHAR(255) NOT NULL,
            LastName VARCHAR(255) NOT NULL,
            Email VARCHAR(255) NOT NULL UNIQUE,
            Phone VARCHAR(20)
        );
    """)

def create_booking_table(cursor):
    """Create the Bookings table if it does not exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Bookings (
            BookingID INT PRIMARY KEY IDENTITY(1,1),
            GuestID INT,
            RoomID INT,
            CheckInDate DATE NOT NULL,
            CheckOutDate DATE NOT NULL,
            TotalCost DECIMAL(10, 2),
            FOREIGN KEY (GuestID) REFERENCES Guests(GuestID),
            FOREIGN KEY (RoomID) REFERENCES Rooms(RoomID)
        );
    """)

def create_tables(cursor):
    """Create all necessary tables if they do not exist."""
    if not table_exists(cursor, 'Hotels'):
        create_hotel_table(cursor)
    if not table_exists(cursor, 'Rooms'):
        create_room_table(cursor)
    if not table_exists(cursor, 'Guests'):
        create_guest_table(cursor)
    if not table_exists(cursor, 'Bookings'):
        create_booking_table(cursor)

def insert_data(cursor, data):
    """Insert data into the database."""
    for hotel in data['Hotels']:
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM Hotels WHERE HotelName = ?)
            INSERT INTO Hotels (HotelName, Location, TotalRooms)
            VALUES (?, ?, ?)
        """, (hotel['HotelName'], hotel['Location'], hotel['TotalRooms']))
    
    for room in data['Rooms']:
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM Rooms WHERE HotelID = ? AND RoomNumber = ?)
            INSERT INTO Rooms (HotelID, RoomNumber, RoomType, PricePerNight)
            VALUES (?, ?, ?, ?)
        """, (room['HotelID'], room['RoomNumber'], room['RoomType'], room['PricePerNight']))
    
    for guest in data['Guests']:
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM Guests WHERE Email = ?)
            INSERT INTO Guests (FirstName, LastName, Email, Phone)
            VALUES (?, ?, ?, ?)
        """, (guest['FirstName'], guest['LastName'], guest['Email'], guest['Phone']))

    for booking in data['Bookings']:
        cursor.execute("""
            INSERT INTO Bookings (GuestID, RoomID, CheckInDate, CheckOutDate, TotalCost)
            VALUES (?, ?, ?, ?, ?)
        """, (booking['GuestID'], booking['RoomID'], booking['CheckInDate'], booking['CheckOutDate'], booking['TotalCost']))

def main():
    """Main function to set up the database and insert data."""
    config = load_config()
    with open('datasets/data.json', 'r') as f:
        data = json.load(f)
    
    # Step 1: Connect to master to create the database
    master_conn = connect_to_rds(config, database='master', autocommit=True)
    master_cursor = master_conn.cursor()
    create_database(master_cursor, config['DB_NAME'])
    master_conn.close()

    # Step 2: Connect to the new database for setup
    connection = connect_to_rds(config, database=config['DB_NAME'])
    cursor = connection.cursor()
    
    create_tables(cursor)  # Create tables if they don't exist
    insert_data(cursor, data)  # Insert data into the tables

    connection.commit()
    connection.close()

if __name__ == "__main__":
    main()
