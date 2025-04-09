import pyodbc
import json
import yaml

# Read database config from the config.yaml
def load_config():
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    return config

def connect_to_rds(config):
    driver_path = "/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.10.so.6.1"
    connection_string = f'DRIVER={driver_path};SERVER={config["RDS_HOST"]},{config["DB_PORT"]};DATABASE={config["DB_NAME"]};UID={config["DB_USER"]};PWD={config["DB_PASSWORD"]}'
    return pyodbc.connect(connection_string)

def create_database(cursor, db_name):
    cursor.execute(f"IF DB_ID('{db_name}') IS NULL CREATE DATABASE {db_name};")
    cursor.execute(f"USE {db_name};")

def create_tables(cursor):
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Hotels' AND xtype='U')
        CREATE TABLE Hotels (
            HotelID INT PRIMARY KEY IDENTITY(1,1),
            HotelName VARCHAR(255) NOT NULL,
            Location VARCHAR(255) NOT NULL,
            TotalRooms INT NOT NULL
        );
    """)
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Rooms' AND xtype='U')
        CREATE TABLE Rooms (
            RoomID INT PRIMARY KEY IDENTITY(1,1),
            HotelID INT,
            RoomNumber INT NOT NULL,
            RoomType VARCHAR(255) NOT NULL,
            PricePerNight DECIMAL(10, 2) NOT NULL,
            FOREIGN KEY (HotelID) REFERENCES Hotels(HotelID),
            CONSTRAINT UC_Room UNIQUE(HotelID, RoomNumber)
        );
    """)
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Guests' AND xtype='U')
        CREATE TABLE Guests (
            GuestID INT PRIMARY KEY IDENTITY(1,1),
            FirstName VARCHAR(255) NOT NULL,
            LastName VARCHAR(255) NOT NULL,
            Email VARCHAR(255) NOT NULL UNIQUE,
            Phone VARCHAR(20)
        );
    """)
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Bookings' AND xtype='U')
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
    """)

def insert_data(cursor, data):
    # Insert Hotels
    for hotel in data['Hotels']:
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM Hotels WHERE HotelName = ?)
            INSERT INTO Hotels (HotelName, Location, TotalRooms)
            VALUES (?, ?, ?)
        """, (hotel['HotelName'], hotel['HotelName'], hotel['Location'], hotel['TotalRooms']))
    
    # Insert Rooms
    for room in data['Rooms']:
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM Rooms WHERE HotelID = ? AND RoomNumber = ?)
            INSERT INTO Rooms (HotelID, RoomNumber, RoomType, PricePerNight)
            VALUES (?, ?, ?, ?)
        """, (room['HotelID'], room['RoomNumber'], room['RoomType'], room['PricePerNight']))
    
    # Insert Guests
    for guest in data['Guests']:
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM Guests WHERE Email = ?)
            INSERT INTO Guests (FirstName, LastName, Email, Phone)
            VALUES (?, ?, ?, ?)
        """, (guest['Email'], guest['FirstName'], guest['LastName'], guest['Email'], guest['Phone']))

    # Insert Bookings
    for booking in data['Bookings']:
        cursor.execute("""
            INSERT INTO Bookings (GuestID, RoomID, CheckInDate, CheckOutDate, TotalCost)
            VALUES (?, ?, ?, ?, ?)
        """, (booking['GuestID'], booking['RoomID'], booking['CheckInDate'], booking['CheckOutDate'], booking['TotalCost']))


def main():
    config = load_config()
    with open('datasets/data.json', 'r') as f:
        data = json.load(f)
    
    connection = connect_to_rds(config)
    cursor = connection.cursor()
    
    create_database(cursor, config['DB_NAME'])
    create_tables(cursor)
    insert_data(cursor, data)

    connection.commit()
    connection.close()

if __name__ == "__main__":
    main()
