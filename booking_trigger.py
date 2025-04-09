import pyodbc
import json
import yaml
from datetime import datetime

def load_config():
    try:
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        print(f"Error loading configuration: {e}")
        raise

def connect_to_rds(config, database='master', autocommit=False):
    try:
        driver_path = "/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.10.so.6.1"
        connection_string = f'DRIVER={driver_path};SERVER={config["RDS_HOST"]},{config["DB_PORT"]};DATABASE={database};UID={config["DB_USER"]};PWD={config["DB_PASSWORD"]}'
        return pyodbc.connect(connection_string, autocommit=autocommit)
    except pyodbc.Error as e:
        print(f"Error connecting to RDS: {e}")
        raise

def calculate_total_cost(cursor, room_id, check_in_date, check_out_date):
    try:
        cursor.execute("""SELECT PricePerNight FROM Rooms WHERE RoomID = ?""", (room_id,))
        room_price = cursor.fetchone()

        if room_price:
            room_price = room_price[0]
            check_in = datetime.strptime(check_in_date, '%Y-%m-%d')
            check_out = datetime.strptime(check_out_date, '%Y-%m-%d')
            duration = (check_out - check_in).days
            total_cost = room_price * duration
            return total_cost
        else:
            print(f"No room found with RoomID: {room_id}")
            return 0
    except Exception as e:
        print(f"Error calculating total cost: {e}")
        raise

def check_and_insert_booking(cursor, booking_data):
    try:
        for booking in booking_data:
            cursor.execute("""
                SELECT * FROM Bookings 
                WHERE RoomID = ? 
                  AND (CheckInDate BETWEEN ? AND ? OR CheckOutDate BETWEEN ? AND ?)
            """, (booking['RoomID'], booking['CheckInDate'], booking['CheckOutDate'], booking['CheckInDate'], booking['CheckOutDate']))
            
            if cursor.fetchone() is None:  # If no overlapping booking
                total_cost = calculate_total_cost(cursor, booking['RoomID'], booking['CheckInDate'], booking['CheckOutDate'])
                cursor.execute("""
                    INSERT INTO Bookings (GuestID, RoomID, CheckInDate, CheckOutDate, TotalCost)
                    VALUES (?, ?, ?, ?, ?)
                """, (booking['GuestID'], booking['RoomID'], booking['CheckInDate'], booking['CheckOutDate'], total_cost))
                print(f"Booking inserted for GuestID: {booking['GuestID']} in RoomID: {booking['RoomID']}")
            else:
                print(f"Booking conflict found for RoomID: {booking['RoomID']} in the requested date range.")
    except Exception as e:
        print(f"Error checking and inserting booking: {e}")
        raise

def main():
    try:
        config = load_config()

        booking_data = [
            {
                'GuestID': 1,
                'RoomID': 101,
                'CheckInDate': '2025-03-01',
                'CheckOutDate': '2025-03-05',
            }
        ]
        
        connection = connect_to_rds(config)
        cursor = connection.cursor()

        check_and_insert_booking(cursor, booking_data)

        connection.commit()
        print("Changes committed successfully.")
    except Exception as e:
        print(f"An error occurred during execution: {e}")
    finally:
        # Ensure the connection is closed even if there was an error
        try:
            connection.close()
            print("Connection closed.")
        except NameError:
            print("No connection to close.")

if __name__ == "__main__":
    main()
