import pyodbc
import json
import yaml

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
def run_queries(cursor):
    # Example queries to run
    cursor.execute("SELECT HotelID, SUM(TotalCost) FROM Bookings b JOIN Rooms r ON b.RoomID = r.RoomID GROUP BY HotelID;")
    result = cursor.fetchall()

# Save the result into a YAML file
    output = []
    for row in result:
        output.append({"HotelName": row[0], "TotalRevenue": row[1]})

    # Write to YAML
    with open('query_results.yaml', 'w') as f:
        yaml.dump(output, f)

def main():
    config = load_config()
    with open('datasets/data.json', 'r') as f:
        data = json.load(f)
    
    connection = connect_to_rds(config)
    cursor = connection.cursor()
    
    create_database(cursor, config['DB_NAME'])
    run_queries(cursor)

    connection.commit()
    connection.close()

if __name__ == "__main__":
    main()
