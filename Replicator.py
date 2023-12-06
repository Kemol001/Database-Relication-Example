import time
import mysql.connector
from pymongo import MongoClient

mysql_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '12345678',
    'database': 'appointments',
}
# MongoDB Configuration
mongo_config = {
    'host': 'localhost',
    'port': 27017,
    'database': 'Appointments',
    'collection': 'appointments',
}

lastUpdateID = 0
databaseSize = 0
def replicate_data():
    global lastUpdateID
    global databaseSize

    # Connect to MySQL
    mysql_conn = mysql.connector.connect(**mysql_config)
    mysql_cursor = mysql_conn.cursor(dictionary=True)

    # Connect to MongoDB
    mongo_client = MongoClient(mongo_config['host'], mongo_config['port'])
    mongo_db = mongo_client[mongo_config['database']]
    mongo_collection = mongo_db[mongo_config['collection']]

    # Initial replication
    if mongo_collection.count_documents({}) == 0:
        mysql_cursor.execute("SELECT MAX(id) FROM appointments")
        lastUpdateID = mysql_cursor.fetchone()['MAX(id)']
        mysql_cursor.execute("SELECT COUNT(*) FROM appointments")
        databaseSize = mysql_cursor.fetchone()['COUNT(*)']
        mysql_cursor.execute("SELECT * FROM appointments")
        initial_data = mysql_cursor.fetchall()
        mongo_collection.insert_many(initial_data)
        # mysql_cursor.close()
        # mysql_conn.close()

    while True:
        mysql_conn.reconnect()
        # mysql_conn = mysql.connector.connect(**mysql_config)
        # mysql_cursor = mysql_conn.cursor(dictionary=True)
        # Poll for changes in MySQL
        mysql_cursor.execute("SELECT MAX(id) FROM appointments")
        newUpdate = mysql_cursor.fetchone()['MAX(id)']
        if(newUpdate > lastUpdateID):
            data_to_insert = (lastUpdateID,)
            polling_query = "SELECT * FROM appointments WHERE id > %s"
            mysql_cursor.execute(polling_query,data_to_insert)
            new_data = mysql_cursor.fetchall()
            print(new_data)
            lastUpdateID = newUpdate
            mysql_cursor.execute("SELECT COUNT(*) FROM appointments")
            databaseSize = mysql_cursor.fetchone()['COUNT(*)']

            # Replicate new data to MongoDB
            mongo_collection.insert_many(new_data)
            print(f"Replicated {len(new_data)} new records to MongoDB.")

        # Delete data from MongoDB
        mysql_cursor.execute("SELECT COUNT(*) FROM appointments")
        databaseSizeUpdate = mysql_cursor.fetchone()['COUNT(*)']
        if(databaseSizeUpdate < databaseSize):
            mongo_collection.delete_many({})
            mysql_cursor.execute("SELECT * FROM appointments")
            initial_data = mysql_cursor.fetchall()
            mongo_collection.insert_many(initial_data)
            print(f"Deleted {databaseSize-databaseSizeUpdate} records from MongoDB.")
            databaseSize = databaseSizeUpdate
        
        # Update data to MongoDB        
        fetch_number_changes = "SELECT owner FROM appointments where id =1"
        mysql_cursor.execute(fetch_number_changes)
        newchanges = int(mysql_cursor.fetchone()['owner'])
        if(newchanges>0):
            mongo_collection.delete_many({})
            print(f"Updated {newchanges} records to MongoDB.")
            newchanges = 0
            change_to_insert = (str(newchanges),)
            change_changes_query = "UPDATE appointments SET owner = %s WHERE id = 1"
            mysql_cursor.execute(change_changes_query,change_to_insert)
            mysql_conn.commit()
            mysql_cursor.execute("SELECT * FROM appointments")
            initial_data = mysql_cursor.fetchall()
            mongo_collection.insert_many(initial_data)

        time.sleep(5)  # Polling interval in seconds

if __name__ == "__main__":
    replicate_data()