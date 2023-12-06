import mysql.connector
from pymongo import MongoClient


def main():
    mysql_config = {
        'host': 'localhost',
        'user': 'root',
        'password': '12345678',
        'database': 'appointments',
    }
    mongo_config = {
    'host': 'localhost',
    'port': 27017,
    'database': 'Appointments',
    'collection': 'appointments',
    }
    try:
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor(dictionary=True)

        create_table_query = "CREATE TABLE IF NOT EXISTS appointments (id INT AUTO_INCREMENT PRIMARY KEY, title varchar(225), owner varchar(225))"
        cursor.execute(create_table_query)
        cursor.execute("SELECT COUNT(*) FROM appointments")
        flag = cursor.fetchone()['COUNT(*)']
        if(flag == 0):
            flag1 = "Number Of Changes"
            flag2 = "0"
            data_to_insert = (flag1,flag2)
            create_change_flag = "INSERT appointments (title, owner) VALUES (%s, %s);"
            cursor.execute(create_change_flag,data_to_insert)
            connection.commit()

        Choice = int(input("Choose an action :\n1)Create Appointment\n2)Delete Appointment\n3)Change Appointment Owner\n4)Show Owner Appointments\n5)Exit\n"))
        while (Choice !=5):
            if(Choice == 1):
                try:
                    title = input("Enter Appointment Name:\n")
                    owner = input("Enter Appointment Owner\n")
                    data_to_insert = (title, owner)
                    create_appointment_query = "INSERT INTO appointments (title, owner) VALUES (%s, %s);"
                    cursor.execute(create_appointment_query, data_to_insert)
                    connection.commit()
                except mysql.connector.Error as err:
                    print("Addition Unsuccessful", err)
            elif(Choice == 2):
                try:
                    name = input("Enter Appointment Name\n")
                    data_to_insert = (name,)
                    delete_appointment_query = "DELETE FROM appointments WHERE title = %s"
                    cursor.execute(delete_appointment_query, data_to_insert)
                    connection.commit()
                except mysql.connector.Error as err:
                    print("No Such Appointment", err)
            elif(Choice == 3):
                try:
                    id = int(input("Enter Appointment ID \n"))
                    name = input("Enter New Owner\n")
                    data_to_insert = (name,id)
                    change_appointment_query = "UPDATE appointments SET owner = %s WHERE id = %s"
                    cursor.execute(change_appointment_query, data_to_insert)
                    fetch_number_changes = "SELECT owner FROM appointments where id =1"
                    cursor.execute(fetch_number_changes)
                    newchanges = int(cursor.fetchone()['owner'])
                    newchanges = newchanges+1
                    change_to_insert = (str(newchanges),)
                    change_changes_query = "UPDATE appointments SET owner = %s WHERE id = 1"
                    cursor.execute(change_changes_query,change_to_insert)
                    connection.commit()
                except mysql.connector.Error as err:
                    print("No Such id", err)
            elif(Choice == 4):
                try:
                    name = input("Enter Appointment Owner \n")
                    data_to_insert = (name,)
                    show_appointment_query = "SELECT * FROM appointments WHERE owner = %s"
                    cursor.execute(show_appointment_query, data_to_insert)
                    details = cursor.fetchall()
                    print(details)
                except mysql.connector.Error as err:
                    print("No Such Owner", err)
            Choice = int(input("Choose an action :\n1)Create Appointment\n2)Delete Appointment\n3)Change Appointment Owner\n4)Show Owner Appointments\n5)Exit\n"))

        cursor.close()
        connection.close()

    except mysql.connector.Error as err:
        print("Cannot Connect To Master Database Switching To Replica Database In Readonly Mode\n")
        mongo_client = MongoClient(mongo_config['host'], mongo_config['port'])
        mongo_db = mongo_client[mongo_config['database']]
        mongo_collection = mongo_db[mongo_config['collection']]
        Choice = int(input("Choose an action :\n1)Show Owner Appointments\n2)Exit\n"))
        while (Choice !=2):
            if(Choice == 1):
                oname = input("Enter Appointment Owner \n")
                query_condition = ({'owner':oname})
                result = mongo_collection.find(query_condition)
                for record in result:
                    print(record)
            Choice = int(input("Choose an action :\n1)Show Owner Appointments\n2)Exit\n"))

if __name__ == "__main__":
    main()

