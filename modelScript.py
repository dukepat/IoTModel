import time
import mysql.connector as cnx
import pandas as pd
#import time
#import matplotlib as plt
#import time
import joblib
connection = cnx.connect(
    host= "dukeazureapp2.mysql.database.azure.com",
    database= "iot_database",
    user= "dukeazuredb2",
    password= 'Myworld$2019')


while True:
#     connection = cnx.connect(
#     host= "dukeazureapp2.mysql.database.azure.com",
#     database= "iot_database",
#     user= "dukeazuredb2",
#     password= 'Myworld$2019')
    
    # Load the existing model
    # with open('model.pkl', 'rb') as f:
    model = joblib.load('model.pkl')

    # Fetch all rows where the State column is None or NULL

    select_query = """
    SELECT *
    FROM MotorDataPredicted
    WHERE State IS NULL;
    """
    cursor=connection.cursor()
    cursor.execute(select_query)
    rows = cursor.fetchall()
    connection.commit()
    if not rows:
        print("No latest data to be executed at:", time.strftime("%H:%M:%S"))
        pass  # Do nothing if rows is empty
    else:
        for row in rows:
            features = [row[2], row[3], row[4], row[5], row[6]]  # Assuming columns Current, Magnetic, ax, ay, az
            if (not row[2] or not row[3] or not row[4] or not row[5] or not row[6]):
                cursor.execute("DELETE FROM MotorDataPredicted WHERE id = %s", ([row[0]]))
                connection.commit()
                print(f"Deleted an incomplete data ({row[2], row[3], row[4], row[5], row[6]}) from db. Executed at:", time.strftime("%H:%M:%S"))
                pass
            else: 
                
                input_df = pd.DataFrame([[row[2], row[3], row[4], row[5], row[6]]], columns=['ct', 'm', 'ax','ay', 'az']).astype(int)
                print(f"......Predicting for: {input_df}")
                predicted_state = model.predict(input_df)
                predicted_state = str(predicted_state[0])
                print(predicted_state)

                update_query = """
                UPDATE MotorDataPredicted
                SET State = %s
                WHERE id = %s;
                """
                cursor.execute(update_query, (predicted_state, row[0]))  # Assuming id is the first column (index 0)
                # rows=[]
                # Committing our changes
                connection.commit()
                print(f"Committed latest data to db. Motor state is {predicted_state} executed at:{time.strftime('%H:%M:%S')}")

    # Sleep for one minute (60 seconds)
    time.sleep(60)