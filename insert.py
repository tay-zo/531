import psycopg2
import logging
import json

INPUT_FILE = "yelp_data_set/yelp_academic_dataset_tip.json"
MAX_ROW_COUNT = 900000
LOG_INTERVAL = 20000

def insertRow(jsonObj, cur):
    query = "INSERT INTO tip (user_id, business_id, text, date, compliment_count) VALUES (%s,%s,%s,%s,%s)"
    params = (jsonObj['user_id'], jsonObj['business_id'], jsonObj['text'], jsonObj['date'], jsonObj['compliment_count'])
    cur.execute(query, params)


def createTable(table, command, cur):
    logging.info("Creating table '{}'...".format(table))
    cur.execute(command)


def dropTable(table, cur):
    logging.info("Dropping table if it exists '{}'...".format(table))
    command = "DROP TABLE IF EXISTS " + table
    cur.execute(command)


def createTipTable(cur):
    dropTable("tip", cur)
    command = "CREATE TABLE IF NOT EXISTS tip (uuid text, user_id VARCHAR(22), business_id VARCHAR(22), text text, date timestamp, compliment_count integer)"
    createTable("tip", command, cur)


def getDbConnection():
    return psycopg2.connect(
        host="localhost",
        database="mydb",
        user="postgres",
        password="postgres",
        # port="5438"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, encoding='utf-8', format='%(asctime)s | %(levelname)s | %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info("Program started")

    logging.info('Connecting to the PostgreSQL database...')
    conn = getDbConnection()
    
    # create a cursor
    cur = conn.cursor()

    # create tip table
    createTipTable(cur)
       
    # Read file
    counter = 0
    with open(INPUT_FILE) as file:
        for line in file:
            jsonObj = json.loads(line)
            insertRow(jsonObj, cur)
            counter += 1

            if counter % LOG_INTERVAL == 0:
                logging.info("%s rows inserted", counter)
            if counter >= MAX_ROW_COUNT:
                break

	# close the communication with the PostgreSQL
    cur.close()

    # commit changes
    conn.commit()

    logging.info("Program ended")