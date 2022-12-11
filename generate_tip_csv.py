import logging
import json, csv
import uuid

INPUT_FILE = "yelp_data_set/yelp_academic_dataset_tip.json"
OUTPUT_CASS_FILE = "yelp_data_set/cass_yelp_academic_dataset_tip.csv"
OUTPUT_PG_FILE = "yelp_data_set/pg_yelp_academic_dataset_tip.csv"

MAX_ROW_COUNT = 900000
LOG_INTERVAL = 20000


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, encoding='utf-8', format='%(asctime)s | %(levelname)s | %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info("Program started")

    counter = 0
    with open(OUTPUT_CASS_FILE, 'w') as outputFile:
        writer = csv.writer(outputFile, delimiter=",", quoting=csv.QUOTE_NONE, escapechar="\\")
        header = ("uuid", "user_id", "business_id", "text", "date", "compliment_count")
        writer.writerow(header)

        with open(INPUT_FILE) as inputFile:
            for line in inputFile:
                jsonObj = json.loads(line)
                text = jsonObj['text'].replace('\n', '_')
                if text[0] != '"' and text[-1] != '"':
                    text = '"' + text + '"'
                row = (uuid.uuid4(), jsonObj['user_id'], jsonObj['business_id'], text, jsonObj['date'], jsonObj['compliment_count'])
                writer.writerow(row)
                counter += 1

                if counter % LOG_INTERVAL == 0:
                    logging.info("%s rows written to cassandra CSV file", counter)
                if counter >= MAX_ROW_COUNT:
                    break

    counter = 0
    with open(OUTPUT_PG_FILE, 'w') as outputFile:
        writer = csv.writer(outputFile, delimiter=",", escapechar="\\")
        header = ("uuid", "user_id", "business_id", "text", "date", "compliment_count")
        writer.writerow(header)

        with open(INPUT_FILE) as inputFile:
            for line in inputFile:
                jsonObj = json.loads(line)
                text = jsonObj['text'].replace('\n', '_')
                row = (uuid.uuid4(), jsonObj['user_id'], jsonObj['business_id'], text, jsonObj['date'], jsonObj['compliment_count'])
                writer.writerow(row)
                counter += 1

                if counter % LOG_INTERVAL == 0:
                    logging.info("%s rows written to PG CSV file", counter)
                if counter >= MAX_ROW_COUNT:
                    break

    logging.info("Program ended")


# Cassanra commands
# CREATE KEYSPACE cassdb WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 2};
# DROP TABLE tip;
# CREATE TABLE tip(uuid uuid PRIMARY KEY, user_id text, business_id text, text text, date text, compliment_count tinyint);
# COPY tip (uuid, user_id, business_id, text, date, compliment_count) FROM '/tmp/yelp/cass_yelp_academic_dataset_tip.csv' WITH HEADER = true;
# select count(*) from tip;

# Postgres commands
# DROP TABLE tip;
# CREATE TABLE IF NOT EXISTS tip (uuid text, user_id VARCHAR(22), business_id VARCHAR(22), text text, date timestamp, compliment_count integer);
# COPY tip (uuid, user_id, business_id, text, date, compliment_count) FROM '/my-data/pg_yelp_academic_dataset_tip.csv' DELIMITER ',' CSV HEADER escape '\';
# select count(*) from tip;