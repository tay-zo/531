import logging
import json, csv

INPUT_FILE = "yelp_data_set/yelp_academic_dataset_review.json"
OUTPUT_CASS_FILE = "yelp_data_set/cass_yelp_academic_dataset_review.csv"
OUTPUT_PG_FILE = "yelp_data_set/pg_yelp_academic_dataset_review.csv"

MAX_ROW_COUNT = 4000000
LOG_INTERVAL = 20000


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, encoding='utf-8', format='%(asctime)s | %(levelname)s | %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info("Program started")

    counter = 0
    with open(OUTPUT_CASS_FILE, 'w') as outputFile:
        writer = csv.writer(outputFile, delimiter=",", quoting=csv.QUOTE_NONE, escapechar="\\")
        header = ("review_id", "user_id", "business_id", "stars", "date", "text", "useful", "funny", "cool")
        writer.writerow(header)

        with open(INPUT_FILE) as inputFile:
            for line in inputFile:
                jsonObj = json.loads(line)
                text = jsonObj['text'].replace('\n', '_')
                if text[0] != '"' and text[-1] != '"':
                    text = '"' + text + '"'
                row = (jsonObj['review_id'], jsonObj['user_id'], jsonObj['business_id'], jsonObj['stars'], jsonObj['date'], text, jsonObj['useful'], jsonObj['funny'], jsonObj['cool'])
                writer.writerow(row)
                counter += 1

                if counter % LOG_INTERVAL == 0:
                    logging.info("%s rows written to cassandra CSV file", counter)
                if counter >= MAX_ROW_COUNT:
                    break
    logging.info("Completed writing rows to cassandra CSV file, final count: %s", counter)

    counter = 0
    with open(OUTPUT_PG_FILE, 'w') as outputFile:
        writer = csv.writer(outputFile, delimiter=",", escapechar="\\")
        header = ("review_id", "user_id", "business_id", "stars", "date", "text", "useful", "funny", "cool")
        writer.writerow(header)

        with open(INPUT_FILE) as inputFile:
            for line in inputFile:
                jsonObj = json.loads(line)
                text = jsonObj['text'].replace('\n', '_')
                row = (jsonObj['review_id'], jsonObj['user_id'], jsonObj['business_id'], jsonObj['stars'], jsonObj['date'], text, jsonObj['useful'], jsonObj['funny'], jsonObj['cool'])
                writer.writerow(row)
                counter += 1

                if counter % LOG_INTERVAL == 0:
                    logging.info("%s rows written to PostgreSQL CSV file", counter)
                if counter >= MAX_ROW_COUNT:
                    break

    logging.info("Completed writing rows to PostgreSQL CSV file, final count: %s", counter)

    logging.info("Program ended")


# Cassanra commands
# CREATE KEYSPACE cassdb WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 2};
# DROP TABLE review;
# CREATE TABLE review(review_id text PRIMARY KEY, user_id text, business_id text, stars float, date text, text text, useful float, funny float, cool float);
# COPY review(review_id, user_id, business_id, stars, date, text, useful, funny, cool) FROM '/tmp/yelp/cass_yelp_academic_dataset_review.csv' WITH HEADER = true;
# select count(*) from review;

# Postgres commands
# DROP TABLE review;
# CREATE TABLE IF NOT EXISTS review(review_id VARCHAR(22) PRIMARY KEY, user_id VARCHAR(22), business_id VARCHAR(22), stars float, date timestamp, text text, useful float, funny integer, cool integer);
# COPY review(review_id, user_id, business_id, stars, date, text, useful, funny, cool) FROM '/my-data/pg_yelp_academic_dataset_review.csv' DELIMITER ',' CSV HEADER escape '\';
# select count(*) from review;