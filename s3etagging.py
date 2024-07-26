import boto3
import regex
from urllib.parse import unquote_plus
from botocore.exceptions import ClientError
import psycopg2
from psycopg2 import sql
from pymongo import MongoClient, errors
from bson import ObjectId
import threading
import concurrent.futures
import csv

from config import (
    S3_BUCKET_NAME,
    S3_FOLDER_NAME,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    MONGODB_CONNECTION_STRING,
    DATABASE_FUZZY,
    COLLECTION_FUZZY,
)


# Create an S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)


# Create a MongoDB client
try:
    client = MongoClient(MONGODB_CONNECTION_STRING)
    database_sap = client[DATABASE_FUZZY]
    output_collection = database_sap[COLLECTION_FUZZY]
    print("Connection successfull")

    # Perform some operations
    # Example: collection.find_one()
except errors.ServerSelectionTimeoutError as err:
    print(f"Failed to connect to server: {err}")


def s3_fun(invoice_name):
    s3_key = f"{S3_FOLDER_NAME}/{invoice_name}"
    # s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=file_content)
    s3_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
    etag = fetch_etag(s3_url)
    if etag:
        final_url = insert_data(etag, s3_url)
    else:
        print("feiled to fetch etag")
        final_url = "failed to fetch etag"
        with open("Logger.json", "a") as file:
            file.write("\n")
            file.write(f"Invoice Name : {invoice_name} failed to etag")
    return final_url


def parse_s3_url(url):
    try:
        # Handling non-standard S3 URL formats
        match = regex.match(r"https?://([^/.]+)\.amazonaws\.com/(.+)", url)
        if match:
            return match.groups()
        # Handling other URL formats where the bucket name is before the first '.'
        # and the object key is everything after the domain
        match = regex.match(r"https?://([^/.]+)\.([^/]+)/(.+)", url)
        if match:
            bucket_name, _, object_key = match.groups()
            return bucket_name, object_key
        raise ValueError(f"Invalid S3 URL format: {url}")
    except regex.error as e:
        print(f"Regex error occurred: {e}")
        print(f"Parsing Error, s3_url: {url}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        print(f"Parsing Error, s3_url: {url}")
        return None


def fetch_etag(s3_url):
    try:
        parse_result = parse_s3_url(s3_url)
        if parse_result is None:
            raise ValueError("Failed to parse S3 URL")
        bucket_name, object_key = parse_result
        decoded_object_key = unquote_plus(
            object_key
        )  # Decode URL-encoded characters including +
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION,
        )
        response = s3_client.head_object(Bucket=bucket_name, Key=decoded_object_key)
        return response["ETag"].strip('"')
    except ClientError as e:
        print(
            f"Error {e.response['Error']['Code']} for URL {s3_url}: {e.response['Error']['Message']}"
        )
        return None
    except Exception as e:
        print(f"Unexpected error for URL {s3_url}: {e}")
        return None


def insert_data(etag, s3_url):
    print(f"insetinh for ehtag: {etag} and url:{s3_url}")
    en_url = "https://files.finkraft.ai/" + etag
    return en_url


def process_document(doci, index, total_documents, output_collection, lock):
    invoice_name = doci["BOOKING_DATA"]["InvoiceName"]
    final_url = s3_fun(invoice_name)

    update_filter_criteria = {"_id": ObjectId(doci["_id"])}
    change = {"FinalUrl": final_url}

    output_collection.update_one(update_filter_criteria, {"$set": change})

    # Printing and writing to output.txt
    with lock:
        print(f"Index: {index}/{total_documents} URL Append Successful: {final_url}")

        with open("output.csv", "a", newline="") as csvfile:
            csv_writer = csv.writer(csvfile)
            rowitem = [index, total_documents, invoice_name, final_url]
            csv_writer.writerow(rowitem)


# if __name__ == "__main__":
#     documents = list(output_collection.find())
#     lendocs = len(documents)
#     if documents:
#         print(f"Extracted invoice data and there are {lendocs} invoices.")

#     for index, doci in enumerate(documents):
#         invoice_name = doci["BOOKING_DATA"]["InvoiceName"]
#         final_url = s3_fun(invoice_name)

#         update_filter_criteria = {"_id": ObjectId(doci["_id"])}
#         change = {"FinalUrl": final_url}
#         output_collection.update_one(update_filter_criteria, {"$set": change})

#         print(f"Index : {index}/{len(documents)} URL Append Succesfull : {final_url}")

#         with open("output.txt", "a") as txtfile:
#             txtfile.write(f"Invoice Name : {invoice_name} , file_url : {final_url} \n")


def main(documents, output_collection):
    lock = threading.Lock()
    total_documents = len(documents)
    with open("output.csv", "a", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        rowitem = ["Index", "Total Documents 1", "Invoice Name", "File URL"]
        csv_writer.writerow(rowitem)

    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        try:
            futures = [
                executor.submit(
                    process_document,
                    doci,
                    index,
                    total_documents,
                    output_collection,
                    lock,
                )
                for index, doci in enumerate(documents)
            ]
            concurrent.futures.wait(futures)
        except KeyboardInterrupt:
            print("KeyboardInterrupt caught, attempting to shut down threads.")
            for future in futures:
                future.cancel()  # Attempt to cancel all pending futures
            executor.shutdown(wait=True)  # Wait for all threads to terminate
            print("All threads shut down successfully.")


if __name__ == "__main__":
    query = {"FinalUrl": {"$exists": False}}
    documents = list(output_collection.find(query))
    lendocs = len(documents)
    if documents:
        print(f"Extracted invoice data and there are {lendocs} invoices.")
    main(documents, output_collection)
