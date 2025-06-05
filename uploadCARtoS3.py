import boto3
from datetime import datetime
import os
import sys
import time


def get_date_from_args():
    if len(sys.argv) > 1:
        try:
            # Try to parse the date argument
            date = datetime.strptime(sys.argv[1], "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            print("Invalid date format. Use YYYY-MM-DD.")
            sys.exit(1)
    else:
        # Use current date if no argument is provided
        date = datetime.now().strftime("%Y-%m-%d")
    return date


rep_date = get_date_from_args()



def upload_csv_to_s3(file_name, bucket_name, bucket_sub, aws_region="us-east-1"):
    """
    Uploads a CSV file to an S3 bucket, creating a folder for the current date if it doesn't exist.

    :param file_path: Path to the CSV file to upload.
    :param bucket_name: Name of the S3 bucket.
    :param aws_region: AWS region of the S3 bucket (default: us-east-1).
    """
    # Initialize S3 client
    s3_client = boto3.client("s3", region_name=aws_region)

    # Get the current date in YYYY-MM-DD format
    #current_date = datetime.now().strftime("%Y-%m-%d")
    rep_date = get_date_from_args()


    # Extract the file name from the file path
    #file_name = os.path.basename(file_path)

    # Define the S3 key (folder + file name)
    s3_key = f"{bucket_sub}/rep_date={rep_date}/{file_name}"

    filewithpath = "/tmp/" + file_name
    try:
        # Upload the file to S3
        s3_client.upload_file(filewithpath, bucket_name, s3_key)
        print(f"File '{file_name}' uploaded to S3 bucket '{bucket_name}' in folder '{s3_key}'.")
    except Exception as e:
        print(f"Failed to upload file to S3: {e}")


def repair_athena_table(database, table, output_location, aws_region="us-east-1"):
    """
    Runs MSCK REPAIR TABLE on the specified Athena table to refresh partitions.
    """
    athena = boto3.client("athena", region_name=aws_region)
    query = f"MSCK REPAIR TABLE {table}"
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
    )
    query_execution_id = response["QueryExecutionId"]

    # Optionally wait for completion
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            print(f"Repair table query finished with state: {state}")
            break
        time.sleep(2)


# Main script
if __name__ == "__main__":
    s3bucket = "bankleaflevel"
    upload_csv_to_s3("customers.csv", s3bucket, 'customers')
    upload_csv_to_s3("accounts.csv", s3bucket, 'accounts')
    upload_csv_to_s3("relationships.csv", s3bucket, 'relationships')


    athena_db = "bankleaflevel"
    athena_table = "accounts"
    athena_output = "s3://bankleaflevelqueryoutput/queryoutput/"
    repair_athena_table(athena_db, athena_table, athena_output)


    athena_db = "bankleaflevel"
    athena_table = "customer"
    athena_output = "s3://bankleaflevelqueryoutput/queryoutput/"
    repair_athena_table(athena_db, athena_table, athena_output)

    athena_db = "bankleaflevel"
    athena_table = "relationships"
    athena_output = "s3://bankleaflevelqueryoutput/queryoutput/"
    repair_athena_table(athena_db, athena_table, athena_output)

    