"""Google Cloud Function to store Things uplink data into Google BigQuery.
"""
from google.cloud import bigquery
import functions_framework

@functions_framework.http
def store_in_bq(request):

    request_json = request.get_json(silent=True)



    # Construct a BigQuery client object.
    client = bigquery.Client()

    # rows_to_insert = [
    #     {"full_name": "Phred Phlyntstone", "age": 32},
    #     {"full_name": "Wylma Phlyntstone", "age": 29},
    # ]

    # errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    # if errors == []:
    #     print("New rows have been added.")
    # else:
    #     print("Encountered errors while inserting rows: {}".format(errors))