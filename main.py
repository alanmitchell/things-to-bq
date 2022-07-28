"""Google Cloud Function to store Things uplink data into Google BigQuery.
"""
from google.cloud import bigquery
import functions_framework
from flask import abort

store_keys = {
    '12345': 'AN'
}

@functions_framework.http
def store_in_bq(request):

    store_key = request.headers.get('store-key', None)
    if store_key is None:
        abort(400)

    if store_key not in store_keys:
        abort(404)

    request_json = request.get_json(silent=True)

    gtw_recs = get_gateway_recs(request_json, store_key)

    return str(gtw_recs)

    # Construct a BigQuery client object.
    #client = bigquery.Client()

    # rows_to_insert = [
    #     {"full_name": "Phred Phlyntstone", "age": 32},
    #     {"full_name": "Wylma Phlyntstone", "age": 29},
    # ]

    # errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    # if errors == []:
    #     print("New rows have been added.")
    # else:
    #     print("Encountered errors while inserting rows: {}".format(errors))

def get_gateway_recs(rec, store_key):

    dr = rec['uplink_message']['settings']['data_rate']['lora']
    data_rate = f"SF{dr['spreading_factor']}BW{dr['bandwidth'] // 1000}"
    payload = rec['uplink_message']['frm_payload']

    # add to list of gateway records
    gtw_recs = []
    for gtw in rec['uplink_message']['rx_metadata']:
        r = {
            'ts': rec['received_at'],
            'source': store_keys[store_key],
            'device': rec['end_device_ids']['device_id'],
            'gateway': gtw['gateway_ids']['gateway_id'],
            'counter': rec['uplink_message']['f_cnt'],
            'snr': gtw['snr'],
            'rssi': gtw['rssi'],
            'data_rate': data_rate,
        }

        gtw_recs.append(r)
        return gtw_recs

