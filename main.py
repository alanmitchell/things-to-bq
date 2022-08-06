"""Google Cloud Function to store Things uplink data into Google BigQuery.
"""
import yaml
import os
import base64

from google.cloud import bigquery
import functions_framework
from flask import abort

from decode import decode_elsys, decode_dragino, decode_e5

# This loads the API key to Source dictionary into 'store_keys'
store_keys = yaml.safe_load(os.environ['API_KEYS'])

@functions_framework.http
def store_in_bq(request):

    store_key = request.headers.get('store-key', None)
    if store_key is None:
        abort(400)

    if store_key not in store_keys:
        abort(404)

    # use the store key to determine the data source
    source = store_keys[store_key]

    request_json = request.get_json(silent=True)

    # Construct a BigQuery client object.
    client = bigquery.Client()

    msg = ''
    try:
        gtw_recs = get_gateway_recs(request_json, source)
        table_id = 'an-projects.things.gateway_reception'
        errors = client.insert_rows_json(table_id, gtw_recs)  
        if errors == []:
            msg += f"{len(gtw_recs)} gateway reception rows have been added. "
        else:
            msg += f"Encountered errors while inserting gateway reception rows: {errors}. "

    except BaseException as err:
        print(err)

    try:
        payload_rec = get_payload_rec(request_json, source)
        table_id = 'an-projects.things.payload'
        errors = client.insert_rows_json(table_id, [payload_rec])  
        if errors == []:
            msg += "Payload record successfully added. "
        else:
            msg += f"Encountered errors while inserting payload record: {errors}. "

    except BaseException as err:
        print(err)

    return msg

def get_gateway_recs(rec, source):

    dr = rec['uplink_message']['settings']['data_rate']['lora']
    data_rate = f"SF{dr['spreading_factor']}BW{dr['bandwidth'] // 1000}"

    # add to list of gateway records
    gtw_recs = []
    for gtw in rec['uplink_message']['rx_metadata']:
        r = {
            'ts': rec['received_at'],
            'source': source,
            'device': rec['end_device_ids']['device_id'],
            'gateway': gtw['gateway_ids']['gateway_id'],
            'counter': rec['uplink_message']['f_cnt'] if 'f_cnt' in rec['uplink_message'] else None,
            'snr': gtw['snr'] if 'snr' in gtw else None,
            'rssi': gtw['rssi'] if 'rssi' in gtw else None,
            'data_rate': data_rate,
        }

        gtw_recs.append(r)

    return gtw_recs

def get_payload_rec(rec, source):

    port = rec['uplink_message']['f_port']
    device_id = rec['end_device_ids']['device_id']
    payload = rec['uplink_message']['frm_payload']

    payload_binary = base64.b64decode(payload)
    vbat = decode_battery_voltage(port, device_id, payload_binary)

    return {
        'ts': rec['received_at'],
        'source': source,
        'device': device_id,
        'payload': payload,
        'vbat': vbat,
    }

def decode_battery_voltage(port, device_id, payload):

    fields = []      # default to no field data
    dev_id_lwr = device_id.lower()    # get variable for lower case device ID

    # Note that many of the decoder functions below return the sensor fields as a dictionary.
    # Both a field dictionary and a field list can be accommodated.  Further down in this
    # function, a field dictionary is converted to a list of tuples.
    try:
        # dispatch to the right decoding function based on characters in the device_id.
        # if device_id contains "lht65" anywhere in it, use the lht65 decoder
        # if device_id starts with "ers" or "elsys" or "elt", use the elsys decoder
        if 'lht65' in dev_id_lwr:
            # only messages on Port 2 are sensor readings (although haven't yet seen 
            # any other types of messages from this sensor)
            if port == 2:
                fields = decode_dragino.decode_lht65(payload)
        elif dev_id_lwr.startswith('lwl01'):
            fields = decode_dragino.decode_lwl01(payload)
        elif dev_id_lwr.startswith('elsys') or (dev_id_lwr[:3] in ('ers', 'elt')):
            # only messages on Port 5 are sensor readings
            if port == 5:
                fields = decode_elsys.decode(payload)
        elif dev_id_lwr.startswith('boat-lt2'):
            if port == 2:
                fields = decode_dragino.decode_boat_lt2(payload)
        elif dev_id_lwr.startswith('ldds'):
            if port == 2:
                fields = decode_dragino.decode_ldds(payload)
        elif dev_id_lwr.startswith('lsn50'):
            if port == 2:
                fields = decode_dragino.decode_lsn50(payload)
        elif dev_id_lwr.startswith('e5'):
            if port == 8:
                fields = decode_e5.decode_e5(payload)

    except:
        # Failed at decoding raw payload.  Go on to see if there might be values in 
        # the payload_fields element.
        pass

    # If the 'fields' variable is a dictionary, convert it to a list of tuples at this point.
    if type(fields) == dict:
        fields = list(fields.items())

    vdd = None
    for sensor_id, val in fields:
        if sensor_id.endswith('vdd'):
            vdd = val
            break

    return vdd
