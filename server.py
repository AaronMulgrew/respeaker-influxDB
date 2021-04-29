import socket
import sys
import datetime
from influxdb import InfluxDBClient
import json
from statistics import mean


# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the socket to the port
server_address = ('localhost', 9001)
print('starting up on {} port {}'.format(*server_address))
sock.bind(server_address)

# connect to influxDB
client = InfluxDBClient(host='localhost', port=8086)
client.create_database('python_timeseries')
# Create a default retention policy on InfluxDB
client.create_retention_policy('awesome_policy', '3d',1, database='python_timeseries', default=True)


# Listen for incoming connections
sock.listen(1)
while True:
    # Wait for a connection
    print('waiting for a connection')
    connection, client_address = sock.accept()
    try:
        print('connection from', client_address)
        batch = []

        # Receive the data and batch load it to a local array before sending to InfluxDB

        while True:
            data = connection.recv(3096)
            if not data:
                break
            # Get the current time for upload
            current_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            try:
                # load the raw JSON
                json_data = json.loads(data)
                # load the Energy values from each microphone
                all_mic_values = [json_data['src'][0]['E'], json_data['src'][1]['E'], json_data['src'][2]['E'], json_data['src'][3]['E']]
                # get the overall average of all microphone values
                avg = mean(all_mic_values)
                # add to list JSON body with acceptable tags and measurement values
                json_body = {
                    "measurement": "noise",
                    "time": current_time,
                    "tags": {
                        "host": "server01",
                    },
                    "fields": {
                        "Mic1": all_mic_values[0],
                        "Mic2": all_mic_values[1],
                        "Mic3": all_mic_values[2],
                        "Mic4": all_mic_values[3],
                        "Avg": avg
                    }
                }


                batch.append(json_body)
            except:
                continue
            if len(batch) > 100:
                # write all the batch items to the influxDB
                # batch in 100 chunks for efficency
                client.write_points(batch, database='python_timeseries', protocol='json')
                batch = []

    finally:
        # Clean up the connection
        connection.close()
