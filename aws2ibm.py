"""This script receives trace data from MQTT by subscribing to a topic"""
import json
from argparse import ArgumentParser
from paho.mqtt.client import Client as MqttClient
import datetime
import os
import sys
import ssl


def run():
    """Main method that parses command options and executes the rest of the script"""

    # create a client
    client_out = create_client_out(
        host=os.environ["MQTT_HOST"],
        port=int(os.environ["MQTT_PORT"]),
        username=os.environ["MQTT_USERNAME"],
        password=os.environ["MQTT_PASSWORD"],
        clientid=os.environ["MQTT_CLIENTID"] + "_rec_mx",
        cafile=os.environ["MQTT_CERT"],
    )

    client_aws = create_client_aws(
        protocol = "x-amzn-mqtt-ca",
        endpoint = os.environ["AWS_MQTT_ENDPOINT"],
        ca = os.environ["AWS_MQTT_CA"],
        cert = os.environ["AWS_MQTT_CERT"],
        private = os.environ["AWS_MQTT_PRIVATE_KEY"],
        port = int(os.environ["AWS_MQTT_PORT"]),
        client_out = client_out
    )

    client_aws.loop_forever()
    # client_out.loop_forever()

def create_client_aws(protocol, endpoint, ca, cert, private, port, client_out):
    """Creating an MQTT Client Object"""
    client = MqttClient(userdata=client_out)

    ssl_context = ssl.create_default_context()
    ssl_context.set_alpn_protocols([protocol])
    ssl_context.load_verify_locations(cafile=ca)
    ssl_context.load_cert_chain(certfile=cert, keyfile=private)
    client.tls_set_context(context=ssl_context)

    client.on_connect = on_connect_aws
    client.on_message = on_message_aws

    client.connect(endpoint, port=port)
    return client

def on_message_aws(client, userdata, message):
    """When a message is sent to a subscribed topic,
    decode the message and send it to another method"""
    try:
        decoded_message = str(message.payload.decode("utf-8", "ignore"))

        topic = "iot-2/type/OpenEEW/id/MX/evt/status/fmt/json"

        userdata.publish(topic, decoded_message)

    except BaseException as exception:
        print(exception)

def on_connect_aws(client, userdata, flags, resultcode):
    """Upon connecting to an MQTT server, subscribe to the topic"""

    topic = "grillo-openeew/traces/+"
    print(f"✅ Subscribed to sensor data with result code {resultcode}")
    client.subscribe(topic)

def create_client_out(host, port, username, password, clientid, cafile=None):
    """Creating an MQTT Client Object"""
    client = MqttClient(clientid)

    if username and password:
        client.username_pw_set(username=username, password=password)

    try:
        client.tls_set(ca_certs=cafile)
    except:
        print("Proceeding without certificate file")

    client.connect(host=host, port=port)

    print(f"✅ Redirecting sensor data to another mqtt")
    return client

if __name__ == '__main__':

    run()