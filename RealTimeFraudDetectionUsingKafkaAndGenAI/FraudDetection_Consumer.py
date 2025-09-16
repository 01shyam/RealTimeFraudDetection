import streamlit as st
import pandas as pd
from time import sleep
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from FraudDetection_GenAIBase import fraud_detect

# Streamlit app title
st.title("Real-Time Fraud Detection Dashboard")

# Placeholder to update table dynamically
placeholder = st.empty()

# Kafka & Schema Registry configs (your existing configs)
kafka_config = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'JADTAZYAET5ARBWE',
    'sasl.password': 'cfltAxiwyCD2pWXKVZQH9RiPExJ2UazW34OiRvnnhDdXmbapYIjAFIzv0jV7RjYw',
    'group.id': 'G1',
    'auto.offset.reset': 'earliest'
}

schema_registry_client = SchemaRegistryClient({
    'url': 'https://psrc-4x67ewe.us-east1.gcp.confluent.cloud',
    'basic.auth.user.info': '{}:{}'.format('THTFWIERVD3FE7YD', 'cflttZMC3bOS05j0Q1IXpj+h1URWiTxx2JfS4u9euIwVOupIrSwWRD4PLWrfKKIg')
})

subject_name = 'Transaction_Table-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
})

consumer.subscribe(['Transaction_Table'])

# Create a DataFrame to store streaming data
stream_df = pd.DataFrame()

# Kafka consumer loop with Streamlit display
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Call your fraud detection function
        result = fraud_detect(msg.value())

        # Convert result to DataFrame (if it's dict)
        if isinstance(result, dict):
            result_df = pd.DataFrame([result])
        elif isinstance(result, pd.DataFrame):
            result_df = result
        else:
            result_df = pd.DataFrame([{"result": str(result)}])

        # Append to streaming DataFrame
        stream_df = pd.concat([stream_df, result_df], ignore_index=True)

        # Update Streamlit table dynamically
        placeholder.dataframe(stream_df)

        # Small delay for UI refresh
        sleep(0.5)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
