from decimal import *
from time import sleep
from uuid import uuid4, UUID
import time
import pandas as pd
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
count=0

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    print("=====================")



#define kafka config
kafka_config={
        'bootstrap.servers': 'pkc-613.us-east1.gcp.confluent.cloud:9092',
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': 'JADTAZARBWE',
        'sasl.password': 'cfltAxiwyCD2pWXKVZQxJ2UazW34OiRvnnhDdXmbapYIjAFIzv0jV7RjYw',

 }

# Creating Schema Registry Client
schema_registry_client=SchemaRegistryClient({
        'url': 'https://psrc-4x67.us-east1.gcp.confluent.cloud',
        'basic.auth.user.info' : '{}:{}'.format('THTFWIERVDE7YD','cflttZMC3bOXpj+h1URWiTxx2JfS4u9euIwVOupIrSwWRD4PLWrfKKIg')
    })


# Getting the data Contract of Value
subject_name='Transaction_Table-value'
schema_str=schema_registry_client.get_latest_version(subject_name).schema.schema_str
print("schema from registry ===========")
print(schema_str)
print("================================")

# creating Avro Serializer for the value
key_serializer=StringSerializer('utf_8')
# for producing the value
avro_serializer=AvroSerializer(schema_registry_client,schema_str)

#define the SerilazingProducer

producer= SerializingProducer({
        'bootstrap.servers': kafka_config['bootstrap.servers'],
        'security.protocol': kafka_config['security.protocol'],
        'sasl.mechanisms': kafka_config['sasl.mechanisms'],
        'sasl.username': kafka_config['sasl.username'],
        'sasl.password': kafka_config['sasl.password'],
        'key.serializer': key_serializer, #key will be serialized as string
        'value.serializer': avro_serializer, #value will be serialized as Avro
})

df = pd.read_excel("Fraud_TestData.xlsx")

# print(df.head())

for index,row in df.iterrows():
        # Create a dictionary from the row_values
        data_value=row.to_dict()
        # data_value["Transaction_Time"]= int(pd.Timestamp(data_value["Transaction_Time"]).timestamp() * 1000)
        print(data_value)
        # Produce this data to Kafka
        producer.produce(
            topic='Transaction_Table',
            key=str(row['Transaction_ID']),
            value=data_value,
            on_delivery=delivery_report,
        )
        producer.flush()
        time.sleep(3)


print(f"All Data successfully published to Kafka and number of records: {count}")





