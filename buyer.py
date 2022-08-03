import json
import time

from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_list"
ORDER_LIMIT = 100

producer = KafkaProducer(bootstrap_servers="localhost:29092")

print("Going to be generating order after 10 seconds")
print("Will generate one unique order every 10 seconds")
time.sleep(5)

for i in range(1, ORDER_LIMIT):
    data = {
        "username" : "Felipe, Renato",
        "books" : "Clean Code, Data Science in Python",
    }

    producer.send(ORDER_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))
    print(f"Done Sending..{i}")