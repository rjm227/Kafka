import json
import time
import redis
from datetime import timedelta
from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_list"
ORDER_LIMIT = 100

#### CONECTAR AO CLIENTE DO REDIS ####
redis_client = redis.Redis(host="127.0.0.1", port=6379, db=0)
producer = KafkaProducer(bootstrap_servers="localhost:29092")

print("Going to be generating order after 10 seconds")
print("Will generate one unique order every 10 seconds")
time.sleep(5)

for i in range(1, ORDER_LIMIT):
    cliente_nome   = input("Digite o seu nome: ")
    cliente_pedido = input("Digite o seu pedido: ")

    redis_pedido = redis_client.get(cliente_nome)
    
    #### Se o cliente nao existir, cadastre-o e compre o livro    
    if redis_pedido is None:
        redis_client.set(cliente_nome, json.dumps([{"pedido": cliente_pedido}]))
        # Apos 60 segundos, registro eh deletado 
        redis_client.expire(cliente_nome, timedelta(seconds=60))
        print("Cliente e pedido cadastrados com sucesso! (60 segundos ate expirar)\n")

    #### Se o cliente existir, pegar a lista de pedidos que ja fez e adicionar o pedido atual
    else:
        new_pedido = redis_pedido.decode("utf-8")
        lista_pedidos = eval(new_pedido)
        lista_pedidos.append({"pedido": cliente_pedido})
        print(lista_pedidos)
        redis_client.set(cliente_nome, json.dumps(lista_pedidos))
        # Apos 45 segundos, registro eh deletado 
        redis_client.expire(cliente_nome, timedelta(seconds=45))
        print("Pedido adicionado com sucesso! (45 segundos ate expirar)\n")

    data = {
        "username" : cliente_nome,
        "books" : cliente_pedido,
    }

    producer.send(ORDER_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))
    print(f"Done Sending..{i}")