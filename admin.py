import json
import time
import redis
from datetime import timedelta
from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_list"

#### CONECTAR AO CLIENTE DO REDIS ####
redis_client = redis.Redis(host="127.0.0.1", port=6379, db=0)
producer = KafkaProducer(bootstrap_servers="localhost:29092")

print("Going to be generating order after 10 seconds")
print("Will generate one unique order every 10 seconds")
time.sleep(5)

while True:
    admin_order = input("C (para adicionar um cliente), V (para visualizar os pedidos de um cliente) e D (para excluir um cliente): ")

    if(admin_order == "C"):
        cliente_nome   = input("Digite o nome do cliente: ")
        cliente_pedido = input("Digite o pedido do cliente: ")
        redis_pedido = redis_client.get(cliente_nome)
        
        #### Se o cliente nao existir, cadastre-o com o pedido   
        if redis_pedido is None:
            redis_client.set(cliente_nome, json.dumps([{"pedido": cliente_pedido}]))
            redis_client.expire(cliente_nome, timedelta(seconds=999))
            print("Cliente e pedido cadastrados com sucesso!\n")

        else:
            print("Erro: cadastro ja existente!\n")

    elif(admin_order == "V"):
        cliente_nome = input("Digite o nome do cliente: ")
        redis_pedido = redis_client.get(cliente_nome)
        
        #### Se o cliente nao existir, retorne erro de nao encontrado 
        if redis_pedido is None:
            print("Erro: cadastro nao existente!\n")

        else:
            print(redis_pedido.decode("utf-8"))

    elif(admin_order == "D"):
        cliente_nome = input("Digite o nome do cliente: ")
        redis_pedido = redis_client.get(cliente_nome)
        
        #### Se o cliente nao existir, retorne erro de nao encontrado 
        if redis_pedido is None:
            print("Erro: cadastro nao existente!\n")

        else:
            redis_client.delete(cliente_nome)
            print("Cliente deletado com sucesso!\n")

    else:
        print("Comando invalido!\n")