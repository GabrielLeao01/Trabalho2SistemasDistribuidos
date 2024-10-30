import pika
import hashlib
import sys
import json
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
import json

dados_do_jogo = {
    "nome_do_jogo": "Red Dead Redemption 2",
    "preco_do_jogo": 139.99,
    "forma_de_pagamento": "Cartao de Credito",
    "status_pagamento": "Aprovado",
    "data_da_compra": "2024-10-29"
}

key = RSA.generate(1024)
private_key = key.export_key(format='PEM')
with open('private_key.pem', 'wb') as f:
    f.write(private_key)
public_key = key.publickey().export_key(format='PEM')
with open('public_key.pem', 'wb') as f:
    f.write(public_key)

dados_json = json.dumps(dados_do_jogo)
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='direct_loja', exchange_type='direct')


routing_key = 'compra'
message = 'Hello world'

def enviarEvento():
    print(dados_do_jogo)
    private_key = RSA.import_key(open('private_key.pem').read())
    hash_msg = SHA256.new(json.dumps(dados_do_jogo).encode('utf-8'))
    signature = pkcs1_15.new(private_key).sign(hash_msg)
    payload = json.dumps({"message": dados_do_jogo, "signature": signature.hex()})
    channel.basic_publish(exchange='direct_loja', routing_key='compra', body=payload)
    print(f'evento enviado, mensagem: {dados_do_jogo}, rota: {"compra"}')
enviarEvento()



result = channel.queue_declare('', exclusive=True)
queue_name = result.method.queue

binding_keys = ['venda']
for binding_key in binding_keys:
    channel.queue_bind(exchange='direct_loja', queue=queue_name, routing_key=binding_key)

print(' [*] Waiting for logs. To exit press CTRL+C')

def callback(ch, method, properties, body):
    data = json.loads(body)
    print(data)
    message = data['message'].encode('utf-8')
    print(f"Muito obrigado, sua compra foi aprovada, dados de compra:\n {json.dumps(data['message'])}")
    signature = bytes.fromhex(data["signature"])
    key = RSA.import_key(open('public_key.pem').read())
    hash_msg = SHA256.new(message)
    try:
        pkcs1_15.new(key).verify(hash_msg, signature)
        print("The signature is valid.")
      


    except (ValueError, TypeError):
        print("The signature is not valid.")

channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()

