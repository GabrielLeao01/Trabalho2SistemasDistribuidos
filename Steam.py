import pika
import hashlib
import sys 
import json
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
import json

def consome_compra():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='direct_loja', exchange_type='direct')
    result = channel.queue_declare('', exclusive=True)
    queue_name = result.method.queue

    key = RSA.generate(1024)
    private_key = key.export_key(format='PEM')
    with open('sprivate_key.pem', 'wb') as f:
        f.write(private_key)
    public_key = key.publickey().export_key(format='PEM')
    with open('spublic_key.pem', 'wb') as f:
        f.write(public_key)

    binding_keys = ['compra']
    for binding_key in binding_keys:
        channel.queue_bind(exchange='direct_loja', queue=queue_name, routing_key=binding_key)

    def callback(ch, method, properties, body):
        print(f"publicação recebida {body}")
        body_str = body.decode('utf-8')
        data = json.loads(body_str)
        message_dict = data["message"]
        message = json.dumps(message_dict) 
    
        signature = bytes.fromhex(data["signature"])
        key = RSA.import_key(open('cpublic_key.pem').read())
        hash_msg = SHA256.new(message.encode('utf-8'))
        try:
            pkcs1_15.new(key).verify(hash_msg, signature)
            print("The signature is valid.")
            message_str = message
            channel.stop_consuming()
            efetiva_compra(message)
        except (ValueError, TypeError):
            channel.stop_consuming()
            print("The signature is not valid.")
        #channel.stop_consuming()
        #efetiva_compra(body)
        
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    print(' Esperando pedidos')
    channel.start_consuming()

def efetiva_compra(msg):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='direct_loja', exchange_type='direct')
    #msg = "Compra realizada com sucesso, seu jogo esta na biblioteca"
    private_key = RSA.import_key(open('sprivate_key.pem').read())
    hash_msg = SHA256.new(msg.encode('utf-8'))
    signature = pkcs1_15.new(private_key).sign(hash_msg)
    signature_hex = signature.hex()
    payload = json.dumps({"message": msg, "signature": signature_hex})
    channel.basic_publish(exchange='direct_loja', routing_key='envio', body=payload)


consome_compra()