from confluent_kafka import Consumer

# Configuración del Consumer
consumer_conf = {
    'bootstrap.servers': 'kafka-nodo02:9092',
    'group.id': 'python-consumer-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)

# Suscribirse al tópico
consumer.subscribe(['demo-topic'])

print("📡 Escuchando mensajes... (Ctrl+C para salir)")
try:
    while True:
        msg = consumer.poll(1.0)  # Espera hasta 1 segundo por un mensaje
        if msg is None:
            continue
        if msg.error():
            print(f' Error: {msg.error()}')
        else:
            print(f' Recibido: {msg.value().decode("utf-8")}')
except KeyboardInterrupt:
    print("\n Consumer detenido.")
finally:
    consumer.close()

