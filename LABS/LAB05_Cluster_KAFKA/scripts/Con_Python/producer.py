from confluent_kafka import Producer

# Configuración del Producer
producer_conf = {
    'bootstrap.servers': 'kafka-nodo01:9092'
}
producer = Producer(producer_conf)

# Función de callback para confirmar entrega
def delivery_report(err, msg):
    if err is not None:
        print(f' Error al enviar: {err}')
    else:
        print(f' Enviado: {msg.value().decode("utf-8")} a {msg.topic()} [{msg.partition()}]')

# Enviar mensajes
for i in range(5):
    msg = f'Hola Kafka desde Python #{i}'
    producer.produce('demo-topic', msg.encode('utf-8'), callback=delivery_report)
    producer.poll(0)  # Procesa mensajes de confirmación pendientes

producer.flush()  # Espera que todos los mensajes sean enviados

