#!/bin/bash

CONTROLADOR=$1  # 1 = Zookeeper, 2 = KRaft
ROL=$2          # producer o consumer
MENSAJES=300

# Detectar número de nodo por IP
IP=$(hostname -I | awk '{print $1}')
case "$IP" in
  172.31.70.116) BROKER_NUM=1 ;;
  172.31.64.227) BROKER_NUM=2 ;;
  172.31.69.157) BROKER_NUM=3 ;;
  172.31.65.228) BROKER_NUM=4 ;;
  172.31.67.191) BROKER_NUM=5 ;;
  *) BROKER_NUM=0 ;;
esac

KAFKA_BIN="/opt/kafka/kafka/bin"
TOPIC_ZK="demo-topic"
TOPIC_KRAFT="demo-kraft"
BROKER_ZK="kafka-nodo01:9092"
BROKER_KRAFT="kafka-nodo01:9192"

if [ "$CONTROLADOR" == "1" ]; then
    TOPIC=$TOPIC_ZK
    BROKER=$BROKER_ZK
elif [ "$CONTROLADOR" == "2" ]; then
    TOPIC=$TOPIC_KRAFT
    BROKER=$BROKER_KRAFT
else
    echo "Uso: $0 [1=Zookeeper | 2=KRaft] [producer|consumer]"
    exit 1
fi

TMP_FILE="/tmp/${ROL}_${BROKER_NUM}.log"
rm -f "$TMP_FILE"

START=$(date +%s.%N)

if [ "$ROL" == "producer" ]; then
    echo "Producer [$BROKER_NUM] : Creando mensajes:"
    seq $MENSAJES | awk -v bn=$BROKER_NUM '{print "Broker "bn" enviando mensaje número "$1}' \
    | tee "$TMP_FILE" \
    | $KAFKA_BIN/kafka-console-producer.sh --bootstrap-server "$BROKER" --topic "$TOPIC" > /dev/null
else
    echo "Consumer [$BROKER_NUM] : Leyendo mensajes:"
    $KAFKA_BIN/kafka-console-consumer.sh --bootstrap-server "$BROKER" \
    --topic "$TOPIC" --from-beginning --timeout-ms 30000 > "$TMP_FILE" 2>/dev/null
fi

END=$(date +%s.%N)
DUR=$(echo "$END - $START" | bc)

# Mostrar primeros 10 mensajes por broker detectado
if [ "$ROL" == "consumer" ]; then
    BROKERS=$(awk '{print $2}' "$TMP_FILE" | grep -E '^[0-9]+$' | sort -u)

    for B in $BROKERS; do
        echo "                : Primeros mensajes de Broker $B:"
        grep "Broker $B " "$TMP_FILE" | head -n 10 | awk '{print "                   : "$0}'
        echo "                   : ..."
    done

    TOTAL=$(wc -l < "$TMP_FILE")
    echo "                :Total mensajes consumidos: $TOTAL"
else
    head -n 15 "$TMP_FILE" | awk '{print "                   > "$0}'
    echo "                   > ..."
fi

# Mostrar tiempo total
echo "                :Tiempo total $ROL de todos los mensajes es $(printf "%.3f" $DUR) segundos."
