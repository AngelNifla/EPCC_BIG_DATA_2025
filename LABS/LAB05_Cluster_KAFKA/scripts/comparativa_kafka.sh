#!/bin/bash

CONTROLADOR=$1  # 1 = Zookeeper, 2 = KRaft
TOPIC_ZK="demo-topic"
TOPIC_KRAFT="demo-kraft"
MENSAJES=300
RESULTS_FILE="resultados_comparacion"

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

BROKER_LIST_ZK="kafka-nodo01:9092,kafka-nodo02:9092,kafka-nodo03:9092,kafka-nodo04:9092,kafka-nodo05:9092"
BROKER_LIST_KRAFT="kafka-nodo01:9192,kafka-nodo02:9192,kafka-nodo03:9192,kafka-nodo04:9192,kafka-nodo05:9192"

KAFKA_BIN="/opt/kafka/kafka/bin"

log() {
    echo -e "\n==========================================="
    echo "[INFO] $1"
}

mostrar_lideres() {
    broker_list=$1
    topic=$2

    log "== Estado de líderes actuales =="
    $KAFKA_BIN/kafka-topics.sh --bootstrap-server "$broker_list" --describe --topic "$topic" | grep "Partition"
}

crear_topico() {
    topic=$1
    broker_list=$2

    log "Creando tópico '$topic'..."
    START_TOPIC=$(date +%s.%N)
    $KAFKA_BIN/kafka-topics.sh --create \
    --bootstrap-server "$broker_list" \
    --replication-factor 3 --partitions 10 --topic "$topic" 2>/dev/null
    END_TOPIC=$(date +%s.%N)
    DUR_TOPIC=$(echo "$END_TOPIC - $START_TOPIC" | bc)

    if [ $? -ne 0 ]; then
        echo "Created topic $topic (ya existía)"
    else
        echo "Created topic $topic."
    fi
    log "Tópico creado en $DUR_TOPIC segundos."
    echo "$DUR_TOPIC" > /tmp/tiempo_creacion_topico.log
}

esperar_confirmacion() {
    read -p "[CONFIRMAR] ¿Los productores y consumidores ya están activos en otros nodos? (y/n): " ready
    if [[ "$ready" != "y" ]]; then
        log "Por favor inicia los scripts auxiliares en los otros nodos y vuelve a ejecutar."
        exit 1
    fi
}

producir_mensajes() {
    topic=$1
    broker_list=$2

    log "Produciendo $MENSAJES mensajes en $topic..."

    TMP_FILE="/tmp/enviados.log"
    rm -f "$TMP_FILE"

    START=$(date +%s.%N)
    seq $MENSAJES | awk -v bn=$BROKER_NUM '{print "Broker "bn"  enviando mensaje número "$1}' \
    | tee "$TMP_FILE" \
    | $KAFKA_BIN/kafka-console-producer.sh --bootstrap-server "$broker_list" --topic "$topic" > /dev/null
    END=$(date +%s.%N)
    DUR=$(echo "$END - $START" | bc)

    head -n 10 "$TMP_FILE" | awk '{print "                   > "$0}'
    echo "                   > ..."

    log "Mensajes producidos en $DUR segundos."
    echo "$DUR" > /tmp/tiempo_produccion.log
}

consumir_mensajes() {
    topic=$1
    broker_list=$2

    log "Consumiendo mensajes de $topic..."

    TMP_FILE="/tmp/recibidos.log"
    rm -f "$TMP_FILE"

    START=$(date +%s.%N)
    $KAFKA_BIN/kafka-console-consumer.sh --bootstrap-server "$broker_list" \
    --topic "$topic" --from-beginning --timeout-ms 30000 > "$TMP_FILE" 2>/dev/null
    END=$(date +%s.%N)
    DUR=$(echo "$END - $START" | bc)

    head -n 10 "$TMP_FILE" | awk '{print "                   : "$0}'
    echo "                   : ..."

    TOTAL=$(wc -l < "$TMP_FILE")
    echo "Processed a total of $TOTAL messages"
    log "Mensajes consumidos en $DUR segundos."
    echo "$DUR" > /tmp/tiempo_consumo.log
}

medir_failover() {
    broker_list=$1
    topic=$2
    TOTAL_FAIL=0
    TOTAL_REJOIN=0
    COUNT=0

    while true; do
        mostrar_lideres "$broker_list" "$topic"
        read -p "[ACTION] Detén manualmente un broker líder y presiona ENTER para continuar..."

        START_FAIL=$(date +%s.%N)
        $KAFKA_BIN/kafka-topics.sh --bootstrap-server "$broker_list" --describe --topic "$topic" > /dev/null
        END_FAIL=$(date +%s.%N)
        DUR_FAIL=$(echo "$END_FAIL - $START_FAIL" | bc)
        log "Failover detectado en $DUR_FAIL segundos."
        TOTAL_FAIL=$(echo "$TOTAL_FAIL + $DUR_FAIL" | bc)

        mostrar_lideres "$broker_list" "$topic"

        read -p "[ACTION] Reactiva el broker detenido y presiona ENTER para verificar reintegración al ISR..."
        START_REJOIN=$(date +%s.%N)
        while true; do
            ISR=$($KAFKA_BIN/kafka-topics.sh --bootstrap-server "$broker_list" --describe --topic "$topic" | grep "Isr:")
            if [[ $ISR == *"$BROKER_NUM"* ]]; then
                END_REJOIN=$(date +%s.%N)
                DUR_REJOIN=$(echo "$END_REJOIN - $START_REJOIN" | bc)
                log "Broker reintegrado al ISR en $DUR_REJOIN segundos."
                TOTAL_REJOIN=$(echo "$TOTAL_REJOIN + $DUR_REJOIN" | bc)
                break
            fi
            sleep 2
        done

        mostrar_lideres "$broker_list" "$topic"
        ((COUNT++))

        read -p "¿Deseas detener otro broker? (y/n): " choice
        if [[ "$choice" != "y" ]]; then
            break
        fi
    done

    PROMEDIO_FAIL=$(echo "$TOTAL_FAIL / $COUNT" | bc -l)
    PROMEDIO_REJOIN=$(echo "$TOTAL_REJOIN / $COUNT" | bc -l)
    log "Promedio failover: $PROMEDIO_FAIL segundos."
    log "Promedio reintegración ISR: $PROMEDIO_REJOIN segundos."

    echo "$PROMEDIO_FAIL" > /tmp/tiempo_failover.log
    echo "$PROMEDIO_REJOIN" > /tmp/tiempo_rejoin.log
}

# Determinar nombre del controlador
if [ "$CONTROLADOR" == "1" ]; then
    CONTROLADOR_NAME="Zookeeper"
    topic=$TOPIC_ZK
    broker_list=$BROKER_LIST_ZK
elif [ "$CONTROLADOR" == "2" ]; then
    CONTROLADOR_NAME="KRaft"
    topic=$TOPIC_KRAFT
    broker_list=$BROKER_LIST_KRAFT
else
    echo "Uso: $0 [1=Zookeeper | 2=KRaft]"
    exit 1
fi

log "Iniciando pruebas con $CONTROLADOR_NAME"
crear_topico "$topic" "$broker_list"
esperar_confirmacion
producir_mensajes "$topic" "$broker_list"
consumir_mensajes "$topic" "$broker_list"
medir_failover "$broker_list" "$topic"
log "Pruebas finalizadas para $CONTROLADOR_NAME"

# Guardar resultados en archivo
{
    echo "== Resultados de la Comparación =="
    echo "Controlador: $CONTROLADOR_NAME"
    echo "Tópico: $topic"
    echo "Mensajes producidos: $MENSAJES"
    echo "Tiempo creación tópico: $(cat /tmp/tiempo_creacion_topico.log) segundos"
    echo "Tiempo producción: $(cat /tmp/tiempo_produccion.log) segundos"
    echo "Mensajes consumidos: $MENSAJES"
    echo "Tiempo consumo: $(cat /tmp/tiempo_consumo.log) segundos"
    echo "Promedio failover: $(cat /tmp/tiempo_failover.log) segundos"
    echo "Promedio reintegración ISR: $(cat /tmp/tiempo_rejoin.log) segundos"
} > "$RESULTS_FILE"
