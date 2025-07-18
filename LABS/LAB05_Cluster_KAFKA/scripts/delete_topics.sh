#!/bin/bash

CONTROLADOR=$1  # 1 = Zookeeper, 2 = KRaft
KAFKA_BIN="/opt/kafka/kafka/bin"
BROKER_LIST_ZK="kafka-nodo01:9092,kafka-nodo02:9092,kafka-nodo03:9092,kafka-nodo04:9092,kafka-nodo05:9092"
BROKER_LIST_KRAFT="kafka-nodo01:9192,kafka-nodo02:9192,kafka-nodo03:9192,kafka-nodo04:9192,kafka-nodo05:9192"

if [ "$CONTROLADOR" == "1" ]; then
    CONTROLADOR_NAME="Zookeeper"
    BROKER_LIST=$BROKER_LIST_ZK
elif [ "$CONTROLADOR" == "2" ]; then
    CONTROLADOR_NAME="KRaft"
    BROKER_LIST=$BROKER_LIST_KRAFT
else
    echo "Uso: $0 [1=Zookeeper | 2=KRaft]"
    exit 1
fi

log() {
    echo -e "\n==========================================="
    echo "[INFO] $1"
}

# Mostrar tópicos existentes
log "Listando tópicos existentes en $CONTROLADOR_NAME..."
TOPICS=$($KAFKA_BIN/kafka-topics.sh --bootstrap-server "$BROKER_LIST" --list)

if [ -z "$TOPICS" ]; then
    echo "[INFO] No hay tópicos en $CONTROLADOR_NAME."
    exit 0
fi

TOPIC_ARRAY=($TOPICS)
for i in "${!TOPIC_ARRAY[@]}"; do
    printf "%3d) %s\n" $((i+1)) "${TOPIC_ARRAY[$i]}"
done

echo -e "\n0) Eliminar TODOS los tópicos"
echo -e "x) Cancelar"

read -p "[SELECCIONA] Ingresa el número del tópico a eliminar o 0 para TODOS: " SELECCION

if [[ "$SELECCION" == "x" || "$SELECCION" == "X" ]]; then
    echo "[INFO] Operación cancelada."
    exit 0
elif [[ "$SELECCION" == "0" ]]; then
    read -p "[CONFIRMAR] ¿Eliminar TODOS los tópicos? (y/n): " CONFIRM_ALL
    if [[ "$CONFIRM_ALL" == "y" ]]; then
        for TOPIC in "${TOPIC_ARRAY[@]}"; do
            log "Eliminando tópico '$TOPIC'..."
            $KAFKA_BIN/kafka-topics.sh --bootstrap-server "$BROKER_LIST" --delete --topic "$TOPIC"
        done
        log "Todos los tópicos eliminados."
    else
        echo "[INFO] Operación cancelada."
        exit 0
    fi
elif [[ "$SELECCION" =~ ^[0-9]+$ ]] && [ "$SELECCION" -ge 1 ] && [ "$SELECCION" -le "${#TOPIC_ARRAY[@]}" ]; then
    TOPIC=${TOPIC_ARRAY[$((SELECCION-1))]}
    read -p "[CONFIRMAR] ¿Eliminar tópico '$TOPIC'? (y/n): " CONFIRM_ONE
    if [[ "$CONFIRM_ONE" == "y" ]]; then
        log "Eliminando tópico '$TOPIC'..."
        $KAFKA_BIN/kafka-topics.sh --bootstrap-server "$BROKER_LIST" --delete --topic "$TOPIC"
        log "Tópico '$TOPIC' eliminado."
    else
        echo "[INFO] Operación cancelada."
        exit 0
    fi
else
    echo "[ERROR] Selección inválida."
    exit 1
fi
