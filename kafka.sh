
#!/usr/bin/env bash

set -euo pipefail

# ======= Estilo =======
if [[ -t 1 ]]; then
  BOLD=$'\033[1m'
  DIM=$'\033[2m'
  RESET=$'\033[0m'
  RED=$'\033[31m'
  GREEN=$'\033[32m'
  YELLOW=$'\033[33m'
  BLUE=$'\033[34m'
  MAGENTA=$'\033[35m'
  CYAN=$'\033[36m'
else
  BOLD=""; DIM=""; RESET=""; RED=""; GREEN=""; YELLOW=""; BLUE=""; MAGENTA=""; CYAN=""
fi
ok()   { printf '%b\n' "${GREEN}✅ %s${RESET}\n" "$*"; }
bad()  { printf '%b\n' "${RED}❌ %s${RESET}\n" "$*"; }
info() { printf '%b\n' "${BLUE}ℹ️  %s${RESET}\n" "$*"; }
title(){ printf '%b\n' "${BOLD}${MAGENTA}▶ %s${RESET}\n" "$*"; }
warn() { printf '%b\n' "${YELLOW}⚠️  %s${RESET}\n" "$*"; }

# ======= Variáveis (sobrescreva via ENV) =======
KAFKA_IMG="${KAFKA_IMG:-registry.redhat.io/amq-streams/kafka-39-rhel9:3.0.1-2}"
ZK_IMG="${KAFKA_IMG:-registry.redhat.io/amq-streams/kafka-39-rhel9:3.0.1-2}"
NET="${NET:-kafka-net}"
BASE_DIR="${BASE_DIR:-$PWD}"

CONF_ZK="${CONF_ZK:-$BASE_DIR/conf/zk/zoo.cfg}"
CONF_DIR_KAFKA="${CONF_DIR_KAFKA:-$BASE_DIR/conf/kafka}"

# configs MINIMAL (1 ZK + 1 broker)
CONF_ZK_MIN="${CONF_ZK_MIN:-$BASE_DIR/conf/zk/zoo-min.cfg}"
CONF_DIR_KAFKA_MIN="${CONF_DIR_KAFKA_MIN:-$BASE_DIR/conf/kafka/min}"

# Tuning e logs (silenciosos por padrão)
KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms512m -Xmx512m}"
KAFKA_GC_LOG_OPTS="${KAFKA_GC_LOG_OPTS:-}"   # vazio = sem ruído de GC
LOG_DIR_ENV="${LOG_DIR_ENV:-/tmp}"

# ======= Helpers =======
usage() {
  cat <<EOF
${BOLD}Uso:${RESET}
  ${CYAN}$0 container-setup${RESET}          ${DIM}# cria rede e volumes (idempotente)${RESET}
  ${CYAN}$0 container-destroy${RESET}        ${DIM}# apaga tudo (containers, volumes, rede)${RESET}

  ${CYAN}$0 start${RESET} [zks|brokers] [--minimal]  ${DIM}# se omitido, sobe zks e brokers (FULL ou MIN)${RESET}
  ${CYAN}$0 stop${RESET}  [zks|brokers]              ${DIM}# se omitido, para brokers e depois zks${RESET}
  ${CYAN}$0 status${RESET}                           ${DIM}# health + modo do cluster (MINIMAL/FULL/MIXED)${RESET}
  ${CYAN}$0 logs${RESET}                             ${DIM}# últimos logs de todos${RESET}

  ${CYAN}$0 zk1|zk2|zk3${RESET} ${YELLOW}start|stop|restart|status|logs${RESET}
  ${CYAN}$0 broker1|broker2|broker3${RESET} ${YELLOW}start|stop|restart|status|logs${RESET}

  ${CYAN}$0 topic_list${RESET}                      ${DIM}# lista tópicos${RESET}
  ${CYAN}$0 topic_create <nome> [partitions] [replication]${RESET}
  ${CYAN}$0 topic_describe <nome>${RESET}
  ${CYAN}$0 produce <topic>${RESET}                  ${DIM}# producer simples (stdin)${RESET}
  ${CYAN}$0 produce_keyed <topic>${RESET}            ${DIM}# producer com chave 'key:value'${RESET}
  ${CYAN}$0 consume <topic> [group]${RESET}          ${DIM}# consumer; se group, entra em CG${RESET}

  ${CYAN}$0 broker_acl_min [usuario]${RESET}      ${DIM}# aplica ACL mínima p/ inter-broker (default: broker)${RESET}
  ${CYAN}$0 broker_acl_reset [usuario]${RESET}    ${DIM}# remove ACL mínima p/ inter-broker${RESET}

${BOLD}Vars:${RESET}
  KAFKA_IMG=${DIM}"$KAFKA_IMG"${RESET}
  ZK_IMG    ${DIM}"$ZK_IMG"${RESET}
  NET       ${DIM}"$NET"${RESET}
  CONF_ZK   ${DIM}"$CONF_ZK"${RESET}
  CONF_DIR_KAFKA ${DIM}"$CONF_DIR_KAFKA"${RESET}
  (modo minimal usa) CONF_ZK_MIN=${DIM}"$CONF_ZK_MIN"${RESET}  CONF_DIR_KAFKA_MIN=${DIM}"$CONF_DIR_KAFKA_MIN"${RESET}
EOF
  exit 1
}

require_file() { [[ -f "$1" ]] || { bad "arquivo não encontrado: $1"; exit 1; }; }
ensure_network() {
  if ! podman network inspect "$NET" >/dev/null 2>&1; then
    title "Criando rede ${NET} 🌐"; podman network create "$NET" >/dev/null; ok "Rede ${NET} criada."
  fi
}

zk_name()         { echo "zk$1"; }
zk_vol_data()     { echo "zk$1-data"; }
zk_vol_log()      { echo "zk$1-log"; }
broker_name()     { echo "kafka$1"; }
broker_vol_data() { echo "kafka$1-data"; }
broker_port()     { case "$1" in 1) echo 19092;; 2) echo 19093;; 3) echo 19094;; esac; }

# Exec silencioso para CLIs
kexec()    { local ctr="$1"; shift; podman exec -e KAFKA_GC_LOG_OPTS= -e KAFKA_HEAP_OPTS="-Xms128m -Xmx128m" "$ctr" bash -lc "$*"; }
kexec_it() { local ctr="$1"; shift; podman exec -it -e KAFKA_GC_LOG_OPTS= -e KAFKA_HEAP_OPTS="-Xms128m -Xmx128m" "$ctr" bash -lc "$*"; }

# ======= SETUP / DESTROY =======
container_setup() {
  title "🔧 Criando volumes e rede"
  ensure_network
  for i in 1 2 3; do
    podman volume create "$(zk_vol_data "$i")" >/dev/null 2>&1 || true
    podman volume create "$(zk_vol_log  "$i")" >/dev/null 2>&1 || true
    podman volume create "$(broker_vol_data "$i")" >/dev/null 2>&1 || true
  done
  ok "Ambiente pronto!"
  info "Garanta os arquivos:
  - ${CONF_ZK} (FULL) e ${CONF_ZK_MIN} (MIN)
  - ${CONF_DIR_KAFKA}/server-[1..3].properties (FULL)
  - ${CONF_DIR_KAFKA_MIN}/server-1.min.properties (MIN)"
}
container_destroy() {
  title "💣 Limpando tudo"
  podman rm -f zk1 zk2 zk3 kafka1 kafka2 kafka3 >/dev/null 2>&1 || true
  for v in \
    "$(zk_vol_data 1)" "$(zk_vol_log 1)" \
    "$(zk_vol_data 2)" "$(zk_vol_log 2)" \
    "$(zk_vol_data 3)" "$(zk_vol_log 3)" \
    "$(broker_vol_data 1)" "$(broker_vol_data 2)" "$(broker_vol_data 3)"
  do podman volume rm -f "$v" >/dev/null 2>&1 || true; done
  podman network rm "$NET" >/dev/null 2>&1 || true
  ok "Tudo removido."
}

# ======= ZOOKEEPER (FULL) =======
zk_start() {
  local id="$1"; ensure_network; require_file "$CONF_ZK"
  local name vdata vlogs; name="$(zk_name "$id")"; vdata="$(zk_vol_data "$id")"; vlogs="$(zk_vol_log "$id")"
  podman rm -f "$name" >/dev/null 2>&1 || true
  local port_flag=""; [[ "$id" == "1" ]] && port_flag="-p 2181:2181"
  title "🦓 Iniciando ZooKeeper ${name}"
  # shellcheck disable=SC2086
  podman run -d --name "$name" --hostname "$name" --network "$NET" \
    $port_flag \
    -e LOG_DIR="$LOG_DIR_ENV" -e KAFKA_GC_LOG_OPTS="$KAFKA_GC_LOG_OPTS" \
    -e KAFKA_OPTS="-Djava.security.auth.login.config=/opt/kafka/config/jaas.conf" \
    -v $BASE_DIR/conf/kafka/jaas.conf:/opt/kafka/config/jaas.conf:Z \
    -v $PWD/conf/kafka/client-broker.properties:/opt/kafka/config/client-broker.properties:Z \
    -v "$vdata:/var/lib/zookeeper/data" \
    -v "$vlogs:/var/lib/zookeeper/log" \
    -v "$CONF_ZK:/opt/kafka/config/zookeeper.properties:Z" \
    "$ZK_IMG" \
    bash -lc "echo $id > /var/lib/zookeeper/data/myid && /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties" >/dev/null
  ok "$name iniciado."
}
# ======= ZK (MINIMAL) =======
zk_start_min() {
  ensure_network; require_file "$CONF_ZK_MIN"
  local name="zk1"
  podman rm -f "$name" >/dev/null 2>&1 || true
  title "🦓 (MIN) Iniciando ZooKeeper ${name}"
  podman run -d --name "$name" --hostname "$name" --network "$NET" \
    -p 2181:2181 \
    -e LOG_DIR="$LOG_DIR_ENV" -e KAFKA_GC_LOG_OPTS="$KAFKA_GC_LOG_OPTS" \
    -e KAFKA_OPTS="-Djava.security.auth.login.config=/opt/kafka/config/jaas.conf" \
    -v $BASE_DIR/conf/kafka/jaas.conf:/opt/kafka/config/jaas.conf:Z \
    -v $PWD/conf/kafka/client-broker.properties:/opt/kafka/config/client-broker.properties:Z \
    -v "$(zk_vol_data 1):/var/lib/zookeeper/data" \
    -v "$(zk_vol_log 1):/var/lib/zookeeper/log" \
    -v "$CONF_ZK_MIN:/opt/kafka/config/zookeeper.properties:Z" \
    "$ZK_IMG" \
    bash -lc "echo 1 > /var/lib/zookeeper/data/myid && /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties" >/dev/null
  ok "$name (minimal) iniciado."
}
zk_stop()   { local n; n="$(zk_name "$1")"; title "Parando ${n} 🛑"; podman rm -f "$n" >/dev/null 2>&1 || true; ok "$n parado."; }
zk_logs()   { title "Logs $(zk_name "$1") 📜"; podman logs -f "$(zk_name "$1")"; }
zk_status() { title "Status $(zk_name "$1") 🔎"; podman ps -a --filter "name=$(zk_name "$1")"; }

# ======= BROKER (FULL) =======
broker_start() {
  local id="$1"; ensure_network
  local name conf port vdata; name="$(broker_name "$id")"; conf="$CONF_DIR_KAFKA/server-$id.properties"; port="$(broker_port "$id")"; vdata="$(broker_vol_data "$id")"
  require_file "$conf"; podman rm -f "$name" >/dev/null 2>&1 || true
  title "🟢 Iniciando Broker ${name} (porta $port)"
  podman run -d --name "$name" --hostname "$name" --network "$NET" \
    -p "$port:9093" \
    -e KAFKA_OPTS="-Djava.security.auth.login.config=/opt/kafka/config/jaas.conf" \
    -v $BASE_DIR/conf/kafka/jaas.conf:/opt/kafka/config/jaas.conf:Z \
    -e LOG_DIR="$LOG_DIR_ENV" \
    -e KAFKA_HEAP_OPTS="$KAFKA_HEAP_OPTS" \
    -e KAFKA_GC_LOG_OPTS="$KAFKA_GC_LOG_OPTS" \
    -v "$vdata:/var/lib/kafka/data" \
    -v "$conf:/opt/kafka/config/server.properties:Z" \
    "$KAFKA_IMG" \
    bash -lc '/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties' >/dev/null
  ok "$name iniciado."
}
# ======= BROKER (MINIMAL) =======
broker_start_min() {
  ensure_network
  local conf="$CONF_DIR_KAFKA_MIN/server-1.min.properties"
  require_file "$conf"
  local name="kafka1"; local port=19092
  podman rm -f "$name" >/dev/null 2>&1 || true
  title "🟢 (MIN) Iniciando Broker ${name} (porta $port)"
  podman run -d --name "$name" --hostname "$name" --network "$NET" \
    -p "$port:9093" \
    -e KAFKA_OPTS="-Djava.security.auth.login.config=/opt/kafka/config/jaas.conf" \
    -v $BASE_DIR/conf/kafka/jaas.conf:/opt/kafka/config/jaas.conf:Z \
    -e LOG_DIR="$LOG_DIR_ENV" \
    -e KAFKA_HEAP_OPTS="$KAFKA_HEAP_OPTS" \
    -e KAFKA_GC_LOG_OPTS="$KAFKA_GC_LOG_OPTS" \
    -v "$(broker_vol_data 1):/var/lib/kafka/data" \
    -v "$conf:/opt/kafka/config/server.properties:Z" \
    "$KAFKA_IMG" \
    bash -lc '/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties' >/dev/null
  ok "$name (minimal) iniciado."
}
broker_stop()   { local n; n="$(broker_name "$1")"; title "Parando ${n} 🛑"; podman rm -f "$n" >/dev/null 2>&1 || true; ok "$n parado."; }
broker_logs()   { title "Logs $(broker_name "$1") 📜"; podman logs -f "$(broker_name "$1")"; }
broker_status() { title "Status $(broker_name "$1") 🔎"; podman ps -a --filter "name=$(broker_name "$1")"; }

# ======= HEALTHCHECK =======
zk_healthcheck() {
  local name; name="$(zk_name "$1")"
  if ! podman container exists "$name" 2>/dev/null; then bad "$name NÃO existe (use: $0 start zks)"; return; fi
  local running; running=$(podman inspect -f '{{.State.Running}}' "$name" 2>/dev/null || echo false)
  if [[ "$running" != "true" ]]; then bad "$name existe mas PARADO ($0 zk$1 start)"; return; fi
  local ip; ip=$(podman inspect -f '{{with index .NetworkSettings.Networks "'"$NET"'"}}{{.IPAddress}}{{end}}' "$name" 2>/dev/null || true)
  if [[ -z "$ip" ]]; then bad "$name rodando mas SEM IP na $NET"; return; fi

  # nc -> /dev/tcp -> TCP aberto (whitelist friendly)
  if kexec "$name" 'command -v nc >/dev/null 2>&1'; then
    local out; out=$(kexec "$name" 'echo ruok | nc -w 1 127.0.0.1 2181' 2>/dev/null || true)
    [[ "$out" == imok* ]] && { ok "$name saudável (ruok)"; return; }
    [[ "$out" == *"not in the whitelist"* ]] && { ok "$name saudável (4lw bloqueado: whitelist)"; return; }
  fi
  if kexec "$name" 'exec 3<>/dev/tcp/127.0.0.1/2181 || exit 2; printf "ruok" >&3; read -t 1 resp <&3 || true; [[ "${resp:-}" == imok* || "${resp:-}" == *"not in the whitelist"* ]]'; then
    ok "$name saudável (TCP ok; 4lw possivelmente bloqueado)"
  else
    if kexec "$name" 'exec 3<>/dev/tcp/127.0.0.1/2181'; then ok "$name porta 2181 aberta (sem 4lw)"; else bad "$name não respondeu na porta 2181"; fi
  fi
}
broker_healthcheck() {
  local name; name="$(broker_name "$1")"
  if kexec "$name" "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092" >/dev/null 2>&1; then
    ok "$name responde API versions"
  else
    bad "$name não respondeu à API"
  fi
}

detect_mode() {
  # FULL: zk1..3 + kafka1..3 running; MINIMAL: apenas zk1 + kafka1 running; MIXED: qualquer outro
  local zk_running=0 br_running=0
  for c in zk1 zk2 zk3; do [[ "$(podman inspect -f '{{.State.Running}}' "$c" 2>/dev/null || echo false)" == "true" ]] && ((zk_running++)); done
  for c in kafka1 kafka2 kafka3; do [[ "$(podman inspect -f '{{.State.Running}}' "$c" 2>/dev/null || echo false)" == "true" ]] && ((br_running++)); done
  if [[ $zk_running -eq 1 && $br_running -eq 1 && "$(podman container exists zk2 && echo 1 || echo 0)" -eq 0 ]]; then
    echo "MINIMAL"
  elif [[ $zk_running -eq 3 && $br_running -eq 3 ]]; then
    echo "FULL"
  else
    echo "MIXED"
  fi
}

status_everything() {
  local mode; mode="$(detect_mode)"
  title "📊 Status geral  ${DIM}(Cluster mode: ${mode})${RESET}"
  printf "\n${BOLD}ZooKeeper:${RESET}\n"
  for i in 1 2 3; do zk_healthcheck "$i"; done
  printf "\n${BOLD}Brokers:${RESET}\n"
  for i in 1 2 3; do broker_healthcheck "$i"; done
}

logs_everything() {
  title "📚 Últimos logs"
  echo "${BOLD}ZooKeepers:${RESET}"; for i in 1 2 3; do echo "—— zk$i ——"; podman logs --tail=20 "$(zk_name "$i")" || true; done
  echo "${BOLD}Brokers:${RESET}";    for i in 1 2 3; do echo "—— kafka$i ——"; podman logs --tail=20 "$(broker_name "$i")" || true; done
}

# ======= TOPICS & CLIENTS =======
topic_list() {
  title "📜 Listando tópicos existentes"
  kexec kafka1 "/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka1:9092,kafka2:9092,kafka3:9092 --command-config /opt/kafka/config/client-broker.properties"
}

topic_create() {
  local name="${1:-}"; local parts="${2:-6}"
  local rf_default=3
  # se estiver rodando só kafka1/zk1 (min), use RF=1 por padrão
  [[ "$(detect_mode)" == "MINIMAL" ]] && rf_default=1
  local rf="${3:-$rf_default}"
  [[ -z "$name" ]] && { bad "uso: $0 topic_create <nome> [partitions=$parts] [replication=$rf]"; exit 1; }
  title "📦 Criando tópico '${name}' (partitions=${parts}, rf=${rf})"
  kexec kafka1 "/opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
    --bootstrap-server kafka1:9092,kafka2:9092,kafka3:9092 \
    --topic '${name}' --partitions ${parts} --replication-factor ${rf}"
  ok "Tópico '${name}' criado (ou já existia)."
}
topic_describe() {
  local name="${1:-}"; [[ -z "$name" ]] && { bad "uso: $0 topic_describe <nome>"; exit 1; }
  title "🔎 Describe do tópico '${name}'"
  kexec kafka1 "/opt/kafka/bin/kafka-topics.sh --describe --bootstrap-server kafka1:9092 --topic '${name}'"
}

produce() {
  local topic="${1:-}"; [[ -z "$topic" ]] && { bad "uso: $0 produce <topic>"; exit 1; }
  info "✍️  Producer simples para '${topic}' (Ctrl+C para sair)."
  kexec_it kafka1 "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka1:9092 --topic '${topic}'"
}
produce_keyed() {
  local topic="${1:-}"; [[ -z "$topic" ]] && { bad "uso: $0 produce_keyed <topic>"; exit 1; }
  info "🔑 Producer com chave para '${topic}' (formato 'key:value')."
  kexec_it kafka2 "/opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka2:9092 --topic '${topic}' \
    --property parse.key=true --property key.separator=:"
}
consume() {
  local topic="${1:-}"; local group="${2:-}"
  [[ -z "$topic" ]] && { bad "uso: $0 consume <topic> [group]"; exit 1; }
  local grp_flag=""; [[ -n "$group" ]] && grp_flag="--group '${group}'"
  info "👂 Consumer para '${topic}' ${group:+(group='${group}')} — lendo desde o início. (Ctrl+C para sair)"
  kexec_it kafka3 "/opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka3:9092 --topic '${topic}' ${grp_flag} --from-beginning"
}

broker_acl_min() {
  local user="${1:-broker}"
  title "🔐 Aplicando ACL mínima para inter-broker (User:${user})"

  # 1) Cluster: controle/metadata entre brokers
  kexec kafka1 "/opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka1:9092 \
    --command-config /opt/kafka/config/client-broker.properties \
    --add --allow-principal User:${user} \
    --cluster --operation ClusterAction --operation Describe"

  # 2) Replicação: followers precisam ler do leader
  kexec kafka1 "/opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka1:9092 \
    --command-config /opt/kafka/config/client-broker.properties \
    --add --allow-principal User:${user} \
    --topic '*' --operation Read --operation Describe"

  # 3) Tópicos internos (broker escreve/coordena)
  # __consumer_offsets
  kexec kafka1 "/opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka1:9092 \
    --command-config /opt/kafka/config/client-broker.properties \
    --add --allow-principal User:${user} \
    --topic __consumer_offsets \
    --operation Read --operation Write --operation Describe --operation Create"

  # __transaction_state (se usar transações/EOS)
  kexec kafka1 "/opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka1:9092 \
    --command-config /opt/kafka/config/client-broker.properties \
    --add --allow-principal User:${user} \
    --topic __transaction_state \
    --operation Read --operation Write --operation Describe --operation Create"

  ok "ACL mínima aplicada para User:${user}."
}

broker_acl_reset() {
  local user="${1:-broker}"
  title "🧹 Removendo ACL mínima de inter-broker (User:${user})"

  # Cluster
  kexec kafka1 "/opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka1:9092 \
    --command-config /opt/kafka/config/client-broker.properties \
    --remove --allow-principal User:${user} \
    --cluster --operation ClusterAction --operation Describe" || true

  # Replicação
  kexec kafka1 "/opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka1:9092 \
    --command-config /opt/kafka/config/client-broker.properties \
    --remove --allow-principal User:${user} \
    --topic '*' --operation Read --operation Describe" || true

  # Internos
  kexec kafka1 "/opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka1:9092 \
    --command-config /opt/kafka/config/client-broker.properties \
    --remove --allow-principal User:${user} \
    --topic __consumer_offsets \
    --operation Read --operation Write --operation Describe --operation Create" || true

  kexec kafka1 "/opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka1:9092 \
    --command-config /opt/kafka/config/client-broker.properties \
    --remove --allow-principal User:${user} \
    --topic __transaction_state \
    --operation Read --operation Write --operation Describe --operation Create" || true

  ok "ACLs removidas para User:${user}."
}

# ======= CLI =======
arg1="${1:-}"; arg2="${2:-}"; arg3="${3:-}"; arg4="${4:-}"
[[ -z "$arg1" ]] && usage

# flag --minimal aplicável a 'start'
IS_MINIMAL=0
if [[ "${arg1:-}" == "start" ]]; then
  if [[ "${arg2:-}" == "--minimal" || "${arg3:-}" == "--minimal" ]]; then
    IS_MINIMAL=1
    [[ "${arg2:-}" == "--minimal" ]] && arg2=""
    [[ "${arg3:-}" == "--minimal" ]] && arg3=""
  fi
fi

start_all_zks()      { if [[ $IS_MINIMAL -eq 1 ]]; then zk_start_min; else zk_start 1; zk_start 2; zk_start 3; fi; }
start_all_brokers()  { if [[ $IS_MINIMAL -eq 1 ]]; then broker_start_min; else broker_start 1; broker_start 2; broker_start 3; fi; }
start_everything()   { info "Subindo conjunto ${IS_MINIMAL:+ (minimal)} 🚀"; start_all_zks; start_all_brokers; }
stop_all_zks()       { zk_stop 1; zk_stop 2; zk_stop 3; }
stop_all_brokers()   { broker_stop 1; broker_stop 2; broker_stop 3; }

case "$arg1" in
  container-setup)   container_setup ;;
  container-destroy) container_destroy ;;
  start)
    case "${arg2:-}" in
      "")        start_everything ;;
      zks)       start_all_zks ;;
      brokers)   start_all_brokers ;;
      --minimal) start_everything ;;
      *) usage ;;
    esac ;;
  stop)
    case "${arg2:-}" in
      "")        stop_all_brokers; stop_all_zks ;;
      zks)       stop_all_zks ;;
      brokers)   stop_all_brokers ;;
      *) usage ;;
    esac ;;
  status) status_everything ;;
  logs)   logs_everything  ;;

  topic_list)      topic_list ;;
  topic_create)    topic_create "$arg2" "${arg3:-6}" "${arg4:-}" ;;
  topic_describe)  topic_describe "$arg2" ;;
  produce)         produce "$arg2" ;;
  produce_keyed)   produce_keyed "$arg2" ;;
  consume)         consume "$arg2" "${arg3:-}" ;;
  broker_acl_min)    broker_acl_min "${arg2:-broker}" ;;
  broker_acl_reset)  broker_acl_reset "${arg2:-broker}" ;;

  zk1|zk2|zk3)
    node="${arg1#zk}"; action="${arg2:-}"; [[ -z "$action" ]] && usage
    case "$action" in
      start)   zk_start "$node" ;;
      stop)    zk_stop "$node" ;;
      restart) zk_stop "$node"; zk_start "$node" ;;
      status)  zk_status "$node" ;;
      logs)    zk_logs "$node" ;;
      *) usage ;;
    esac ;;
  broker1|broker2|broker3)
    node="${arg1#broker}"; action="${arg2:-}"; [[ -z "$action" ]] && usage
    case "$action" in
      start)   broker_start "$node" ;;
      stop)    broker_stop "$node" ;;
      restart) broker_stop "$node"; broker_start "$node" ;;
      status)  broker_status "$node" ;;
      logs)    broker_logs "$node" ;;
      *) usage ;;
    esac ;;
  *) usage ;;
esac