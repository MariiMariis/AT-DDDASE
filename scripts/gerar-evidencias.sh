#!/usr/bin/env bash
set -uo pipefail
export MSYS_NO_PATHCONV=1

GATEWAY="${GATEWAY:-http://localhost:8080}"
EUREKA="${EUREKA:-http://localhost:8761}"
ELASTIC="${ELASTIC:-http://localhost:9200}"
ZIPKIN="${ZIPKIN:-http://localhost:9411}"
KIBANA="${KIBANA:-http://localhost:5601}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-freela-kafka}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-freela-postgres}"
TOPICO="contrato.eventos"
EXECUCAO="$(date +%Y%m%d%H%M%S)"
CLIENTE="11111111-1111-1111-1111-111111111111"
FREELANCER="$(printf 'eeeeeeee-eeee-eeee-eeee-%012d' "$((10#${EXECUCAO: -6}))")"

secao() { printf '\n## %s\n\n' "$1"; }
cmd() { printf '```text\n'; "$@"; printf '```\n'; }
id_de() { sed -E 's/.*"id":"([^"]+)".*/\1/'; }
contar() { grep -o "$1" | wc -l | tr -d ' '; }
kafka() { docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/"$@" 2>/dev/null; }
psql_db() { docker exec "$POSTGRES_CONTAINER" psql -U freela -d "$1" -c "$2"; }

criar_contrato() {
  curl -s -X POST "$GATEWAY/api/contratos" -H 'Content-Type: application/json' -H "X-Correlation-Id: $1" \
    -d "{\"clienteId\":\"$CLIENTE\",\"freelancerId\":\"$2\",\"titulo\":\"$3\",\"valor\":$4}"
}
transicao() { curl -s -o /dev/null -w '%{http_code}' -X POST "$GATEWAY/api/contratos/$1/$2" -H "X-Correlation-Id: $3"; }
mensagens_do_contrato() {
  kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "$TOPICO" --from-beginning --timeout-ms 8000 \
    --formatter-property print.partition=true --formatter-property print.offset=true \
    --formatter-property print.key=true --formatter-property print.headers=true | grep -F "$1"
}
resumo_auditoria() {
  curl -s "$GATEWAY/api/auditoria?contratoId=$1" \
    | grep -oE '"eventType":"[A-Za-z]+"|"particao":[0-9]+|"offset":[0-9]+' \
    | sed -E 's/"([a-zA-Z]+)":"?([^"]*)"?/\2/' | paste -d' ' - - - \
    | awk '{printf "%s(P%s@%s) ", $1, $2, $3} END {print ""}'
}
es_por_servico() {
  curl -s "$ELASTIC/freela-logs-*/_search" -H 'Content-Type: application/json' \
    -d "{\"size\":0,\"query\":{\"term\":{\"$1.keyword\":\"$2\"}},\"aggs\":{\"servicos\":{\"terms\":{\"field\":\"service.keyword\"}}}}" \
    | grep -oE '"value":[0-9]+|"key":"[^"]+","doc_count":[0-9]+' | sed -E 's/"value":/total de logs: /; s/"key":"([^"]+)","doc_count":/  \1: /'
}
es_linhas() {
  curl -s "$ELASTIC/freela-logs-*/_search" -H 'Content-Type: application/json' \
    -d "{\"size\":100,\"sort\":[{\"@timestamp\":\"asc\"}],\"_source\":[\"@timestamp\",\"service\",\"message\"],\"query\":{\"term\":{\"$1.keyword\":\"$2\"}}}" \
    | grep -oE '"(@timestamp|service|message)":"[^"]{0,90}' \
    | awk -F'":"' '{k=$1; sub(/^"/, "", k); v[k]=$2; n++; if (n==3) {printf "%s  %-20s %s\n", substr(v["@timestamp"],12,12), v["service"], v["message"]; n=0; delete v}}'
}

printf '# Evidências de execução - %s\n' "$(date '+%Y-%m-%d %H:%M:%S')"

secao "0. Serviços registrados no Eureka"
cmd bash -c "curl -s -H 'Accept: application/json' $EUREKA/eureka/apps | grep -oE '\"app\":\"[A-Z-]+\"' | sort -u"

CORR="evid-$EXECUCAO-criar"
secao "1. Requisição recebida pelo API Gateway (POST /api/contratos, X-Correlation-Id: $CORR)"
RESPOSTA=$(curl -s -i -X POST "$GATEWAY/api/contratos" -H 'Content-Type: application/json' -H "X-Correlation-Id: $CORR" \
  -d "{\"clienteId\":\"$CLIENTE\",\"freelancerId\":\"$FREELANCER\",\"titulo\":\"Evidencia $EXECUCAO\",\"valor\":3500.00}")
cmd printf '%s\n' "$(printf '%s' "$RESPOSTA" | tr -d '\r' | grep -E '^HTTP|^X-Correlation-Id|^\{')"
CONTRATO=$(printf '%s' "$RESPOSTA" | tail -1 | id_de)
printf 'Ciclo de vida pelo Gateway: entrega=%s conclusao=%s\n' \
  "$(transicao "$CONTRATO" entrega "evid-$EXECUCAO-entrega")" "$(transicao "$CONTRATO" conclusao "evid-$EXECUCAO-conclusao")"
sleep 10

secao "2. Alteração persistida no contrato-service (contrato_db: contratos e outbox_eventos)"
cmd psql_db contrato_db "select id, status, criado_em from contratos where id = '$CONTRATO';"
cmd psql_db contrato_db "select id as event_id, type, aggregateid, correlation_id, left(traceparent, 30) as traceparent from outbox_eventos where aggregateid = '$CONTRATO' order by occurred_at;"

secao "3. Eventos publicados no Kafka (tópico $TOPICO, chave = contratoId)"
cmd bash -c "$(declare -f kafka mensagens_do_contrato); KAFKA_CONTAINER=$KAFKA_CONTAINER TOPICO=$TOPICO mensagens_do_contrato '$CONTRATO' | cut -c1-400"

secao "4. Consumo do evento pelos serviços interessados (grupos de consumidores, lag)"
for grupo in notificacao-service reputacao-service auditoria-service; do
  cmd bash -c "docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group $grupo 2>/dev/null | awk 'NF {print \$1, \$2, \$3, \$4, \$5, \$6}' | column -t"
done

secao "5. Persistência realizada pelos consumidores"
printf 'Notificações (notificacao_db):\n'
cmd bash -c "curl -s '$GATEWAY/api/notificacoes?contratoId=$CONTRATO' | grep -oE '\"papelDestinatario\":\"[A-Z]+\",\"tipo\":\"[A-Za-z]+\"'"
printf 'Reputação do freelancer (reputacao_db):\n'
cmd bash -c "curl -s '$GATEWAY/api/reputacoes/$FREELANCER'; echo"
printf 'Auditoria (auditoria_db) - eventType(partição@offset):\n'
cmd resumo_auditoria "$CONTRATO"

secao "6. Tratamento de mensagem duplicada (reenvio do mesmo ContratoConcluido 2x)"
DUP=$(mensagens_do_contrato "$CONTRATO" | grep ContratoConcluido | head -1 | awk -F'\t' '{print $(NF-1) "|" $NF}')
EVENTO_DUP=$(printf '%s' "$DUP" | grep -oE '"eventId":"[^"]+"' | head -1 | cut -d'"' -f4)
ANTES_REP=$(curl -s "$GATEWAY/api/reputacoes/$FREELANCER")
ANTES_NOT=$(curl -s "$GATEWAY/api/notificacoes?contratoId=$CONTRATO" | contar '"eventId"')
ANTES_AUD=$(curl -s "$GATEWAY/api/auditoria?contratoId=$CONTRATO" | contar '"eventId"')
for _ in 1 2; do printf '%s\n' "$DUP" | docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPICO" --reader-property parse.key=true --reader-property key.separator='|' 2>/dev/null; done
sleep 8
printf 'eventId reenviado: %s\n\nOcorrências desse eventId no tópico:\n' "$EVENTO_DUP"
cmd bash -c "$(declare -f kafka mensagens_do_contrato); KAFKA_CONTAINER=$KAFKA_CONTAINER TOPICO=$TOPICO mensagens_do_contrato '$EVENTO_DUP' | awk -F'\t' '{print \$1, \$2}'"
cmd printf 'ANTES  -> reputacao=%s\n          notificacoes=%s auditoria=%s\nDEPOIS -> reputacao=%s\n          notificacoes=%s auditoria=%s\n' \
  "$ANTES_REP" "$ANTES_NOT" "$ANTES_AUD" "$(curl -s "$GATEWAY/api/reputacoes/$FREELANCER")" \
  "$(curl -s "$GATEWAY/api/notificacoes?contratoId=$CONTRATO" | contar '"eventId"')" \
  "$(curl -s "$GATEWAY/api/auditoria?contratoId=$CONTRATO" | contar '"eventId"')"
cmd psql_db reputacao_db "select event_id, event_type, processado_em from eventos_processados where event_id = '$EVENTO_DUP';"
printf 'Logs dos consumidores ao receber as duplicatas (Elasticsearch, eventId=%s):\n' "$EVENTO_DUP"
cmd bash -c "$(declare -f es_linhas); ELASTIC=$ELASTIC es_linhas eventId '$EVENTO_DUP' | grep -i duplicado"

secao "7. Ordem dos eventos de um mesmo contrato (6 contratos em paralelo)"
IDS=$(mktemp)
for i in 1 2 3 4 5 6; do
  (
    C=$(criar_contrato "evid-$EXECUCAO-ordem-$i" "$FREELANCER" "Ordem $i" "10$i.00" | id_de)
    transicao "$C" entrega "evid-$EXECUCAO-ordem-$i" >/dev/null
    transicao "$C" conclusao "evid-$EXECUCAO-ordem-$i" >/dev/null
    echo "$C" >> "$IDS"
  ) &
done
wait; sleep 10
printf 'contratoId - eventType(partição@offset) registrados pela auditoria, ordenados por data do evento:\n'
printf '```text\n'
while read -r C; do printf '%s  %s\n' "${C:0:8}" "$(resumo_auditoria "$C")"; done < "$IDS"
printf '```\n'
rm -f "$IDS"

secao "8. Logs da mesma operação consultados de forma centralizada (Elasticsearch, correlationId=$CORR)"
cmd es_por_servico correlationId "$CORR"
cmd es_linhas correlationId "$CORR"
printf 'Kibana: %s/app/discover#/?_g=(time:(from:now-1h,to:now))&_a=(columns:!(service,correlationId,contratoId,eventId,message),dataSource:(dataViewId:freela-logs,type:dataView),query:(language:kuery,query:%%27correlationId:%%22%s%%22%%27))\n' "$KIBANA" "$CORR"
printf '\nLogs do ciclo de vida completo (contratoId=%s):\n' "$CONTRATO"
cmd es_por_servico contratoId "$CONTRATO"

secao "9. Trace correspondente no Zipkin (correlationId=$CORR)"
TRACE=$(curl -s "$ZIPKIN/api/v2/traces?annotationQuery=correlationId%3D$CORR&limit=1&lookback=3600000")
TRACE_ID=$(printf '%s' "$TRACE" | grep -oE '"traceId":"[a-f0-9]+"' | head -1 | cut -d'"' -f4)
cmd printf 'traceId: %s\nspans:\n%s\n' "$TRACE_ID" \
  "$(printf '%s' "$TRACE" | grep -oE '"kind":"[A-Z]+","name":"[^"]+"[^}]*"localEndpoint":\{"serviceName":"[^"]+"' \
     | sed -E 's/"kind":"([A-Z]+)","name":"([^"]+)".*"serviceName":"([^"]+)"/  [\3] \1 \2/' | sort)"
printf 'Zipkin: %s/zipkin/traces/%s\n' "$ZIPKIN" "$TRACE_ID"
printf '\nMesmo traceId nos logs centralizados:\n'
cmd es_por_servico traceId "$TRACE_ID"

secao "10. Mensagem inválida enviada para a DLT"
printf '%s\n' "evid-$EXECUCAO-poison|{\"eventId\":\"invalido\"" | docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPICO" --reader-property parse.key=true --reader-property key.separator='|' 2>/dev/null
sleep 8
cmd bash -c "curl -s '$GATEWAY/api/auditoria/dlt' | grep -oE '\"key\":\"evid-$EXECUCAO-poison\"[^}]*\"excecao\":\"[^\"]+\",\"motivo\":\"[^\"]+\",\"pendente\":(true|false)'"

printf '\nContrato usado nas evidências: %s\n' "$CONTRATO"
