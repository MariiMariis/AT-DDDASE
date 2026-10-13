package br.com.freela.notificacao;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Service
public class DltReprocessador {
    public static final String HEADER_FALHA_REPROCESSAMENTO = "freela-reprocessamento-falha";
    private static final Logger log = LoggerFactory.getLogger(DltReprocessador.class);
    private static final Duration LIMITE_LEITURA = Duration.ofSeconds(20);

    private final ContratoEventosListener listener;
    private final KafkaTemplate<String, String> template;
    private final JsonMapper jsonMapper;
    private final String bootstrapServers;
    private final String topicoDlt;
    private final String grupoReprocessamento;

    @SuppressWarnings("unchecked")
    public DltReprocessador(ContratoEventosListener listener, KafkaTemplate<?, ?> template, JsonMapper jsonMapper,
                            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                            @Value("${freela.kafka.topico-dlt}") String topicoDlt,
                            @Value("${spring.application.name}") String aplicacao) {
        this.listener = listener;
        this.template = (KafkaTemplate<String, String>) template;
        this.jsonMapper = jsonMapper;
        this.bootstrapServers = bootstrapServers;
        this.topicoDlt = topicoDlt;
        this.grupoReprocessamento = aplicacao + "-dlt-reprocessamento";
    }

    public synchronized List<DltMensagem> listar() {
        try (var consumer = novoConsumidor()) {
            var particoes = particoes(consumer);
            var comprometidos = consumer.committed(new HashSet<>(particoes));
            consumer.seekToBeginning(particoes);
            var mensagens = new ArrayList<DltMensagem>();
            ler(consumer, particoes, consumer.endOffsets(particoes), r -> {
                var commit = comprometidos.get(new TopicPartition(r.topic(), r.partition()));
                boolean pendente = commit == null || r.offset() >= commit.offset();
                mensagens.add(paraMensagem(r, pendente));
            });
            log.info("dlt.listagem topicoDlt={} mensagens={}", topicoDlt, mensagens.size());
            return mensagens;
        }
    }

    public synchronized ResultadoReprocessamento reprocessar() {
        log.info("dlt.reprocessamento.inicio topicoDlt={} grupo={}", topicoDlt, grupoReprocessamento);
        try (var consumer = novoConsumidor()) {
            var particoes = particoes(consumer);
            var comprometidos = consumer.committed(new HashSet<>(particoes));
            for (var tp : particoes) {
                var commit = comprometidos.get(tp);
                if (commit == null) consumer.seekToBeginning(List.of(tp)); else consumer.seek(tp, commit.offset());
            }
            var fim = consumer.endOffsets(particoes);
            int[] contadores = new int[3];
            ler(consumer, particoes, fim, r -> {
                contadores[0]++;
                var original = registroOriginal(r);
                try {
                    listener.processar(original);
                    contadores[1]++;
                    log.info("dlt.reprocessamento.sucesso particaoDlt={} offsetDlt={} key={} topicoOriginal={} particaoOriginal={} offsetOriginal={}",
                            r.partition(), r.offset(), r.key(), original.topic(), original.partition(), original.offset());
                } catch (RuntimeException e) {
                    contadores[2]++;
                    reenviarParaDlt(r, e);
                    log.error("dlt.reprocessamento.falha particaoDlt={} offsetDlt={} key={} excecao={} motivo={} acao=REENVIADA_PARA_DLT",
                            r.partition(), r.offset(), r.key(), e.getClass().getSimpleName(), e.getMessage());
                }
            });
            Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
            fim.forEach((tp, offset) -> commits.put(tp, new OffsetAndMetadata(offset)));
            consumer.commitSync(commits);
            var resultado = new ResultadoReprocessamento(contadores[0], contadores[1], contadores[2]);
            log.info("dlt.reprocessamento.fim topicoDlt={} lidas={} reprocessadas={} falharam={}",
                    topicoDlt, resultado.lidas(), resultado.reprocessadas(), resultado.falharam());
            return resultado;
        }
    }

    private Consumer<String, String> novoConsumidor() {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, grupoReprocessamento);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        return new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
    }

    private List<TopicPartition> particoes(Consumer<String, String> consumer) {
        var particoes = consumer.partitionsFor(topicoDlt).stream()
                .map(p -> new TopicPartition(topicoDlt, p.partition())).toList();
        consumer.assign(particoes);
        return particoes;
    }

    private void ler(Consumer<String, String> consumer, List<TopicPartition> particoes, Map<TopicPartition, Long> fim,
                     java.util.function.Consumer<ConsumerRecord<String, String>> acao) {
        long limite = System.currentTimeMillis() + LIMITE_LEITURA.toMillis();
        while (System.currentTimeMillis() < limite
                && particoes.stream().anyMatch(tp -> consumer.position(tp) < fim.get(tp))) {
            for (var r : consumer.poll(Duration.ofMillis(500))) {
                if (r.offset() < fim.get(new TopicPartition(r.topic(), r.partition()))) acao.accept(r);
            }
        }
    }

    private ConsumerRecord<String, String> registroOriginal(ConsumerRecord<String, String> r) {
        String topico = texto(r, KafkaHeaders.DLT_ORIGINAL_TOPIC);
        Integer particao = inteiro(r, KafkaHeaders.DLT_ORIGINAL_PARTITION);
        Long offset = longo(r, KafkaHeaders.DLT_ORIGINAL_OFFSET);
        return new ConsumerRecord<>(topico != null ? topico : r.topic(), particao != null ? particao : r.partition(),
                offset != null ? offset : r.offset(), r.key(), r.value());
    }

    private void reenviarParaDlt(ConsumerRecord<String, String> r, RuntimeException e) {
        var registro = new ProducerRecord<>(topicoDlt, r.partition(), r.key(), r.value());
        for (Header h : r.headers()) {
            if (!HEADER_FALHA_REPROCESSAMENTO.equals(h.key())) registro.headers().add(h);
        }
        registro.headers().add(HEADER_FALHA_REPROCESSAMENTO,
                (e.getClass().getSimpleName() + ": " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
        template.send(registro).join();
    }

    private DltMensagem paraMensagem(ConsumerRecord<String, String> r, boolean pendente) {
        String eventId = null;
        String eventType = null;
        try {
            var json = jsonMapper.readTree(r.value());
            eventId = json.path("eventId").asString(null);
            eventType = json.path("eventType").asString(null);
        } catch (RuntimeException ignorada) {
            log.debug("dlt.listagem.json_invalido particaoDlt={} offsetDlt={}", r.partition(), r.offset());
        }
        String motivo = texto(r, HEADER_FALHA_REPROCESSAMENTO);
        return new DltMensagem(r.partition(), r.offset(), r.key(), eventId, eventType,
                texto(r, KafkaHeaders.DLT_ORIGINAL_TOPIC), inteiro(r, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                longo(r, KafkaHeaders.DLT_ORIGINAL_OFFSET), texto(r, KafkaHeaders.DLT_EXCEPTION_FQCN),
                motivo != null ? motivo : texto(r, KafkaHeaders.DLT_EXCEPTION_MESSAGE), pendente, r.value());
    }

    private static byte[] header(ConsumerRecord<String, String> r, String nome) {
        var h = r.headers().lastHeader(nome);
        return h == null ? null : h.value();
    }

    private static String texto(ConsumerRecord<String, String> r, String nome) {
        var v = header(r, nome);
        return v == null ? null : new String(v, StandardCharsets.UTF_8);
    }

    private static Integer inteiro(ConsumerRecord<String, String> r, String nome) {
        var v = header(r, nome);
        return v == null || v.length != Integer.BYTES ? null : ByteBuffer.wrap(v).getInt();
    }

    private static Long longo(ConsumerRecord<String, String> r, String nome) {
        var v = header(r, nome);
        return v == null || v.length != Long.BYTES ? null : ByteBuffer.wrap(v).getLong();
    }
}
