package br.com.freela.auditoria;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:auditoria-ordem;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.group-id=auditoria-ordem-test",
        "eureka.client.enabled=false"
})
@EmbeddedKafka(partitions = 3, topics = "contrato.eventos")
class OrdenacaoConcorrenteTest {

    private static final List<String> SEQUENCIA = List.of("ContratoCriado", "EntregaRegistrada", "ContratoConcluido");

    @Autowired EmbeddedKafkaBroker broker;
    @Autowired EventoAuditoriaRepository repository;
    @Autowired KafkaListenerEndpointRegistry registry;
    @MockitoSpyBean AuditoriaService auditoriaService;

    record Processamento(UUID contratoId, String eventType, String thread, int particao) {}

    @Test
    void processaContratosEmParaleloPreservandoOrdemPorContrato() throws Exception {
        List<Processamento> processados = Collections.synchronizedList(new ArrayList<>());
        doAnswer(inv -> {
            EventoContrato evento = inv.getArgument(0);
            Thread.sleep(ThreadLocalRandom.current().nextInt(5, 25));
            processados.add(new Processamento(evento.contratoId(), evento.eventType(),
                    Thread.currentThread().getName(), inv.getArgument(2)));
            return inv.callRealMethod();
        }).when(auditoriaService).registrar(any(), anyString(), anyInt(), anyLong(), anyString());

        aguardarUmaParticaoPorConsumidor();

        List<UUID> contratos = new ArrayList<>();
        for (int i = 0; i < 12; i++) contratos.add(UUID.randomUUID());

        Map<String, Object> props = KafkaTestUtils.producerProps(broker);
        try (var producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
            for (String tipo : SEQUENCIA) {
                for (UUID contratoId : contratos) {
                    producer.send(new ProducerRecord<>("contrato.eventos", contratoId.toString(),
                            ContratoEventosListenerTest.mensagem(UUID.randomUUID(), tipo, contratoId, "corr-" + contratoId)));
                }
            }
            producer.flush();
        }

        int esperado = contratos.size() * SEQUENCIA.size();
        long limite = System.currentTimeMillis() + 30_000;
        while (repository.count() < esperado && System.currentTimeMillis() < limite) Thread.sleep(200);
        assertThat(repository.count()).isEqualTo(esperado);

        Map<UUID, List<String>> ordemPorContrato = new ArrayList<>(processados).stream()
                .collect(Collectors.groupingBy(Processamento::contratoId,
                        Collectors.mapping(Processamento::eventType, Collectors.toList())));
        assertThat(ordemPorContrato).hasSize(contratos.size());
        ordemPorContrato.values().forEach(ordem -> assertThat(ordem).containsExactlyElementsOf(SEQUENCIA));

        Map<UUID, Long> particoesPorContrato = processados.stream()
                .collect(Collectors.groupingBy(Processamento::contratoId,
                        Collectors.mapping(Processamento::particao, Collectors.toSet())))
                .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> (long) e.getValue().size()));
        assertThat(particoesPorContrato.values()).containsOnly(1L);

        assertThat(processados.stream().map(Processamento::particao).distinct().count()).isGreaterThan(1);
        assertThat(processados.stream().map(Processamento::thread).distinct().count()).isGreaterThan(1);
    }

    private void aguardarUmaParticaoPorConsumidor() throws InterruptedException {
        long limite = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < limite) {
            boolean distribuido = registry.getListenerContainers().stream()
                    .map(c -> (ConcurrentMessageListenerContainer<?, ?>) c)
                    .flatMap(c -> c.getContainers().stream())
                    .allMatch(c -> c.getAssignedPartitions() != null && c.getAssignedPartitions().size() == 1);
            if (distribuido) return;
            Thread.sleep(200);
        }
        throw new IllegalStateException("Particoes nao distribuidas entre os consumidores");
    }
}
