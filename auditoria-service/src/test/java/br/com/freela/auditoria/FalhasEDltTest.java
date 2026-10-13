package br.com.freela.auditoria;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:auditoria-falhas;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.group-id=auditoria-falhas-test",
        "freela.kafka.retry.tentativas=2",
        "freela.kafka.retry.intervalo-inicial-ms=50",
        "freela.kafka.retry.intervalo-maximo-ms=100",
        "eureka.client.enabled=false"
})
@EmbeddedKafka(partitions = 3, topics = {"contrato.eventos", "contrato.eventos.auditoria.DLT"})
class FalhasEDltTest {

    @Autowired EmbeddedKafkaBroker broker;
    @Autowired AuditoriaController auditoria;
    @Autowired DltReprocessador dlt;
    @MockitoSpyBean AuditoriaService auditoriaService;

    private void enviar(int particao, String key, String valor) {
        try (var producer = new KafkaProducer<>(KafkaTestUtils.producerProps(broker), new StringSerializer(), new StringSerializer())) {
            producer.send(new ProducerRecord<>("contrato.eventos", particao, key, valor));
            producer.flush();
        }
    }

    private <T> T aguardar(Supplier<Optional<T>> condicao) throws InterruptedException {
        long limite = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < limite) {
            var valor = condicao.get();
            if (valor.isPresent()) return valor.get();
            Thread.sleep(200);
        }
        throw new AssertionError("Condição não atingida no tempo limite");
    }

    private Optional<DltMensagem> naDlt(String key) {
        return dlt.listar().stream().filter(m -> key.equals(m.key())).reduce((a, b) -> b);
    }

    private Optional<List<EventoAuditoriaResponse>> auditado(UUID eventId) {
        var registros = auditoria.listar(eventId, null, null, null);
        return registros.isEmpty() ? Optional.empty() : Optional.of(registros);
    }

    @Test
    void mensagemInvalidaVaiDiretoParaDltSemBloquearAsSeguintes() throws Exception {
        var keyInvalida = "contrato-invalido-" + UUID.randomUUID();
        var eventoValido = UUID.randomUUID(); var contratoValido = UUID.randomUUID();

        enviar(0, keyInvalida, "{isto nao e json");
        enviar(0, contratoValido.toString(), ContratoEventosListenerTest.mensagem(eventoValido, "ContratoCriado", contratoValido, "corr-ok"));

        aguardar(() -> auditado(eventoValido));
        var mensagem = aguardar(() -> naDlt(keyInvalida));
        assertThat(mensagem.excecao()).contains("EventoInvalidoException");
        assertThat(mensagem.topicoOriginal()).isEqualTo("contrato.eventos");
        assertThat(mensagem.particaoOriginal()).isZero();
        assertThat(mensagem.pendente()).isTrue();
    }

    @Test
    void falhaTransitoriaSeRecuperaNasRetentativas() throws Exception {
        var eventId = UUID.randomUUID(); var contratoId = UUID.randomUUID();
        doThrow(new IllegalStateException("banco indisponivel"))
                .doThrow(new IllegalStateException("banco indisponivel"))
                .doCallRealMethod()
                .when(auditoriaService).registrar(argThat(e -> e != null && eventId.equals(e.eventId())), anyString(), anyInt(), anyLong(), anyString());

        enviar(1, contratoId.toString(), ContratoEventosListenerTest.mensagem(eventId, "ContratoCriado", contratoId, "corr-retry"));

        aguardar(() -> auditado(eventId));
        verify(auditoriaService, times(3)).registrar(argThat(e -> e != null && eventId.equals(e.eventId())), anyString(), anyInt(), anyLong(), anyString());
        assertThat(naDlt(contratoId.toString())).isEmpty();
    }

    @Test
    void falhaPersistenteVaiParaDltAposTentativasEPodeSerReprocessada() throws Exception {
        var eventId = UUID.randomUUID(); var contratoId = UUID.randomUUID();
        doThrow(new IllegalStateException("erro persistente"))
                .when(auditoriaService).registrar(argThat(e -> e != null && eventId.equals(e.eventId())), anyString(), anyInt(), anyLong(), anyString());

        enviar(2, contratoId.toString(), ContratoEventosListenerTest.mensagem(eventId, "EntregaRegistrada", contratoId, "corr-dlt"));

        var mensagem = aguardar(() -> naDlt(contratoId.toString()));
        assertThat(mensagem.eventId()).isEqualTo(eventId.toString());
        assertThat(mensagem.excecao()).contains("IllegalStateException");
        assertThat(mensagem.particaoOriginal()).isEqualTo(2);
        verify(auditoriaService, times(3)).registrar(argThat(e -> e != null && eventId.equals(e.eventId())), anyString(), anyInt(), anyLong(), anyString());
        assertThat(auditoria.listar(eventId, null, null, null)).isEmpty();

        Mockito.reset(auditoriaService);
        doCallRealMethod().when(auditoriaService).registrar(Mockito.any(), anyString(), anyInt(), anyLong(), anyString());
        var resultado = dlt.reprocessar();

        assertThat(resultado.reprocessadas()).isGreaterThanOrEqualTo(1);
        var registro = aguardar(() -> auditado(eventId)).getFirst();
        assertThat(registro.particao()).isEqualTo(2);
        assertThat(registro.offset()).isEqualTo(mensagem.offsetOriginal());
        assertThat(naDlt(contratoId.toString())).get().extracting(DltMensagem::pendente).isEqualTo(false);
    }
}
