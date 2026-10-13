package br.com.freela.reputacao;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:reputacao-idem;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false",
        "eureka.client.enabled=false"
})
class IdempotenciaReputacaoTest {

    @Autowired ContratoEventosListener listener;
    @Autowired ReputacaoRepository repository;
    @Autowired EventoProcessadoRepository processados;

    private ConsumerRecord<String, String> record(UUID eventId, String tipo, UUID contratoId, UUID freelancer) {
        return new ConsumerRecord<>("contrato.eventos", 1, 0L, contratoId.toString(),
                ContratoEventosListenerTest.mensagem(eventId, tipo, contratoId, freelancer, "2500.00"));
    }

    @Test
    void reprocessamentoNaoIncrementaContadoresNemValor() {
        var freelancer = UUID.randomUUID(); var contratoId = UUID.randomUUID();
        var criado = record(UUID.randomUUID(), "ContratoCriado", contratoId, freelancer);
        var concluido = record(UUID.randomUUID(), "ContratoConcluido", contratoId, freelancer);

        listener.onMessage(criado);
        listener.onMessage(concluido);
        listener.onMessage(criado);
        listener.onMessage(concluido);
        listener.onMessage(concluido);

        var r = repository.findById(freelancer).orElseThrow();
        assertThat(r.contratosIniciados).isEqualTo(1);
        assertThat(r.contratosConcluidos).isEqualTo(1);
        assertThat(r.valorTotal).isEqualByComparingTo(new BigDecimal("2500.00"));
    }

    @Test
    void eventoIgnoradoTambemERegistradoComoProcessado() {
        var eventId = UUID.randomUUID();
        listener.onMessage(record(eventId, "EntregaRegistrada", UUID.randomUUID(), UUID.randomUUID()));
        assertThat(processados.existsById(eventId)).isTrue();
    }
}
