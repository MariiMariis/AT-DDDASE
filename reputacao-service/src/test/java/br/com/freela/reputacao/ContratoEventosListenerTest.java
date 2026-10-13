package br.com.freela.reputacao;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:reputacao;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false",
        "eureka.client.enabled=false"
})
class ContratoEventosListenerTest {

    @Autowired ContratoEventosListener listener;
    @Autowired ReputacaoRepository repository;

    static String mensagem(UUID eventId, String eventType, UUID contratoId, UUID freelancerId, String valor) {
        return """
                {"eventId":"%s","eventType":"%s","eventVersion":1,"occurredAt":"2026-10-12T10:00:00Z",
                 "contratoId":"%s","correlationId":"corr-1","source":"contrato-service",
                 "payload":{"clienteId":"%s","freelancerId":"%s","titulo":"API","valor":%s,"status":"ATIVO"}}
                """.formatted(eventId, eventType, contratoId, UUID.randomUUID(), freelancerId, valor);
    }

    private void enviar(String eventType, UUID contratoId, UUID freelancerId, String valor) {
        listener.onMessage(new ConsumerRecord<>("contrato.eventos", 0, 0L, contratoId.toString(),
                mensagem(UUID.randomUUID(), eventType, contratoId, freelancerId, valor)));
    }

    @Test
    void cicloDeVidaAtualizaReputacaoDoFreelancer() {
        var freelancer = UUID.randomUUID();
        var contratoA = UUID.randomUUID(); var contratoB = UUID.randomUUID();
        enviar("ContratoCriado", contratoA, freelancer, "3500.00");
        enviar("EntregaRegistrada", contratoA, freelancer, "3500.00");
        enviar("ContratoConcluido", contratoA, freelancer, "3500.00");
        enviar("ContratoCriado", contratoB, freelancer, "800.00");
        enviar("ContratoCancelado", contratoB, freelancer, "800.00");

        var r = repository.findById(freelancer).orElseThrow();
        assertThat(r.contratosIniciados).isEqualTo(2);
        assertThat(r.contratosConcluidos).isEqualTo(1);
        assertThat(r.contratosCancelados).isEqualTo(1);
        assertThat(r.valorTotal).isEqualByComparingTo(new BigDecimal("3500.00"));
    }
}
