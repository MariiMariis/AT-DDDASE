package br.com.freela.notificacao;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:notificacao;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.kafka.listener.auto-startup=false",
        "eureka.client.enabled=false"
})
class ContratoEventosListenerTest {

    @Autowired ContratoEventosListener listener;
    @Autowired NotificacaoRepository repository;

    static String mensagem(UUID eventId, String eventType, UUID contratoId, UUID clienteId, UUID freelancerId) {
        return """
                {"eventId":"%s","eventType":"%s","eventVersion":1,"occurredAt":"2026-10-12T10:00:00Z",
                 "contratoId":"%s","correlationId":"corr-1","source":"contrato-service",
                 "payload":{"clienteId":"%s","freelancerId":"%s","titulo":"API","valor":3500.00,"status":"ATIVO"}}
                """.formatted(eventId, eventType, contratoId, clienteId, freelancerId);
    }

    private void enviar(String eventType, UUID contratoId, UUID clienteId, UUID freelancerId) {
        listener.onMessage(new ConsumerRecord<>("contrato.eventos", 0, 0L, contratoId.toString(),
                mensagem(UUID.randomUUID(), eventType, contratoId, clienteId, freelancerId)));
    }

    @Test
    void contratoCriadoNotificaClienteEFreelancer() {
        var contratoId = UUID.randomUUID(); var cliente = UUID.randomUUID(); var freelancer = UUID.randomUUID();
        enviar("ContratoCriado", contratoId, cliente, freelancer);
        var notificacoes = repository.findByContratoIdOrderByCriadaEmAsc(contratoId);
        assertThat(notificacoes).extracting(n -> n.destinatarioId).containsExactlyInAnyOrder(cliente, freelancer);
        assertThat(notificacoes).extracting(n -> n.correlationId).containsOnly("corr-1");
    }

    @Test
    void entregaRegistradaNotificaSomenteCliente() {
        var contratoId = UUID.randomUUID(); var cliente = UUID.randomUUID();
        enviar("EntregaRegistrada", contratoId, cliente, UUID.randomUUID());
        assertThat(repository.findByContratoIdOrderByCriadaEmAsc(contratoId))
                .singleElement().satisfies(n -> {
                    assertThat(n.destinatarioId).isEqualTo(cliente);
                    assertThat(n.papelDestinatario).isEqualTo("CLIENTE");
                });
    }
}
