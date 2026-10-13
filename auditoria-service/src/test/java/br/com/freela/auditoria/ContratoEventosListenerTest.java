package br.com.freela.auditoria;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:auditoria;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.kafka.listener.auto-startup=false",
        "eureka.client.enabled=false"
})
class ContratoEventosListenerTest {

    @Autowired ContratoEventosListener listener;
    @Autowired AuditoriaController controller;

    static String mensagem(UUID eventId, String eventType, UUID contratoId, String correlationId) {
        return """
                {"eventId":"%s","eventType":"%s","eventVersion":1,"occurredAt":"2026-10-12T10:00:00Z",
                 "contratoId":"%s","correlationId":"%s","source":"contrato-service",
                 "payload":{"clienteId":"%s","freelancerId":"%s","titulo":"API","valor":3500.00,"status":"ATIVO"}}
                """.formatted(eventId, eventType, contratoId, correlationId, UUID.randomUUID(), UUID.randomUUID());
    }

    @Test
    void registraEventoConsultavelPorContratoECorrelation() {
        var eventId = UUID.randomUUID(); var contratoId = UUID.randomUUID();
        var json = mensagem(eventId, "ContratoCriado", contratoId, "corr-audit-1");
        listener.onMessage(new ConsumerRecord<>("contrato.eventos", 2, 15L, contratoId.toString(), json));

        assertThat(controller.listar(null, contratoId, null, null)).singleElement().satisfies(e -> {
            assertThat(e.eventId()).isEqualTo(eventId);
            assertThat(e.eventType()).isEqualTo("ContratoCriado");
            assertThat(e.correlationId()).isEqualTo("corr-audit-1");
            assertThat(e.ocorridoEm()).isNotNull();
            assertThat(e.particao()).isEqualTo(2);
            assertThat(e.offset()).isEqualTo(15L);
            assertThat(e.payload()).isEqualTo(json);
        });
        assertThat(controller.listar(null, null, "corr-audit-1", null)).hasSize(1);
        assertThat(controller.listar(eventId, null, null, null)).hasSize(1);
    }
}
