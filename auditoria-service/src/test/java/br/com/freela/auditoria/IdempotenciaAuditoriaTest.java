package br.com.freela.auditoria;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:auditoria-idem;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.kafka.listener.auto-startup=false",
        "eureka.client.enabled=false"
})
class IdempotenciaAuditoriaTest {

    @Autowired ContratoEventosListener listener;
    @Autowired AuditoriaController controller;

    @Test
    void mesmaMensagemEmOffsetsDiferentesGeraUmUnicoRegistro() {
        var eventId = UUID.randomUUID(); var contratoId = UUID.randomUUID();
        var json = ContratoEventosListenerTest.mensagem(eventId, "ContratoConcluido", contratoId, "corr-dup");

        listener.onMessage(new ConsumerRecord<>("contrato.eventos", 0, 10L, contratoId.toString(), json));
        listener.onMessage(new ConsumerRecord<>("contrato.eventos", 0, 11L, contratoId.toString(), json));

        assertThat(controller.listar(eventId, null, null, null)).singleElement()
                .satisfies(e -> assertThat(e.offset()).isEqualTo(10L));
    }
}
