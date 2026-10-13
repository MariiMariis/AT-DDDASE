package br.com.freela.notificacao;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import tools.jackson.databind.json.JsonMapper;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:notificacao-idem;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.kafka.listener.auto-startup=false",
        "eureka.client.enabled=false"
})
class IdempotenciaNotificacaoTest {

    @Autowired ContratoEventosListener listener;
    @Autowired NotificacaoService service;
    @Autowired NotificacaoRepository repository;
    @Autowired EventoProcessadoRepository processados;
    @Autowired JsonMapper jsonMapper;

    @Test
    void mensagemReprocessadaNaoGeraNotificacaoRepetida() {
        var eventId = UUID.randomUUID(); var contratoId = UUID.randomUUID();
        var json = ContratoEventosListenerTest.mensagem(eventId, "ContratoCriado", contratoId, UUID.randomUUID(), UUID.randomUUID());
        var record = new ConsumerRecord<>("contrato.eventos", 0, 7L, contratoId.toString(), json);

        listener.onMessage(record);
        listener.onMessage(record);
        listener.onMessage(record);

        assertThat(repository.findByContratoIdOrderByCriadaEmAsc(contratoId)).hasSize(2);
        assertThat(processados.existsById(eventId)).isTrue();
    }

    @Test
    void serviceIdentificaEventoDuplicado() {
        var eventId = UUID.randomUUID(); var contratoId = UUID.randomUUID();
        var json = ContratoEventosListenerTest.mensagem(eventId, "EntregaRegistrada", contratoId, UUID.randomUUID(), UUID.randomUUID());
        var evento = jsonMapper.readValue(json, EventoContrato.class);

        assertThat(service.processar(evento)).isEqualTo(ResultadoProcessamento.PROCESSADO);
        assertThat(service.processar(evento)).isEqualTo(ResultadoProcessamento.DUPLICADO);
        assertThat(repository.findByContratoIdOrderByCriadaEmAsc(contratoId)).hasSize(1);
    }

    @Test
    void constraintUnicaImpedeDuplicidadeMesmoSemRegistroDeProcessamento() {
        var eventId = UUID.randomUUID(); var contratoId = UUID.randomUUID();
        var cliente = UUID.randomUUID();
        repository.save(new Notificacao(eventId, contratoId, cliente, "CLIENTE", "EntregaRegistrada", "existente", "corr-1"));
        var json = ContratoEventosListenerTest.mensagem(eventId, "EntregaRegistrada", contratoId, cliente, UUID.randomUUID());

        listener.onMessage(new ConsumerRecord<>("contrato.eventos", 0, 8L, contratoId.toString(), json));

        assertThat(repository.findByContratoIdOrderByCriadaEmAsc(contratoId)).hasSize(1);
        assertThat(processados.existsById(eventId)).isFalse();
    }
}
