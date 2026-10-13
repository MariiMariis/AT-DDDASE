package br.com.freela.contrato.application;

import br.com.freela.contrato.infrastructure.messaging.OutboxEventoJpaEntity;
import br.com.freela.contrato.infrastructure.messaging.SpringDataOutboxRepository;
import br.com.freela.contrato.infrastructure.web.CorrelationIdFilter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:contrato;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.kafka.admin.auto-create=false",
        "eureka.client.enabled=false"
})
class ContratoOutboxIntegrationTest {

    @Autowired ContratoApplicationService service;
    @Autowired SpringDataOutboxRepository outbox;
    @Autowired JsonMapper jsonMapper;

    @AfterEach
    void limparMdc() { MDC.clear(); }

    private CriarContratoCommand comando() {
        return new CriarContratoCommand(UUID.randomUUID(), UUID.randomUUID(), "Landing page", new BigDecimal("1200.00"));
    }

    @Test
    void persisteContratoEEventoNaMesmaTransacao() {
        MDC.put(CorrelationIdFilter.MDC_KEY, "corr-teste-1");
        var contrato = service.criar(comando());
        service.registrarEntrega(contrato.id());
        service.concluir(contrato.id());

        var eventos = outbox.findByAggregateIdOrderByOccurredAtAsc(contrato.id().toString());
        assertThat(eventos).extracting(OutboxEventoJpaEntity::getType)
                .containsExactly("ContratoCriado", "EntregaRegistrada", "ContratoConcluido");
        assertThat(eventos).extracting(OutboxEventoJpaEntity::getAggregateType).containsOnly("contrato");
        assertThat(eventos).extracting(OutboxEventoJpaEntity::getCorrelationId).containsOnly("corr-teste-1");

        var mensagem = jsonMapper.readTree(eventos.getFirst().getPayload());
        assertThat(mensagem.get("eventId").asString()).isEqualTo(eventos.getFirst().getId().toString());
        assertThat(mensagem.get("eventType").asString()).isEqualTo("ContratoCriado");
        assertThat(mensagem.get("contratoId").asString()).isEqualTo(contrato.id().toString());
        assertThat(mensagem.get("correlationId").asString()).isEqualTo("corr-teste-1");
        assertThat(mensagem.get("occurredAt").isNull()).isFalse();
        assertThat(mensagem.get("payload").get("freelancerId").asString()).isEqualTo(contrato.freelancerId().toString());
    }

    @Test
    void transicaoInvalidaNaoGravaEventoNoOutbox() {
        var contrato = service.criar(comando());
        assertThatThrownBy(() -> service.concluir(contrato.id())).isInstanceOf(IllegalStateException.class);
        assertThat(outbox.findByAggregateIdOrderByOccurredAtAsc(contrato.id().toString()))
                .extracting(OutboxEventoJpaEntity::getType).containsExactly("ContratoCriado");
    }
}
