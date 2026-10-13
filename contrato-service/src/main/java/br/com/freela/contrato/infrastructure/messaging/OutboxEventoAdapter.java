package br.com.freela.contrato.infrastructure.messaging;

import br.com.freela.contrato.application.port.EventoOutbox;
import br.com.freela.contrato.domain.event.ContratoEvent;
import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.infrastructure.web.CorrelationIdFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

@Component
public class OutboxEventoAdapter implements EventoOutbox {
    public static final String AGGREGATE_TYPE = "contrato";
    private static final Logger log = LoggerFactory.getLogger(OutboxEventoAdapter.class);
    private final SpringDataOutboxRepository repository;
    private final JsonMapper jsonMapper;

    public OutboxEventoAdapter(SpringDataOutboxRepository repository, JsonMapper jsonMapper) {
        this.repository = repository;
        this.jsonMapper = jsonMapper;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void registrar(List<DomainEvent> eventos) {
        String correlationId = MDC.get(CorrelationIdFilter.MDC_KEY);
        for (DomainEvent evento : eventos) {
            if (!(evento instanceof ContratoEvent contratoEvent)) {
                throw new IllegalArgumentException("Evento não suportado: " + evento.eventType());
            }
            var mensagem = new EventoIntegracao(evento.eventId(), evento.eventType(), EventoIntegracao.VERSAO,
                    evento.occurredAt(), contratoEvent.contratoId(), correlationId, EventoIntegracao.SOURCE,
                    ContratoPayload.de(contratoEvent));
            repository.save(new OutboxEventoJpaEntity(evento.eventId(), AGGREGATE_TYPE,
                    contratoEvent.contratoId().toString(), evento.eventType(),
                    jsonMapper.writeValueAsString(mensagem), correlationId, evento.occurredAt()));
            log.info("outbox.evento.registrado contratoId={} eventId={} eventType={} correlationId={}",
                    contratoEvent.contratoId(), evento.eventId(), evento.eventType(), correlationId);
        }
    }
}
