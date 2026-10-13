package br.com.freela.contrato.infrastructure.messaging;

import br.com.freela.contrato.application.port.EventoOutbox;
import br.com.freela.contrato.domain.event.ContratoEvent;
import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.infrastructure.web.CorrelationIdFilter;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class OutboxEventoAdapter implements EventoOutbox {
    public static final String AGGREGATE_TYPE = "contrato";
    private static final Logger log = LoggerFactory.getLogger(OutboxEventoAdapter.class);
    private final SpringDataOutboxRepository repository;
    private final JsonMapper jsonMapper;
    private final Tracer tracer;
    private final Propagator propagator;
    private final String topico;

    public OutboxEventoAdapter(SpringDataOutboxRepository repository, JsonMapper jsonMapper, Tracer tracer,
                               Propagator propagator, @Value("${freela.kafka.topico-contrato-eventos}") String topico) {
        this.repository = repository;
        this.jsonMapper = jsonMapper;
        this.tracer = tracer;
        this.propagator = propagator;
        this.topico = topico;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void registrar(List<DomainEvent> eventos) {
        String correlationId = MDC.get(CorrelationIdFilter.MDC_KEY);
        for (DomainEvent evento : eventos) {
            if (!(evento instanceof ContratoEvent contratoEvent)) {
                throw new IllegalArgumentException("Evento não suportado: " + evento.eventType());
            }
            Span.Builder builder = tracer.spanBuilder()
                    .name(topico + " outbox " + evento.eventType())
                    .kind(Span.Kind.PRODUCER)
                    .remoteServiceName("kafka")
                    .tag("messaging.system", "kafka")
                    .tag("messaging.destination.name", topico)
                    .tag("eventId", evento.eventId().toString())
                    .tag("eventType", evento.eventType())
                    .tag("contratoId", contratoEvent.contratoId().toString())
                    .tag("correlationId", String.valueOf(correlationId));
            Span atual = tracer.currentSpan();
            if (atual != null) builder.setParent(atual.context());
            Span span = builder.start();
            try (var ignorado = tracer.withSpan(span)) {
                var mensagem = new EventoIntegracao(evento.eventId(), evento.eventType(), EventoIntegracao.VERSAO,
                        evento.occurredAt(), contratoEvent.contratoId(), correlationId, EventoIntegracao.SOURCE,
                        ContratoPayload.de(contratoEvent));
                repository.save(new OutboxEventoJpaEntity(evento.eventId(), AGGREGATE_TYPE,
                        contratoEvent.contratoId().toString(), evento.eventType(),
                        jsonMapper.writeValueAsString(mensagem), correlationId, traceparent(span), evento.occurredAt()));
                log.info("outbox.evento.registrado contratoId={} eventId={} eventType={} correlationId={} topico={}",
                        contratoEvent.contratoId(), evento.eventId(), evento.eventType(), correlationId, topico);
            } finally {
                span.end();
            }
        }
    }

    private String traceparent(Span span) {
        Map<String, String> contexto = new HashMap<>();
        propagator.inject(span.context(), contexto, Map::put);
        return contexto.get("traceparent");
    }
}
