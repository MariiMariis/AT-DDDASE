package br.com.freela.contrato.domain.event;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.model.StatusContrato;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record EntregaRegistrada(UUID eventId, Instant occurredAt, UUID contratoId,
        UUID clienteId, UUID freelancerId, String titulo, BigDecimal valor, StatusContrato status)
        implements ContratoEvent {
    public static EntregaRegistrada novo(Contrato c) {
        return new EntregaRegistrada(UUID.randomUUID(), Instant.now(), c.id(), c.clienteId(), c.freelancerId(), c.titulo(), c.valor(), c.status());
    }
    @Override public String eventType() { return "EntregaRegistrada"; }
}
