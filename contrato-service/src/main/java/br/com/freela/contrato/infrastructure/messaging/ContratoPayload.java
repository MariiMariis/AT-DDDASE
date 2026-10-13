package br.com.freela.contrato.infrastructure.messaging;

import br.com.freela.contrato.domain.event.ContratoEvent;
import java.math.BigDecimal;
import java.util.UUID;

public record ContratoPayload(UUID clienteId, UUID freelancerId, String titulo, BigDecimal valor, String status) {
    static ContratoPayload de(ContratoEvent e) {
        return new ContratoPayload(e.clienteId(), e.freelancerId(), e.titulo(), e.valor(), e.status().name());
    }
}
