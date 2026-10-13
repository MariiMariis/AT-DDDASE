package br.com.freela.contrato.domain.event;

import br.com.freela.contrato.domain.model.StatusContrato;
import br.com.freela.contrato.domain.shared.DomainEvent;
import java.math.BigDecimal;
import java.util.UUID;

public sealed interface ContratoEvent extends DomainEvent
        permits ContratoCriado, EntregaRegistrada, ContratoConcluido, ContratoCancelado {
    UUID contratoId();
    UUID clienteId();
    UUID freelancerId();
    String titulo();
    BigDecimal valor();
    StatusContrato status();
    @Override default UUID aggregateId() { return contratoId(); }
}
