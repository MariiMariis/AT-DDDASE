package br.com.freela.reputacao;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record ReputacaoResponse(UUID freelancerId, int contratosIniciados, int contratosConcluidos,
                                int contratosCancelados, BigDecimal valorTotal, Instant atualizadoEm) {
    static ReputacaoResponse from(ReputacaoFreelancer r) {
        return new ReputacaoResponse(r.freelancerId, r.contratosIniciados, r.contratosConcluidos,
                r.contratosCancelados, r.valorTotal, r.atualizadoEm);
    }
}
