package br.com.freela.reputacao;
import jakarta.persistence.*; import java.math.BigDecimal; import java.time.Instant; import java.util.UUID;
@Entity @Table(name="reputacoes")
class ReputacaoFreelancer {
 @Id UUID freelancerId; int contratosIniciados; int contratosConcluidos; int contratosCancelados; BigDecimal valorTotal=BigDecimal.ZERO; Instant atualizadoEm;
 protected ReputacaoFreelancer(){} ReputacaoFreelancer(UUID id){this.freelancerId=id;}
 void registrarInicio(){contratosIniciados++; atualizadoEm=Instant.now();}
 void registrarContrato(BigDecimal valor){contratosConcluidos++; valorTotal=valorTotal.add(valor); atualizadoEm=Instant.now();}
 void registrarCancelamento(){contratosCancelados++; atualizadoEm=Instant.now();}
}
