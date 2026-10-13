package br.com.freela.notificacao;
import jakarta.persistence.*; import java.time.Instant; import java.util.UUID;
@Entity @Table(name="notificacoes")
class Notificacao {
 @Id UUID id; UUID eventId; UUID contratoId; UUID destinatarioId; String papelDestinatario; String tipo; @Column(length=500) String mensagem; String correlationId; Instant criadaEm;
 protected Notificacao(){}
 Notificacao(UUID eventId, UUID contratoId, UUID destinatarioId, String papelDestinatario, String tipo, String mensagem, String correlationId){this.id=UUID.randomUUID();this.eventId=eventId;this.contratoId=contratoId;this.destinatarioId=destinatarioId;this.papelDestinatario=papelDestinatario;this.tipo=tipo;this.mensagem=mensagem;this.correlationId=correlationId;this.criadaEm=Instant.now();}
}
