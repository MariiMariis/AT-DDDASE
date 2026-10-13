package br.com.freela.auditoria;
import jakarta.persistence.*; import java.time.Instant; import java.util.UUID;
@Entity @Table(name="auditoria_eventos", indexes={@Index(name="idx_auditoria_aggregate",columnList="aggregateId"),@Index(name="idx_auditoria_correlation",columnList="correlationId")}, uniqueConstraints=@UniqueConstraint(name="uk_auditoria_event_id",columnNames="eventId"))
class EventoAuditoria {
 @Id UUID id; UUID eventId; UUID aggregateId; String eventType; String correlationId; String origem; String topico; Integer particao; Long offsetKafka; @Column(columnDefinition="text") String payload; Instant ocorridoEm; Instant recebidoEm;
 protected EventoAuditoria(){}
 EventoAuditoria(UUID eventId,UUID aggregateId,String eventType,String correlationId,String origem,String topico,Integer particao,Long offsetKafka,String payload,Instant ocorridoEm){this.id=UUID.randomUUID();this.eventId=eventId;this.aggregateId=aggregateId;this.eventType=eventType;this.correlationId=correlationId;this.origem=origem;this.topico=topico;this.particao=particao;this.offsetKafka=offsetKafka;this.payload=payload;this.ocorridoEm=ocorridoEm;this.recebidoEm=Instant.now();}
}
