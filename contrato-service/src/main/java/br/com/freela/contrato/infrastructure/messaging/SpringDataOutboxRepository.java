package br.com.freela.contrato.infrastructure.messaging;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;
public interface SpringDataOutboxRepository extends JpaRepository<OutboxEventoJpaEntity, UUID> {
    List<OutboxEventoJpaEntity> findByAggregateIdOrderByOccurredAtAsc(String aggregateId);
}
