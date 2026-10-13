package br.com.freela.auditoria;
import org.springframework.data.jpa.repository.JpaRepository; import org.springframework.data.jpa.repository.JpaSpecificationExecutor; import java.util.UUID;
interface EventoAuditoriaRepository extends JpaRepository<EventoAuditoria, UUID>, JpaSpecificationExecutor<EventoAuditoria>{
 boolean existsByEventId(UUID eventId);
}
