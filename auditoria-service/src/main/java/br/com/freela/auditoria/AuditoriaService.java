package br.com.freela.auditoria;
import org.slf4j.*; import org.springframework.stereotype.Service; import org.springframework.transaction.annotation.Transactional;
@Service
public class AuditoriaService {
 private static final Logger log=LoggerFactory.getLogger(AuditoriaService.class); private final EventoAuditoriaRepository repository;
 public AuditoriaService(EventoAuditoriaRepository r){this.repository=r;}
 @Transactional public void registrar(EventoContrato evento,String topico,int particao,long offset,String payload){
  log.info("auditoria.registro.inicio eventId={} contratoId={} eventType={} correlationId={}",evento.eventId(),evento.contratoId(),evento.eventType(),evento.correlationId());
  var e=repository.save(new EventoAuditoria(evento.eventId(),evento.contratoId(),evento.eventType(),evento.correlationId(),evento.source(),topico,particao,offset,payload,evento.occurredAt()));
  log.info("auditoria.registro.sucesso auditoriaId={} eventId={} contratoId={} eventType={}",e.id,evento.eventId(),evento.contratoId(),evento.eventType());
 }
}
