package br.com.freela.auditoria;
import org.slf4j.*; import org.springframework.data.domain.Sort; import org.springframework.data.jpa.domain.Specification; import org.springframework.web.bind.annotation.*; import java.util.*;
@RestController @RequestMapping("/api/auditoria")
public class AuditoriaController {
 private static final Logger log=LoggerFactory.getLogger(AuditoriaController.class); private final EventoAuditoriaRepository repository;
 AuditoriaController(EventoAuditoriaRepository r){this.repository=r;}
 @GetMapping public List<EventoAuditoriaResponse> listar(@RequestParam(required=false) UUID eventId,
                                                         @RequestParam(required=false) UUID contratoId,
                                                         @RequestParam(required=false) String correlationId,
                                                         @RequestParam(required=false) String eventType){
  log.info("http.auditoria.listar eventId={} contratoId={} correlationId={} eventType={}",eventId,contratoId,correlationId,eventType);
  Specification<EventoAuditoria> spec=Specification.unrestricted();
  if(eventId!=null) spec=spec.and((r,q,cb)->cb.equal(r.get("eventId"),eventId));
  if(contratoId!=null) spec=spec.and((r,q,cb)->cb.equal(r.get("aggregateId"),contratoId));
  if(correlationId!=null) spec=spec.and((r,q,cb)->cb.equal(r.get("correlationId"),correlationId));
  if(eventType!=null) spec=spec.and((r,q,cb)->cb.equal(r.get("eventType"),eventType));
  return repository.findAll(spec,Sort.by("ocorridoEm")).stream().map(EventoAuditoriaResponse::from).toList();
 }
}
