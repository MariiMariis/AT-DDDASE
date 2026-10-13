package br.com.freela.reputacao;
import org.slf4j.*; import org.springframework.stereotype.Service; import org.springframework.transaction.annotation.Transactional; import java.util.UUID; import java.util.function.Consumer;
@Service
public class ReputacaoService {
 private static final Logger log=LoggerFactory.getLogger(ReputacaoService.class); private final ReputacaoRepository repository;
 public ReputacaoService(ReputacaoRepository r){this.repository=r;}

 @Transactional public boolean processar(EventoContrato evento){
  var p=evento.payload();
  return switch(evento.eventType()){
   case "ContratoCriado" -> atualizar(evento,"inicio",ReputacaoFreelancer::registrarInicio);
   case "ContratoConcluido" -> atualizar(evento,"conclusao",r->r.registrarContrato(p.valor()));
   case "ContratoCancelado" -> atualizar(evento,"cancelamento",ReputacaoFreelancer::registrarCancelamento);
   default -> {
    log.info("reputacao.evento.ignorado eventId={} eventType={} contratoId={} freelancerId={}",evento.eventId(),evento.eventType(),evento.contratoId(),p.freelancerId());
    yield false;
   }
  };
 }

 private boolean atualizar(EventoContrato evento,String operacao,Consumer<ReputacaoFreelancer> alteracao){
  UUID freelancerId=evento.payload().freelancerId();
  log.info("reputacao.atualizacao.inicio eventId={} eventType={} contratoId={} freelancerId={} operacao={}",evento.eventId(),evento.eventType(),evento.contratoId(),freelancerId,operacao);
  var r=repository.findById(freelancerId).orElseGet(()->new ReputacaoFreelancer(freelancerId));
  alteracao.accept(r);
  repository.save(r);
  log.info("reputacao.atualizacao.sucesso eventId={} contratoId={} freelancerId={} contratosIniciados={} contratosConcluidos={} contratosCancelados={} valorTotal={}",
   evento.eventId(),evento.contratoId(),freelancerId,r.contratosIniciados,r.contratosConcluidos,r.contratosCancelados,r.valorTotal);
  return true;
 }
}
