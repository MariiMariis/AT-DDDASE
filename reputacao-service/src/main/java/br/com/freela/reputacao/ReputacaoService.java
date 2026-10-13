package br.com.freela.reputacao;
import org.slf4j.*; import org.springframework.stereotype.Service; import org.springframework.transaction.annotation.Transactional; import java.util.UUID; import java.util.function.Consumer;
@Service
public class ReputacaoService {
 private static final Logger log=LoggerFactory.getLogger(ReputacaoService.class); private final ReputacaoRepository repository; private final EventoProcessadoRepository processados;
 public ReputacaoService(ReputacaoRepository r, EventoProcessadoRepository p){this.repository=r;this.processados=p;}

 @Transactional public ResultadoProcessamento processar(EventoContrato evento){
  var p=evento.payload();
  if(processados.existsById(evento.eventId())){
   log.warn("reputacao.evento.duplicado eventId={} eventType={} contratoId={} freelancerId={} resultado=DUPLICADO_IGNORADO",evento.eventId(),evento.eventType(),evento.contratoId(),p.freelancerId());
   return ResultadoProcessamento.DUPLICADO;
  }
  var resultado=switch(evento.eventType()){
   case "ContratoCriado" -> atualizar(evento,"inicio",ReputacaoFreelancer::registrarInicio);
   case "ContratoConcluido" -> atualizar(evento,"conclusao",r->r.registrarContrato(p.valor()));
   case "ContratoCancelado" -> atualizar(evento,"cancelamento",ReputacaoFreelancer::registrarCancelamento);
   default -> {
    log.info("reputacao.evento.ignorado eventId={} eventType={} contratoId={} freelancerId={}",evento.eventId(),evento.eventType(),evento.contratoId(),p.freelancerId());
    yield ResultadoProcessamento.IGNORADO;
   }
  };
  processados.saveAndFlush(new EventoProcessado(evento));
  return resultado;
 }

 private ResultadoProcessamento atualizar(EventoContrato evento,String operacao,Consumer<ReputacaoFreelancer> alteracao){
  UUID freelancerId=evento.payload().freelancerId();
  log.info("reputacao.atualizacao.inicio eventId={} eventType={} contratoId={} freelancerId={} operacao={}",evento.eventId(),evento.eventType(),evento.contratoId(),freelancerId,operacao);
  var r=repository.findById(freelancerId).orElseGet(()->new ReputacaoFreelancer(freelancerId));
  alteracao.accept(r);
  repository.save(r);
  log.info("reputacao.atualizacao.sucesso eventId={} contratoId={} freelancerId={} contratosIniciados={} contratosConcluidos={} contratosCancelados={} valorTotal={}",
   evento.eventId(),evento.contratoId(),freelancerId,r.contratosIniciados,r.contratosConcluidos,r.contratosCancelados,r.valorTotal);
  return ResultadoProcessamento.PROCESSADO;
 }
}
