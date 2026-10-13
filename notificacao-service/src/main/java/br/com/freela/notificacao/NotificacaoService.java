package br.com.freela.notificacao;
import org.slf4j.*; import org.springframework.stereotype.Service; import org.springframework.transaction.annotation.Transactional; import java.util.ArrayList; import java.util.List; import java.util.UUID;
@Service
public class NotificacaoService {
 private static final Logger log=LoggerFactory.getLogger(NotificacaoService.class); private final NotificacaoRepository repository;
 public NotificacaoService(NotificacaoRepository r){this.repository=r;}

 @Transactional public int processar(EventoContrato evento){
  var destinatarios=destinatarios(evento);
  if(destinatarios.isEmpty()){
   log.info("notificacao.evento.ignorado eventId={} eventType={} contratoId={} resultado=SEM_DESTINATARIO",evento.eventId(),evento.eventType(),evento.contratoId());
   return 0;
  }
  for(var d:destinatarios){
   registrar(evento,d.id(),d.papel(),mensagem(evento,d.papel()));
  }
  return destinatarios.size();
 }

 @Transactional public void registrar(EventoContrato evento, UUID destinatarioId, String papel, String mensagem){
  log.info("notificacao.registro.inicio eventId={} contratoId={} destinatarioId={} papel={} tipo={}",evento.eventId(),evento.contratoId(),destinatarioId,papel,evento.eventType());
  var n=repository.save(new Notificacao(evento.eventId(),evento.contratoId(),destinatarioId,papel,evento.eventType(),mensagem,evento.correlationId()));
  log.info("notificacao.registro.sucesso notificacaoId={} eventId={} contratoId={} destinatarioId={} resultado=REGISTRADA",n.id,evento.eventId(),evento.contratoId(),destinatarioId);
 }

 private List<Destinatario> destinatarios(EventoContrato e){
  var cliente=new Destinatario(e.payload().clienteId(),"CLIENTE");
  var freelancer=new Destinatario(e.payload().freelancerId(),"FREELANCER");
  var lista=new ArrayList<Destinatario>();
  switch(e.eventType()){
   case "ContratoCriado","ContratoConcluido","ContratoCancelado" -> { lista.add(cliente); lista.add(freelancer); }
   case "EntregaRegistrada" -> lista.add(cliente);
   default -> {}
  }
  return lista;
 }

 private String mensagem(EventoContrato e, String papel){
  var titulo=e.payload().titulo();
  return switch(e.eventType()){
   case "ContratoCriado" -> papel.equals("CLIENTE")?"Seu contrato '"+titulo+"' foi criado.":"Você foi contratado para '"+titulo+"'.";
   case "EntregaRegistrada" -> "A entrega do contrato '"+titulo+"' foi registrada e aguarda sua conclusão.";
   case "ContratoConcluido" -> "O contrato '"+titulo+"' foi concluído.";
   case "ContratoCancelado" -> "O contrato '"+titulo+"' foi cancelado.";
   default -> "Atualização no contrato '"+titulo+"'.";
  };
 }

 private record Destinatario(UUID id, String papel){}
}
