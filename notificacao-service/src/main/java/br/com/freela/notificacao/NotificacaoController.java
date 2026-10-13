package br.com.freela.notificacao;
import org.slf4j.*; import org.springframework.web.bind.annotation.*; import java.util.*;
@RestController @RequestMapping("/api/notificacoes")
public class NotificacaoController {
 private static final Logger log=LoggerFactory.getLogger(NotificacaoController.class); private final NotificacaoRepository repository;
 NotificacaoController(NotificacaoRepository r){this.repository=r;}
 @GetMapping public List<NotificacaoResponse> listar(@RequestParam(required=false) UUID contratoId){
  log.info("http.notificacao.listar contratoId={}",contratoId);
  var notificacoes=contratoId==null?repository.findAll():repository.findByContratoIdOrderByCriadaEmAsc(contratoId);
  return notificacoes.stream().map(NotificacaoResponse::from).toList();
 }
}
