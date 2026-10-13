package br.com.freela.reputacao;
import org.slf4j.*; import org.springframework.http.ResponseEntity; import org.springframework.web.bind.annotation.*; import java.util.*;
@RestController @RequestMapping("/api/reputacoes")
public class ReputacaoController {
 private static final Logger log=LoggerFactory.getLogger(ReputacaoController.class); private final ReputacaoRepository repository;
 ReputacaoController(ReputacaoRepository r){this.repository=r;}
 @GetMapping public List<ReputacaoResponse> listar(){log.info("http.reputacao.listar");return repository.findAll().stream().map(ReputacaoResponse::from).toList();}
 @GetMapping("/{freelancerId}") public ResponseEntity<ReputacaoResponse> buscar(@PathVariable UUID freelancerId){
  log.info("http.reputacao.buscar freelancerId={}",freelancerId);
  return repository.findById(freelancerId).map(ReputacaoResponse::from).map(ResponseEntity::ok).orElseGet(()->ResponseEntity.notFound().build());
 }
}
