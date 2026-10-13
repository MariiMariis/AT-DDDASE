package br.com.freela.notificacao;
import org.springframework.data.jpa.repository.JpaRepository; import java.util.List; import java.util.UUID;
interface NotificacaoRepository extends JpaRepository<Notificacao, UUID>{
 List<Notificacao> findByContratoIdOrderByCriadaEmAsc(UUID contratoId);
}
