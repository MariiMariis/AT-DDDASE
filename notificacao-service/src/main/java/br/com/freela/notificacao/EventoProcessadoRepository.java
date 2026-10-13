package br.com.freela.notificacao;
import org.springframework.data.jpa.repository.JpaRepository; import java.util.UUID;
interface EventoProcessadoRepository extends JpaRepository<EventoProcessado, UUID>{}
