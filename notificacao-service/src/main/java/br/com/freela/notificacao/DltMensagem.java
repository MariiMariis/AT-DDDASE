package br.com.freela.notificacao;

public record DltMensagem(int particaoDlt, long offsetDlt, String key, String eventId, String eventType,
                          String topicoOriginal, Integer particaoOriginal, Long offsetOriginal,
                          String excecao, String motivo, boolean pendente, String mensagem) {}
