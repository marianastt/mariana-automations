# Relatório Fotográfico — Auditorias de Operadores

Gera relatórios .docx das auditorias aprovadas no Selo Polen e notifica os responsáveis HubSpot via Slack.

## Como funciona
1. Consulta no BigQuery as auditorias com `status = 'Approved'` aprovadas nas últimas 24h
2. Para cada auditoria, gera um .docx com os dados estruturados (infraestrutura, equipamentos, EPI, treinamentos, etc.)
3. Salva os arquivos em pastas por UF no Google Drive compartilhado
4. Posta uma mensagem consolidada no canal `#auditorias_operadores` agrupando os relatórios por responsável HubSpot

## Dependências
- Python 3.11+
- `google-cloud-bigquery`, `python-docx`
- Acesso ao BigQuery (`analytics-big-query-242119`) via service account
- Pasta sincronizada do Drive ou API do Drive

## Variáveis de ambiente
- `GOOGLE_APPLICATION_CREDENTIALS` — caminho para o service account key (JSON)

## Status
🚧 Em desenvolvimento — script gerador funcional, automação de schedule em construção.
