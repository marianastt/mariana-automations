# mariana-automations

Automações pessoais — relatórios, scripts e integrações.

## Automações

### 📸 Relatório Fotográfico de Auditorias
Pasta: [`relatorio_fotografico/`](./relatorio_fotografico/)

Gera relatórios .docx das auditorias de operadores logísticos aprovadas no dia anterior, organiza por UF no Drive e notifica responsáveis no Slack.

- **Fonte:** BigQuery (`selo_polen_prod_gcp_fivetran_public`)
- **Output:** Documentos Word + mensagem consolidada no canal `#auditorias_operadores`
- **Frequência:** Diária, dias úteis às 9h
