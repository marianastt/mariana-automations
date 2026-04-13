#!/usr/bin/env python3
"""
Daily runner — Relatório Fotográfico de Auditorias.

Consulta no BigQuery as auditorias aprovadas nas últimas 24h e gera uma
mensagem formatada para o Slack, agrupando por responsável HubSpot.

Saída:
  - Imprime no stdout um JSON com:
      {
        "summary": "Mensagem formatada para o Slack (texto)",
        "audits": [...lista de auditorias com metadados...],
        "missing_owner": [...auditorias sem responsável identificado...],
        "count": N
      }

Uso (local):
  export GOOGLE_APPLICATION_CREDENTIALS=./service-account-key.json
  python daily_runner.py [--days 1] [--channel C0AS7P24WLU]

No Cowork (remoto): o agente executa este script, lê o JSON de saída
e posta `summary` no canal #auditorias_operadores via Slack MCP.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

BQ_PROJECT = "analytics-big-query-242119"
BQ_DATASET = "selo_polen_prod_gcp_fivetran_public"
HUBSPOT_DATASET = "hubspot_fivetran"

DEFAULT_CHANNEL = "C0AS7P24WLU"  # #auditorias_operadores


def get_bq_client():
    from google.cloud import bigquery
    return bigquery.Client(project=BQ_PROJECT)


def query_approved_audits(days: int = 1) -> list[dict]:
    """Audits approved in the last N days (default 1 = last 24h)."""
    sql = f"""
    WITH approved AS (
      SELECT
        a.id AS audit_id,
        a.subsidiaryid AS subsidiary_id,
        a.aprovedat AS approved_at,
        s.cnpj,
        s.state AS uf,
        s.city,
        c.id AS company_id,
        c.name AS company_name
      FROM `{BQ_PROJECT}.{BQ_DATASET}.audits` a
      JOIN `{BQ_PROJECT}.{BQ_DATASET}.subsidiaries` s ON s.id = a.subsidiaryid
      JOIN `{BQ_PROJECT}.{BQ_DATASET}.companies` c ON c.id = s.companyid
      WHERE a._fivetran_deleted IS NOT TRUE
        AND s._fivetran_deleted IS NOT TRUE
        AND c._fivetran_deleted IS NOT TRUE
        AND a.status = 'Approved'
        AND a.aprovedat >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
    ),
    with_deal AS (
      SELECT
        ap.*,
        oc.deal_id
      FROM approved ap
      LEFT JOIN `{BQ_PROJECT}.{BQ_DATASET}.operatorcontracts` oc
        ON oc.subsidiaryid = ap.subsidiary_id
       AND oc._fivetran_deleted IS NOT TRUE
    )
    SELECT
      wd.audit_id,
      wd.subsidiary_id,
      wd.approved_at,
      wd.cnpj,
      wd.uf,
      wd.city,
      wd.company_name,
      wd.deal_id,
      d.property_dealname AS deal_name,
      d.owner_id,
      o.email AS owner_email,
      CONCAT(COALESCE(o.first_name, ''), ' ', COALESCE(o.last_name, '')) AS owner_name
    FROM with_deal wd
    LEFT JOIN `{BQ_PROJECT}.{HUBSPOT_DATASET}.deal` d
      ON CAST(d.deal_id AS STRING) = CAST(wd.deal_id AS STRING)
     AND d._fivetran_deleted IS NOT TRUE
    LEFT JOIN `{BQ_PROJECT}.{HUBSPOT_DATASET}.owner` o
      ON CAST(o.owner_id AS STRING) = CAST(d.owner_id AS STRING)
    ORDER BY wd.approved_at DESC
    """
    client = get_bq_client()
    rows = list(client.query(sql).result())
    return [dict(r) for r in rows]


def format_slack_message(audits: list[dict]) -> str:
    """Build the consolidated Slack message grouped by HubSpot owner."""
    if not audits:
        return ":sparkles: Nenhuma auditoria foi aprovada nas últimas 24h."

    by_owner: dict[str, list[dict]] = defaultdict(list)
    missing: list[dict] = []
    for a in audits:
        owner_name = (a.get("owner_name") or "").strip()
        if owner_name:
            by_owner[owner_name].append(a)
        else:
            missing.append(a)

    plural = "auditorias aprovadas" if len(audits) > 1 else "auditoria aprovada"
    lines: list[str] = [
        f":bell: *{len(audits)} {plural}* nas últimas 24h",
        "",
    ]

    for owner, items in sorted(by_owner.items()):
        # Slack mention by email if available
        owner_ref = owner
        email = (items[0].get("owner_email") or "").strip()
        if email:
            owner_ref = f"<mailto:{email}|{owner}>"
        lines.append(f"*Responsável: {owner_ref}*")
        for a in items:
            name = a.get("company_name") or "—"
            uf = a.get("uf") or "—"
            lines.append(f"• {name} · {uf}")
        lines.append("")

    if missing:
        lines.append(":warning: *Sem responsável identificado:*")
        for a in missing:
            name = a.get("company_name") or "—"
            uf = a.get("uf") or "—"
            lines.append(f"• {name} · {uf}")
        lines.append("")

    return "\n".join(lines).rstrip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=1,
                        help="Janela em dias (default: 1 = últimas 24h)")
    parser.add_argument("--channel", default=DEFAULT_CHANNEL,
                        help=f"Canal do Slack (default: {DEFAULT_CHANNEL})")
    args = parser.parse_args()

    audits = query_approved_audits(days=args.days)

    # Convert datetime objects to strings for JSON serialization
    for a in audits:
        for k, v in list(a.items()):
            if isinstance(v, (datetime, date)):
                a[k] = v.isoformat()

    summary = format_slack_message(audits)

    output = {
        "channel": args.channel,
        "summary": summary,
        "count": len(audits),
        "audits": audits,
        "missing_owner": [a for a in audits if not (a.get("owner_name") or "").strip()],
    }

    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
