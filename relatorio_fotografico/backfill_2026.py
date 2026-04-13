#!/usr/bin/env python3
"""
Backfill — gera todos os relatórios de auditorias aprovadas em 2026
e salva no Google Drive (sincronizado via Drive for Desktop), organizados por UF.

Comportamento:
- Cria pasta da UF se não existir (não duplica)
- Auditorias sem UF vão para pasta SEM_UF
- Sobrescreve arquivos existentes (versão mais recente)
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

from google.cloud import bigquery

BQ_PROJECT = "analytics-big-query-242119"
BQ_DATASET = "selo_polen_prod_gcp_fivetran_public"

DRIVE_BASE = Path(
    "/Users/marianatakagi/Library/CloudStorage/"
    "GoogleDrive-mariana@brpolen.com.br/Drives compartilhados/"
    "Operações/1. Operadores Logísticos - Polen (18 07 2023)/"
    "Relatórios Auditorias"
)

SCRIPT_PATH = Path(__file__).parent / "gerar_relatorio_fotografico.py"


def query_2026_audits() -> list[dict]:
    sql = f"""
    SELECT DISTINCT
      s.id AS subsidiary_id,
      s.cnpj,
      UPPER(TRIM(COALESCE(s.state, ''))) AS uf,
      c.name AS company_name
    FROM `{BQ_PROJECT}.{BQ_DATASET}.audits` a
    JOIN `{BQ_PROJECT}.{BQ_DATASET}.subsidiaries` s ON s.id = a.subsidiaryid
    JOIN `{BQ_PROJECT}.{BQ_DATASET}.companies` c ON c.id = s.companyid
    WHERE a._fivetran_deleted IS NOT TRUE
      AND s._fivetran_deleted IS NOT TRUE
      AND c._fivetran_deleted IS NOT TRUE
      AND a.status = 'Approved'
      AND EXTRACT(YEAR FROM a.aprovedat) = 2026
    ORDER BY uf, company_name
    """
    client = bigquery.Client(project=BQ_PROJECT)
    return [dict(r) for r in client.query(sql).result()]


def normalize_uf(uf: str) -> str:
    """Normaliza UF (ex: 'Rs' -> 'RS', 'Parana' -> 'PR'). Vazio -> SEM_UF."""
    uf = (uf or "").strip().upper()
    if not uf:
        return "SEM_UF"
    # Casos especiais — escrita por extenso
    fix = {
        "PARANA": "PR", "PARANÁ": "PR",
        "SAO PAULO": "SP", "SÃO PAULO": "SP",
        "RIO DE JANEIRO": "RJ",
        "MINAS GERAIS": "MG",
    }
    return fix.get(uf, uf if len(uf) == 2 else "SEM_UF")


def get_existing_uf_folders() -> dict[str, Path]:
    """Mapa UF (uppercase) -> Path da pasta existente."""
    result = {}
    if not DRIVE_BASE.exists():
        return result
    for child in DRIVE_BASE.iterdir():
        if child.is_dir():
            result[child.name.upper()] = child
    return result


def safe_folder_name(name: str, cnpj: str) -> str:
    """Pasta do operador: 'NOME OPERADOR - CNPJ'"""
    name_clean = re.sub(r"[^\w\s-]", "", name)[:80].strip()
    name_clean = re.sub(r"\s+", " ", name_clean)
    cnpj_clean = re.sub(r"\D", "", cnpj or "")
    return f"{name_clean} - {cnpj_clean}" if cnpj_clean else name_clean


def main():
    print(f"📂 Pasta destino: {DRIVE_BASE}")
    if not DRIVE_BASE.exists():
        print(f"❌ ERRO: pasta do Drive não encontrada. Drive for Desktop tá rodando?")
        sys.exit(1)

    print("🔍 Consultando auditorias aprovadas em 2026...")
    audits = query_2026_audits()
    print(f"   {len(audits)} encontradas\n")

    existing = get_existing_uf_folders()
    print(f"📁 UFs já existentes no Drive: {sorted(existing.keys())}\n")

    # Estatísticas
    success = 0
    failed = []
    skipped = []
    folders_created = []

    for i, a in enumerate(audits, 1):
        uf = normalize_uf(a["uf"])
        cnpj = a["cnpj"] or ""
        company = a["company_name"]
        sub_id = a["subsidiary_id"]

        # Pasta da UF (reutiliza existente ou cria nova)
        if uf in existing:
            uf_folder = existing[uf]
        else:
            uf_folder = DRIVE_BASE / uf
            uf_folder.mkdir(exist_ok=True)
            existing[uf] = uf_folder
            folders_created.append(uf)

        # Pasta do operador (UF/Nome - CNPJ/)
        op_folder = uf_folder / safe_folder_name(company, cnpj)
        op_folder.mkdir(exist_ok=True)

        prefix = f"[{i}/{len(audits)}] {uf} | {company[:50]}"
        print(prefix)

        # Gera DCO + Relatório Fotográfico
        any_failed = False
        for mode, filename in [("dco", "DCO.docx"),
                               ("fotografico", "Relatorio_Fotografico.docx")]:
            out_path = op_folder / filename
            try:
                result = subprocess.run(
                    [sys.executable, str(SCRIPT_PATH),
                     "--subsidiary-id", sub_id,
                     "--mode", mode,
                     "--output", str(out_path)],
                    capture_output=True, text=True, timeout=120,
                )
                if result.returncode == 0 and out_path.exists():
                    size_kb = out_path.stat().st_size / 1024
                    print(f"   ✅ {filename} ({size_kb:.0f}KB)")
                else:
                    err = (result.stderr or result.stdout)[-200:]
                    print(f"   ❌ {filename}: {err}")
                    failed.append((f"{company} [{mode}]", err))
                    any_failed = True
            except subprocess.TimeoutExpired:
                print(f"   ⏱️ {filename}: timeout")
                failed.append((f"{company} [{mode}]", "timeout"))
                any_failed = True
            except Exception as e:
                print(f"   ❌ {filename}: {e}")
                failed.append((f"{company} [{mode}]", str(e)))
                any_failed = True

        if not any_failed:
            success += 1

    # Resumo
    print("\n" + "=" * 60)
    print(f"✅ Sucesso: {success}/{len(audits)}")
    if folders_created:
        print(f"📁 Pastas criadas: {', '.join(sorted(folders_created))}")
    if failed:
        print(f"❌ Falhas: {len(failed)}")
        for company, err in failed[:10]:
            print(f"   • {company[:50]}: {err[:100]}")


if __name__ == "__main__":
    main()
