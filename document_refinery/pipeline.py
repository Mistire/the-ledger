"""
document_refinery/pipeline.py
==============================
Adapter that bridges the-ledger's DocumentProcessingAgent to the Week 3
Document Intelligence Refinery pipeline.

stub_agents.py calls:
    from document_refinery.pipeline import extract_financial_facts
    facts_dict = await extract_financial_facts(file_path, document_type)

This module translates that call into the Week 3 ExtractionRouter + FactExtractor
and returns a dict matching the FinancialFacts schema expected by the event store.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

# ── Locate the Week 3 package ────────────────────────────────────────────────
_WEEK3_ROOT = Path(__file__).parent.parent.parent / "week-3" / "document-intelligence-refinery"
if str(_WEEK3_ROOT) not in sys.path:
    sys.path.insert(0, str(_WEEK3_ROOT))


async def extract_financial_facts(file_path: str, document_type: str) -> dict[str, Any]:
    """
    Extract financial facts from a PDF using the Week 3 pipeline.

    Args:
        file_path: Absolute or relative path to the PDF file.
        document_type: "income_statement" or "balance_sheet"

    Returns:
        dict matching FinancialFacts fields:
            total_revenue, gross_profit, operating_income, net_income,
            ebitda, total_assets, total_liabilities, total_equity,
            cash_and_equivalents, fiscal_year, currency
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _extract_sync, file_path, document_type)


def _extract_sync(file_path: str, document_type: str) -> dict[str, Any]:
    """Synchronous extraction — runs in a thread pool via run_in_executor."""
    try:
        from src.agents.triage import TriageAgent
        from src.extraction.router import ExtractionRouter
        from src.agents.fact_extractor import FactExtractor
        from src.models.document_profile import DocumentProfile
    except ImportError as e:
        raise ImportError(
            f"Week 3 pipeline not importable from {_WEEK3_ROOT}. "
            f"Ensure the week-3/document-intelligence-refinery package is present. Error: {e}"
        )

    pdf_path = Path(file_path)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {file_path}")

    # Change working directory to Week 3 root so relative paths (config.yaml, etc.) resolve
    original_cwd = os.getcwd()
    try:
        os.chdir(_WEEK3_ROOT)

        # Step 1: Triage — profile the document
        triage = TriageAgent()
        profile: DocumentProfile = triage.triage(str(pdf_path.resolve()))

        # Step 2: Extract — route through A/B/C strategies
        router = ExtractionRouter(config_path="config.yaml")
        extracted_doc = router.route_and_extract(profile)

        # Step 3: Fact extraction — pull financial figures via LLM
        fact_extractor = FactExtractor()
        facts = fact_extractor.extract_facts(extracted_doc)

    finally:
        os.chdir(original_cwd)

    # ── Map FactEntry list → FinancialFacts dict ──────────────────────────────
    # Build a lookup: normalised key → value
    fact_lookup: dict[str, Any] = {}
    for f in facts:
        key_lower = f.key.lower().replace(" ", "_").replace("-", "_")
        fact_lookup[key_lower] = _parse_numeric(f.value)

    # Map common aliases to FinancialFacts field names
    field_map = {
        "total_revenue":       ["total_revenue", "revenue", "net_revenue", "total_sales", "sales"],
        "gross_profit":        ["gross_profit", "gross_income"],
        "operating_income":    ["operating_income", "operating_profit", "ebit"],
        "net_income":          ["net_income", "net_profit", "profit_after_tax", "net_earnings"],
        "ebitda":              ["ebitda", "earnings_before_interest_taxes_depreciation_amortization"],
        "total_assets":        ["total_assets", "assets"],
        "total_liabilities":   ["total_liabilities", "liabilities"],
        "total_equity":        ["total_equity", "shareholders_equity", "equity", "net_assets"],
        "cash_and_equivalents":["cash_and_equivalents", "cash", "cash_and_cash_equivalents"],
    }

    result: dict[str, Any] = {
        "fiscal_year": _extract_fiscal_year(facts),
        "currency": "USD",
        "document_type": document_type,
    }

    for field, aliases in field_map.items():
        for alias in aliases:
            if alias in fact_lookup:
                result[field] = fact_lookup[alias]
                break
        else:
            result[field] = None  # genuinely unknown — do not fabricate

    # Include raw facts for audit trail
    result["_raw_facts"] = [
        {"key": f.key, "value": f.value, "page": f.page_number, "confidence": f.confidence}
        for f in facts
    ]

    return result


def _parse_numeric(value: str) -> float | None:
    """Parse a value string like '$1,234,567' or '1.2M' into a float."""
    if value is None:
        return None
    s = str(value).strip().replace(",", "").replace("$", "").replace(" ", "")
    multipliers = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}
    if s and s[-1].lower() in multipliers:
        try:
            return float(s[:-1]) * multipliers[s[-1].lower()]
        except ValueError:
            pass
    try:
        return float(s)
    except ValueError:
        return None


def _extract_fiscal_year(facts) -> int | None:
    """Try to extract fiscal year from fact periods."""
    import re
    for f in facts:
        if f.period:
            m = re.search(r"(20\d{2})", str(f.period))
            if m:
                return int(m.group(1))
    return None
