"""
ledger/registry/client.py — Applicant Registry read-only client
===============================================================
COMPLETION STATUS: STUB — implement the query methods.

This client reads from the applicant_registry schema in PostgreSQL.
It is READ-ONLY. No agent or event store component ever writes here.
The Applicant Registry is the external CRM — seeded by datagen/generate_all.py.
"""
from __future__ import annotations
from dataclasses import dataclass
import asyncpg

@dataclass
class CompanyProfile:
    company_id: str; name: str; industry: str; naics: str
    jurisdiction: str; legal_type: str; founded_year: int
    employee_count: int; risk_segment: str; trajectory: str
    submission_channel: str; ip_region: str

@dataclass
class FinancialYear:
    fiscal_year: int; total_revenue: float; gross_profit: float
    operating_income: float; ebitda: float; net_income: float
    total_assets: float; total_liabilities: float; total_equity: float
    long_term_debt: float; cash_and_equivalents: float
    current_assets: float; current_liabilities: float
    accounts_receivable: float; inventory: float
    debt_to_equity: float; current_ratio: float
    debt_to_ebitda: float; interest_coverage_ratio: float
    gross_margin: float; ebitda_margin: float; net_margin: float

@dataclass
class ComplianceFlag:
    flag_type: str; severity: str; is_active: bool; added_date: str; note: str

class ApplicantRegistryClient:
    """
    READ-ONLY access to the Applicant Registry.
    Agents call these methods to get company profiles and historical data.
    Never write to this database from the event store system.
    """

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get_company(self, company_id: str) -> CompanyProfile | None:
        """SELECT * FROM applicant_registry.companies WHERE company_id = $1"""
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM applicant_registry.companies WHERE company_id = $1",
                    company_id,
                )
                if row is None:
                    return None
                return CompanyProfile(
                    company_id=row["company_id"],
                    name=row["name"],
                    industry=row["industry"],
                    naics=row["naics"],
                    jurisdiction=row["jurisdiction"],
                    legal_type=row["legal_type"],
                    founded_year=row["founded_year"],
                    employee_count=row["employee_count"],
                    risk_segment=row["risk_segment"],
                    trajectory=row["trajectory"],
                    submission_channel=row["submission_channel"],
                    ip_region=row["ip_region"],
                )
        except Exception:
            return None

    async def get_financial_history(self, company_id: str,
                                     years: list[int] | None = None) -> list[FinancialYear]:
        """
        SELECT * FROM applicant_registry.financial_history
        WHERE company_id = $1 [AND fiscal_year = ANY($2)]
        ORDER BY fiscal_year ASC
        """
        try:
            async with self._pool.acquire() as conn:
                if years:
                    rows = await conn.fetch(
                        "SELECT * FROM applicant_registry.financial_history"
                        " WHERE company_id = $1 AND fiscal_year = ANY($2)"
                        " ORDER BY fiscal_year ASC",
                        company_id, years,
                    )
                else:
                    rows = await conn.fetch(
                        "SELECT * FROM applicant_registry.financial_history"
                        " WHERE company_id = $1 ORDER BY fiscal_year ASC",
                        company_id,
                    )
                return [
                    FinancialYear(
                        fiscal_year=r["fiscal_year"],
                        total_revenue=float(r["total_revenue"]),
                        gross_profit=float(r["gross_profit"]),
                        operating_income=float(r["operating_income"]),
                        ebitda=float(r["ebitda"]),
                        net_income=float(r["net_income"]),
                        total_assets=float(r["total_assets"]),
                        total_liabilities=float(r["total_liabilities"]),
                        total_equity=float(r["total_equity"]),
                        long_term_debt=float(r["long_term_debt"]),
                        cash_and_equivalents=float(r["cash_and_equivalents"]),
                        current_assets=float(r["current_assets"]),
                        current_liabilities=float(r["current_liabilities"]),
                        accounts_receivable=float(r["accounts_receivable"]),
                        inventory=float(r["inventory"]),
                        debt_to_equity=float(r["debt_to_equity"]),
                        current_ratio=float(r["current_ratio"]),
                        debt_to_ebitda=float(r["debt_to_ebitda"]),
                        interest_coverage_ratio=float(r["interest_coverage_ratio"]),
                        gross_margin=float(r["gross_margin"]),
                        ebitda_margin=float(r["ebitda_margin"]),
                        net_margin=float(r["net_margin"]),
                    )
                    for r in rows
                ]
        except Exception:
            return []

    async def get_compliance_flags(self, company_id: str,
                                    active_only: bool = False) -> list[ComplianceFlag]:
        """
        SELECT * FROM applicant_registry.compliance_flags
        WHERE company_id = $1 [AND is_active = TRUE]
        """
        try:
            async with self._pool.acquire() as conn:
                if active_only:
                    rows = await conn.fetch(
                        "SELECT * FROM applicant_registry.compliance_flags"
                        " WHERE company_id = $1 AND is_active = TRUE",
                        company_id,
                    )
                else:
                    rows = await conn.fetch(
                        "SELECT * FROM applicant_registry.compliance_flags"
                        " WHERE company_id = $1",
                        company_id,
                    )
                return [
                    ComplianceFlag(
                        flag_type=r["flag_type"],
                        severity=r["severity"],
                        is_active=r["is_active"],
                        added_date=str(r["added_date"]),
                        note=r["note"] or "",
                    )
                    for r in rows
                ]
        except Exception:
            return []

    async def get_loan_relationships(self, company_id: str) -> list[dict]:
        """SELECT * FROM applicant_registry.loan_relationships WHERE company_id = $1"""
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT * FROM applicant_registry.loan_relationships"
                    " WHERE company_id = $1",
                    company_id,
                )
                return [dict(r) for r in rows]
        except Exception:
            return []
