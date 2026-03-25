"""
ledger/agents/stub_agents.py
============================
Full implementations for DocumentProcessingAgent, FraudDetectionAgent,
ComplianceAgent, and DecisionOrchestratorAgent.
"""
from __future__ import annotations
import dataclasses
import json
import time
from datetime import datetime
from decimal import Decimal
from typing import TypedDict
from uuid import uuid4

from langgraph.graph import StateGraph, END

from ledger.agents.base_agent import BaseApexAgent
from ledger.schema.events import (
    # Document package
    DocumentFormatValidated, DocumentFormatRejected,
    ExtractionStarted, ExtractionCompleted, ExtractionFailed,
    QualityAssessmentCompleted, PackageReadyForAnalysis,
    # Loan application
    CreditAnalysisRequested, ComplianceCheckRequested, DecisionRequested,
    ApplicationDeclined, ApplicationApproved, HumanReviewRequested,
    # Fraud
    FraudScreeningInitiated, FraudAnomalyDetected, FraudScreeningCompleted,
    FraudAnomaly, FraudAnomalyType,
    # Compliance
    ComplianceCheckInitiated, ComplianceRulePassed, ComplianceRuleFailed,
    ComplianceRuleNoted, ComplianceCheckCompleted, ComplianceVerdict,
    # Decision
    DecisionGenerated,
    # Value objects
    FinancialFacts, DocumentType,
)


# ─── DOCUMENT PROCESSING AGENT ───────────────────────────────────────────────

class DocProcState(TypedDict):
    application_id: str
    session_id: str
    document_ids: list[str] | None
    document_paths: list[str] | None
    extraction_results: list[dict] | None
    quality_assessment: dict | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


class DocumentProcessingAgent(BaseApexAgent):
    """
    Wraps the Week 3 Document Intelligence pipeline.
    Nodes: validate_inputs → validate_document_formats → extract_income_statement
           → extract_balance_sheet → assess_quality → write_output
    """

    def build_graph(self):
        g = StateGraph(DocProcState)
        g.add_node("validate_inputs",           self._node_validate_inputs)
        g.add_node("validate_document_formats", self._node_validate_formats)
        g.add_node("extract_income_statement",  self._node_extract_is)
        g.add_node("extract_balance_sheet",     self._node_extract_bs)
        g.add_node("assess_quality",            self._node_assess_quality)
        g.add_node("write_output",              self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",           "validate_document_formats")
        g.add_edge("validate_document_formats", "extract_income_statement")
        g.add_edge("extract_income_statement",  "extract_balance_sheet")
        g.add_edge("extract_balance_sheet",     "assess_quality")
        g.add_edge("assess_quality",            "write_output")
        g.add_edge("write_output",              END)
        return g.compile()

    def _initial_state(self, application_id: str) -> DocProcState:
        return DocProcState(
            application_id=application_id, session_id=self.session_id,
            document_ids=None, document_paths=None,
            extraction_results=None, quality_assessment=None,
            errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state: DocProcState) -> DocProcState:
        t = time.time()
        app_id = state["application_id"]
        # Load DocumentUploaded events from loan stream
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        uploaded = [e for e in loan_events if e["event_type"] == "DocumentUploaded"]

        doc_ids = [e["payload"]["document_id"] for e in uploaded]
        doc_paths = [e["payload"].get("file_path", "") for e in uploaded]
        doc_types = {e["payload"].get("document_type", "") for e in uploaded}

        required = {"income_statement", "balance_sheet"}
        missing = required - doc_types
        ms = int((time.time() - t) * 1000)

        if missing:
            await self._record_input_failed(list(missing), [f"Missing required document types: {missing}"])
            raise ValueError(f"Missing required documents: {missing}")

        await self._record_input_validated(["application_id", "document_ids", "file_paths"], ms)
        await self._record_node_execution(
            "validate_inputs", ["application_id"], ["document_ids", "document_paths"], ms
        )
        return {**state, "document_ids": doc_ids, "document_paths": doc_paths}

    async def _node_validate_formats(self, state: DocProcState) -> DocProcState:
        t = time.time()
        app_id = state["application_id"]
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        uploaded = [e for e in loan_events if e["event_type"] == "DocumentUploaded"]

        for ev in uploaded:
            p = ev["payload"]
            doc_id = p["document_id"]
            doc_type = p.get("document_type", "unknown")
            detected_format = p.get("document_format", "pdf")
            page_count = 1  # default; real impl would inspect file

            fmt_event = DocumentFormatValidated(
                package_id=app_id,
                document_id=doc_id,
                document_type=doc_type,
                page_count=page_count,
                detected_format=detected_format,
                validated_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"docpkg-{app_id}", [fmt_event])

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution(
            "validate_document_formats", ["document_ids"], ["format_validated"], ms
        )
        return state

    async def _node_extract_is(self, state: DocProcState) -> DocProcState:
        t = time.time()
        app_id = state["application_id"]
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        is_events = [
            e for e in loan_events
            if e["event_type"] == "DocumentUploaded"
            and e["payload"].get("document_type") == "income_statement"
        ]

        results = list(state.get("extraction_results") or [])

        for ev in is_events:
            p = ev["payload"]
            doc_id = p["document_id"]
            file_path = p.get("file_path", "")

            start_event = ExtractionStarted(
                package_id=app_id, document_id=doc_id,
                document_type="income_statement",
                pipeline_version="1.0", extraction_model="mineru-1.0",
                started_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"docpkg-{app_id}", [start_event])

            facts_dict: dict = {}
            try:
                from document_refinery.pipeline import extract_financial_facts  # type: ignore
                facts_dict = await extract_financial_facts(file_path, "income_statement")
            except Exception as exc:
                fail_event = ExtractionFailed(
                    package_id=app_id, document_id=doc_id,
                    error_type=type(exc).__name__,
                    error_message=str(exc)[:500],
                    partial_facts=None,
                    failed_at=datetime.now(),
                ).to_store_dict()
                await self._append_with_retry(f"docpkg-{app_id}", [fail_event])
                raise

            facts = FinancialFacts(**{k: v for k, v in facts_dict.items() if hasattr(FinancialFacts, k)})
            done_event = ExtractionCompleted(
                package_id=app_id, document_id=doc_id,
                document_type="income_statement",
                facts=facts,
                raw_text_length=len(str(facts_dict)),
                tables_extracted=1,
                processing_ms=int((time.time() - t) * 1000),
                completed_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"docpkg-{app_id}", [done_event])
            results.append({"document_id": doc_id, "document_type": "income_statement", "facts": facts_dict})

        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("week3_extraction_pipeline", f"income_statement docs={len(is_events)}", f"extracted {len(results)} results", ms)
        await self._record_node_execution("extract_income_statement", ["document_paths"], ["extraction_results"], ms)
        return {**state, "extraction_results": results}

    async def _node_extract_bs(self, state: DocProcState) -> DocProcState:
        t = time.time()
        app_id = state["application_id"]
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        bs_events = [
            e for e in loan_events
            if e["event_type"] == "DocumentUploaded"
            and e["payload"].get("document_type") == "balance_sheet"
        ]

        results = list(state.get("extraction_results") or [])

        for ev in bs_events:
            p = ev["payload"]
            doc_id = p["document_id"]
            file_path = p.get("file_path", "")

            start_event = ExtractionStarted(
                package_id=app_id, document_id=doc_id,
                document_type="balance_sheet",
                pipeline_version="1.0", extraction_model="mineru-1.0",
                started_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"docpkg-{app_id}", [start_event])

            facts_dict: dict = {}
            try:
                from document_refinery.pipeline import extract_financial_facts  # type: ignore
                facts_dict = await extract_financial_facts(file_path, "balance_sheet")
            except Exception as exc:
                fail_event = ExtractionFailed(
                    package_id=app_id, document_id=doc_id,
                    error_type=type(exc).__name__,
                    error_message=str(exc)[:500],
                    partial_facts=None,
                    failed_at=datetime.now(),
                ).to_store_dict()
                await self._append_with_retry(f"docpkg-{app_id}", [fail_event])
                raise

            facts = FinancialFacts(**{k: v for k, v in facts_dict.items() if hasattr(FinancialFacts, k)})
            done_event = ExtractionCompleted(
                package_id=app_id, document_id=doc_id,
                document_type="balance_sheet",
                facts=facts,
                raw_text_length=len(str(facts_dict)),
                tables_extracted=1,
                processing_ms=int((time.time() - t) * 1000),
                completed_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"docpkg-{app_id}", [done_event])
            results.append({"document_id": doc_id, "document_type": "balance_sheet", "facts": facts_dict})

        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("week3_extraction_pipeline", f"balance_sheet docs={len(bs_events)}", f"extracted {len(results)} results", ms)
        await self._record_node_execution("extract_balance_sheet", ["document_paths"], ["extraction_results"], ms)
        return {**state, "extraction_results": results}

    async def _node_assess_quality(self, state: DocProcState) -> DocProcState:
        t = time.time()
        app_id = state["application_id"]
        results = state.get("extraction_results") or []

        # Merge all extracted facts
        merged: dict = {}
        for r in results:
            for k, v in (r.get("facts") or {}).items():
                if v is not None and k not in merged:
                    merged[k] = v

        SYSTEM = """You are a financial document quality analyst.
Check the extracted financial facts for internal consistency. Do NOT make credit decisions.
Return ONLY a JSON object:
{
  "overall_confidence": <float 0-1>,
  "is_coherent": <bool>,
  "anomalies": ["<anomaly description>"],
  "critical_missing_fields": ["<field name>"],
  "reextraction_recommended": <bool>,
  "auditor_notes": "<brief notes>"
}
Check: Assets = Liabilities + Equity (within 1%), margins are plausible, EBITDA > 0 if profitable."""

        USER = f"""Extracted financial facts:
{json.dumps({k: str(v) for k, v in merged.items() if v is not None}, indent=2)}"""

        ti = to = 0
        cost = 0.0
        try:
            content, ti, to, cost = await self._call_llm(SYSTEM, USER, max_tokens=512)
            qa = self._parse_json(content)
        except Exception as exc:
            qa = {
                "overall_confidence": 0.7,
                "is_coherent": True,
                "anomalies": [],
                "critical_missing_fields": [],
                "reextraction_recommended": False,
                "auditor_notes": f"Quality assessment unavailable: {exc!s:.80}",
            }

        # Use first doc_id or app_id as package doc_id
        doc_id = results[0]["document_id"] if results else "unknown"
        qa_event = QualityAssessmentCompleted(
            package_id=app_id,
            document_id=doc_id,
            overall_confidence=float(qa.get("overall_confidence", 0.7)),
            is_coherent=bool(qa.get("is_coherent", True)),
            anomalies=qa.get("anomalies", []),
            critical_missing_fields=qa.get("critical_missing_fields", []),
            reextraction_recommended=bool(qa.get("reextraction_recommended", False)),
            auditor_notes=qa.get("auditor_notes", ""),
            assessed_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"docpkg-{app_id}", [qa_event])

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("assess_quality", ["extraction_results"], ["quality_assessment"], ms, ti, to, cost)
        return {**state, "quality_assessment": qa}

    async def _node_write_output(self, state: DocProcState) -> DocProcState:
        t = time.time()
        app_id = state["application_id"]
        results = state.get("extraction_results") or []
        qa = state.get("quality_assessment") or {}

        ready_event = PackageReadyForAnalysis(
            package_id=app_id,
            application_id=app_id,
            documents_processed=len(results),
            has_quality_flags=bool(qa.get("anomalies") or qa.get("critical_missing_fields")),
            quality_flag_count=len(qa.get("anomalies", [])) + len(qa.get("critical_missing_fields", [])),
            ready_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"docpkg-{app_id}", [ready_event])

        credit_req = CreditAnalysisRequested(
            application_id=app_id,
            requested_at=datetime.now(),
            requested_by=self.session_id,
        ).to_store_dict()
        await self._append_with_retry(f"loan-{app_id}", [credit_req])

        events_written = [
            {"stream_id": f"docpkg-{app_id}", "event_type": "PackageReadyForAnalysis"},
            {"stream_id": f"loan-{app_id}", "event_type": "CreditAnalysisRequested"},
        ]
        await self._record_output_written(events_written, f"Package ready: {len(results)} docs processed. CreditAnalysisRequested triggered.")
        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("write_output", ["quality_assessment"], ["events_written"], ms)
        return {**state, "output_events": events_written, "next_agent": "credit_analysis"}


# ─── FRAUD DETECTION AGENT ───────────────────────────────────────────────────

class FraudState(TypedDict):
    application_id: str
    session_id: str
    extracted_facts: dict | None
    registry_profile: dict | None
    historical_financials: list[dict] | None
    fraud_signals: list[dict] | None
    fraud_score: float | None
    anomalies: list[dict] | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


class FraudDetectionAgent(BaseApexAgent):
    """
    Cross-references extracted document facts against historical registry data.
    Nodes: validate_inputs → load_document_facts → cross_reference_registry
           → analyze_fraud_patterns → write_output
    """

    def build_graph(self):
        g = StateGraph(FraudState)
        g.add_node("validate_inputs",          self._node_validate_inputs)
        g.add_node("load_document_facts",      self._node_load_facts)
        g.add_node("cross_reference_registry", self._node_cross_reference)
        g.add_node("analyze_fraud_patterns",   self._node_analyze)
        g.add_node("write_output",             self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",          "load_document_facts")
        g.add_edge("load_document_facts",      "cross_reference_registry")
        g.add_edge("cross_reference_registry", "analyze_fraud_patterns")
        g.add_edge("analyze_fraud_patterns",   "write_output")
        g.add_edge("write_output",             END)
        return g.compile()

    def _initial_state(self, application_id: str) -> FraudState:
        return FraudState(
            application_id=application_id, session_id=self.session_id,
            extracted_facts=None, registry_profile=None, historical_financials=None,
            fraud_signals=None, fraud_score=None, anomalies=None,
            errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state: FraudState) -> FraudState:
        t = time.time()
        app_id = state["application_id"]
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        has_trigger = any(e["event_type"] == "FraudScreeningRequested" for e in loan_events)
        ms = int((time.time() - t) * 1000)
        if not has_trigger:
            await self._record_input_failed(["FraudScreeningRequested"], ["No FraudScreeningRequested event found on loan stream"])
            raise ValueError("FraudScreeningRequested event not found")
        await self._record_input_validated(["application_id", "FraudScreeningRequested"], ms)
        await self._record_node_execution("validate_inputs", ["application_id"], ["validated"], ms)
        return state

    async def _node_load_facts(self, state: FraudState) -> FraudState:
        t = time.time()
        app_id = state["application_id"]
        pkg_events = await self.store.load_stream(f"docpkg-{app_id}")
        extraction_events = [e for e in pkg_events if e["event_type"] == "ExtractionCompleted"]

        merged: dict = {}
        for ev in extraction_events:
            facts = ev["payload"].get("facts") or {}
            for k, v in facts.items():
                if v is not None and k not in merged:
                    merged[k] = v

        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("load_event_store_stream", f"docpkg-{app_id}", f"{len(extraction_events)} ExtractionCompleted events", ms)
        await self._record_node_execution("load_document_facts", ["docpkg_stream"], ["extracted_facts"], ms)
        return {**state, "extracted_facts": merged}

    async def _node_cross_reference(self, state: FraudState) -> FraudState:
        t = time.time()
        app_id = state["application_id"]
        facts = state.get("extracted_facts") or {}

        # Load loan application to get applicant_id
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        submitted = next((e for e in loan_events if e["event_type"] == "ApplicationSubmitted"), None)
        applicant_id = submitted["payload"].get("applicant_id", "COMP-001") if submitted else "COMP-001"

        # Query registry
        profile_obj = await self.registry.get_company(applicant_id)
        fin_objs    = await self.registry.get_financial_history(applicant_id)

        profile    = dataclasses.asdict(profile_obj) if profile_obj else {"company_id": applicant_id, "trajectory": "STABLE"}
        financials = [dataclasses.asdict(f) for f in fin_objs]

        # Compute revenue_discrepancy_factor deterministically
        fraud_signals: list[dict] = []
        revenue_discrepancy_factor = 0.0
        submission_pattern_factor = 0.0
        balance_sheet_consistency = 0.0

        doc_revenue = float(facts.get("total_revenue") or 0)
        if financials and doc_revenue > 0:
            prior = financials[-1]
            registry_revenue = float(prior.get("total_revenue") or 0)
            if registry_revenue > 0:
                gap = abs(doc_revenue - registry_revenue) / registry_revenue
                trajectory = profile.get("trajectory", "STABLE")
                if gap > 0.40 and trajectory not in ("GROWTH", "RECOVERING"):
                    revenue_discrepancy_factor = 0.25
                    fraud_signals.append({
                        "type": "revenue_discrepancy",
                        "severity": "HIGH",
                        "gap_pct": round(gap * 100, 1),
                        "doc_revenue": doc_revenue,
                        "registry_revenue": registry_revenue,
                    })

        # Submission pattern factor: unusual channel or IP region
        channel = profile.get("submission_channel", "web")
        ip_region = profile.get("ip_region", "US")
        if channel == "api" and ip_region not in ("US", "CA", "GB"):
            submission_pattern_factor = 0.10
            fraud_signals.append({"type": "unusual_submission_pattern", "severity": "MEDIUM", "channel": channel, "ip_region": ip_region})

        # Balance sheet consistency
        total_assets = float(facts.get("total_assets") or 0)
        total_liabilities = float(facts.get("total_liabilities") or 0)
        total_equity = float(facts.get("total_equity") or 0)
        if total_assets > 0 and total_liabilities > 0 and total_equity > 0:
            discrepancy = abs(total_assets - (total_liabilities + total_equity))
            if discrepancy / total_assets > 0.01:
                balance_sheet_consistency = 0.15
                fraud_signals.append({"type": "balance_sheet_inconsistency", "severity": "MEDIUM", "discrepancy_usd": discrepancy})

        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("query_applicant_registry", f"company_id={applicant_id}", f"profile + {len(financials)} fiscal years", ms)
        await self._record_node_execution("cross_reference_registry", ["extracted_facts"], ["fraud_signals", "registry_profile"], ms)
        return {
            **state,
            "registry_profile": profile,
            "historical_financials": financials,
            "fraud_signals": fraud_signals,
            "_revenue_discrepancy_factor": revenue_discrepancy_factor,
            "_submission_pattern_factor": submission_pattern_factor,
            "_balance_sheet_consistency": balance_sheet_consistency,
        }

    async def _node_analyze(self, state: FraudState) -> FraudState:
        t = time.time()
        facts    = state.get("extracted_facts") or {}
        signals  = state.get("fraud_signals") or []
        profile  = state.get("registry_profile") or {}
        hist     = state.get("historical_financials") or []

        # Base score from deterministic factors
        base = 0.05
        rev_factor  = state.get("_revenue_discrepancy_factor", 0.0)
        sub_factor  = state.get("_submission_pattern_factor", 0.0)
        bs_factor   = state.get("_balance_sheet_consistency", 0.0)
        computed_score = base + rev_factor + sub_factor + bs_factor

        SYSTEM = """You are a financial fraud analyst.
Given cross-reference results, identify specific named anomalies.
For each anomaly: type, severity (LOW/MEDIUM/HIGH), evidence, affected_fields.
Also compute a final fraud_score between 0.0 and 1.0.
Return ONLY JSON:
{
  "fraud_score": <float 0-1>,
  "anomalies": [{"type": "<type>", "severity": "<LOW|MEDIUM|HIGH>", "evidence": "<text>", "affected_fields": ["<field>"]}],
  "recommendation": "<PROCEED|FLAG_FOR_REVIEW|DECLINE>"
}"""

        USER = f"""Cross-reference signals: {json.dumps(signals)}
Computed base fraud score: {computed_score:.3f}
Company profile: {json.dumps({k: profile.get(k) for k in ['trajectory', 'submission_channel', 'ip_region', 'industry']})}
Historical financials (last year): {json.dumps(hist[-1] if hist else {})}
Extracted facts: {json.dumps({k: str(v) for k, v in facts.items() if v is not None})}"""

        ti = to = 0
        cost = 0.0
        try:
            content, ti, to, cost = await self._call_llm(SYSTEM, USER, max_tokens=512)
            assessment = self._parse_json(content)
        except Exception as exc:
            assessment = {
                "fraud_score": computed_score,
                "anomalies": [{"type": s["type"], "severity": s["severity"], "evidence": str(s), "affected_fields": []} for s in signals],
                "recommendation": "FLAG_FOR_REVIEW" if computed_score >= 0.30 else "PROCEED",
            }

        # Use LLM score but floor at computed_score
        fraud_score = max(float(assessment.get("fraud_score", computed_score)), computed_score)
        fraud_score = min(fraud_score, 1.0)

        # Determine recommendation
        if fraud_score > 0.60:
            recommendation = "DECLINE"
        elif fraud_score >= 0.30:
            recommendation = "FLAG_FOR_REVIEW"
        else:
            recommendation = "PROCEED"

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("analyze_fraud_patterns", ["fraud_signals", "extracted_facts"], ["fraud_score", "anomalies"], ms, ti, to, cost)
        return {**state, "fraud_score": fraud_score, "anomalies": assessment.get("anomalies", [])}

    async def _node_write_output(self, state: FraudState) -> FraudState:
        t = time.time()
        app_id     = state["application_id"]
        fraud_score = float(state.get("fraud_score") or 0.05)
        anomalies   = state.get("anomalies") or []

        if fraud_score > 0.60:
            recommendation = "DECLINE"
            risk_level = "HIGH"
        elif fraud_score >= 0.30:
            recommendation = "FLAG_FOR_REVIEW"
            risk_level = "MEDIUM"
        else:
            recommendation = "PROCEED"
            risk_level = "LOW"

        # FraudScreeningInitiated
        init_event = FraudScreeningInitiated(
            application_id=app_id,
            session_id=self.session_id,
            screening_model_version="fraud-v1.0",
            initiated_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"fraud-{app_id}", [init_event])

        # FraudAnomalyDetected for each MEDIUM+ anomaly
        medium_plus = [a for a in anomalies if a.get("severity", "LOW") in ("MEDIUM", "HIGH")]
        for anomaly in medium_plus:
            atype_str = anomaly.get("type", "revenue_discrepancy")
            try:
                atype = FraudAnomalyType(atype_str)
            except ValueError:
                atype = FraudAnomalyType.REVENUE_DISCREPANCY

            anomaly_event = FraudAnomalyDetected(
                application_id=app_id,
                session_id=self.session_id,
                anomaly=FraudAnomaly(
                    anomaly_type=atype,
                    description=anomaly.get("evidence", "Anomaly detected"),
                    severity=anomaly.get("severity", "MEDIUM"),
                    evidence=anomaly.get("evidence", ""),
                    affected_fields=anomaly.get("affected_fields", []),
                ),
                detected_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"fraud-{app_id}", [anomaly_event])

        # FraudScreeningCompleted
        completed_event = FraudScreeningCompleted(
            application_id=app_id,
            session_id=self.session_id,
            fraud_score=fraud_score,
            risk_level=risk_level,
            anomalies_found=len(medium_plus),
            recommendation=recommendation,
            screening_model_version="fraud-v1.0",
            input_data_hash=self._sha(state.get("extracted_facts") or {}),
            completed_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"fraud-{app_id}", [completed_event])

        # ComplianceCheckRequested on loan stream
        compliance_req = ComplianceCheckRequested(
            application_id=app_id,
            requested_at=datetime.now(),
            triggered_by_event_id=self.session_id,
            regulation_set_version="2026-Q1-v1",
            rules_to_evaluate=["REG-001", "REG-002", "REG-003", "REG-004", "REG-005", "REG-006"],
        ).to_store_dict()
        await self._append_with_retry(f"loan-{app_id}", [compliance_req])

        events_written = [
            {"stream_id": f"fraud-{app_id}", "event_type": "FraudScreeningInitiated"},
            {"stream_id": f"fraud-{app_id}", "event_type": "FraudScreeningCompleted"},
            {"stream_id": f"loan-{app_id}", "event_type": "ComplianceCheckRequested"},
        ]
        await self._record_output_written(events_written, f"Fraud score: {fraud_score:.3f} ({risk_level}). {len(medium_plus)} anomalies. Recommendation: {recommendation}.")
        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("write_output", ["fraud_score", "anomalies"], ["events_written"], ms)
        return {**state, "output_events": events_written, "next_agent": "compliance"}


# ─── COMPLIANCE AGENT ─────────────────────────────────────────────────────────

class ComplianceState(TypedDict):
    application_id: str
    session_id: str
    company_profile: dict | None
    requested_amount_usd: float | None
    rule_results: list[dict]
    has_hard_block: bool
    block_rule_id: str | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


# Regulation definitions — deterministic, no LLM in decision path
REGULATIONS = {
    "REG-001": {
        "name": "Bank Secrecy Act (BSA) Check",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: not any(
            f.get("flag_type") == "AML_WATCH" and f.get("is_active")
            for f in co.get("compliance_flags", [])
        ),
        "failure_reason": "Active AML Watch flag present. Remediation required.",
        "remediation": "Provide enhanced due diligence documentation within 10 business days.",
    },
    "REG-002": {
        "name": "OFAC Sanctions Screening",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: not any(
            f.get("flag_type") == "SANCTIONS_REVIEW" and f.get("is_active")
            for f in co.get("compliance_flags", [])
        ),
        "failure_reason": "Active OFAC Sanctions Review. Application blocked.",
        "remediation": None,
    },
    "REG-003": {
        "name": "Jurisdiction Lending Eligibility",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: co.get("jurisdiction") != "MT",
        "failure_reason": "Jurisdiction MT not approved for commercial lending at this time.",
        "remediation": None,
    },
    "REG-004": {
        "name": "Legal Entity Type Eligibility",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: not (
            co.get("legal_type") == "Sole Proprietor"
            and (co.get("requested_amount_usd", 0) or 0) > 250_000
        ),
        "failure_reason": "Sole Proprietor loans >$250K require additional documentation.",
        "remediation": "Submit SBA Form 912 and personal financial statement.",
    },
    "REG-005": {
        "name": "Minimum Operating History",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: (2024 - (co.get("founded_year") or 2024)) >= 2,
        "failure_reason": "Business must have at least 2 years of operating history.",
        "remediation": None,
    },
    "REG-006": {
        "name": "CRA Community Reinvestment",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: True,
        "note_type": "CRA_CONSIDERATION",
        "note_text": "Jurisdiction qualifies for Community Reinvestment Act consideration.",
    },
}


class ComplianceAgent(BaseApexAgent):
    """
    Evaluates 6 deterministic regulatory rules in sequence.
    Stops at first hard block.
    """

    def build_graph(self):
        async def _reg001(s): return await self._evaluate_rule(s, "REG-001")
        async def _reg002(s): return await self._evaluate_rule(s, "REG-002")
        async def _reg003(s): return await self._evaluate_rule(s, "REG-003")
        async def _reg004(s): return await self._evaluate_rule(s, "REG-004")
        async def _reg005(s): return await self._evaluate_rule(s, "REG-005")
        async def _reg006(s): return await self._evaluate_rule(s, "REG-006")
        g = StateGraph(ComplianceState)
        g.add_node("validate_inputs",      self._node_validate_inputs)
        g.add_node("load_company_profile", self._node_load_profile)
        g.add_node("evaluate_reg001",      _reg001)
        g.add_node("evaluate_reg002",      _reg002)
        g.add_node("evaluate_reg003",      _reg003)
        g.add_node("evaluate_reg004",      _reg004)
        g.add_node("evaluate_reg005",      _reg005)
        g.add_node("evaluate_reg006",      _reg006)
        g.add_node("write_output",         self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",      "load_company_profile")
        g.add_edge("load_company_profile", "evaluate_reg001")

        for src, nxt in [
            ("evaluate_reg001", "evaluate_reg002"),
            ("evaluate_reg002", "evaluate_reg003"),
            ("evaluate_reg003", "evaluate_reg004"),
            ("evaluate_reg004", "evaluate_reg005"),
            ("evaluate_reg005", "evaluate_reg006"),
            ("evaluate_reg006", "write_output"),
        ]:
            g.add_conditional_edges(
                src,
                lambda s, _nxt=nxt: "write_output" if s["has_hard_block"] else _nxt,
            )
        g.add_edge("write_output", END)
        return g.compile()

    def _initial_state(self, application_id: str) -> ComplianceState:
        return ComplianceState(
            application_id=application_id, session_id=self.session_id,
            company_profile=None, requested_amount_usd=None,
            rule_results=[], has_hard_block=False, block_rule_id=None,
            errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state: ComplianceState) -> ComplianceState:
        t = time.time()
        app_id = state["application_id"]
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        has_trigger = any(e["event_type"] == "ComplianceCheckRequested" for e in loan_events)
        ms = int((time.time() - t) * 1000)
        if not has_trigger:
            await self._record_input_failed(["ComplianceCheckRequested"], ["No ComplianceCheckRequested event found"])
            raise ValueError("ComplianceCheckRequested event not found")
        await self._record_input_validated(["application_id", "ComplianceCheckRequested"], ms)
        await self._record_node_execution("validate_inputs", ["application_id"], ["validated"], ms)
        return state

    async def _node_load_profile(self, state: ComplianceState) -> ComplianceState:
        t = time.time()
        app_id = state["application_id"]

        # Get applicant_id and requested_amount from loan stream
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        submitted = next((e for e in loan_events if e["event_type"] == "ApplicationSubmitted"), None)
        applicant_id = submitted["payload"].get("applicant_id", "COMP-001") if submitted else "COMP-001"
        requested_amount = float(submitted["payload"].get("requested_amount_usd", 0)) if submitted else 0.0

        # Load company profile and compliance flags from registry
        profile_obj = await self.registry.get_company(applicant_id)
        flag_objs   = await self.registry.get_compliance_flags(applicant_id)

        profile = dataclasses.asdict(profile_obj) if profile_obj else {"company_id": applicant_id, "jurisdiction": "CA", "legal_type": "LLC", "founded_year": 2020}
        flags   = [dataclasses.asdict(f) for f in flag_objs]

        # Merge flags into profile for rule checks
        profile["compliance_flags"] = flags
        profile["requested_amount_usd"] = requested_amount

        # Append ComplianceCheckInitiated
        init_event = ComplianceCheckInitiated(
            application_id=app_id,
            session_id=self.session_id,
            regulation_set_version="2026-Q1-v1",
            rules_to_evaluate=list(REGULATIONS.keys()),
            initiated_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"compliance-{app_id}", [init_event])

        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("query_applicant_registry", f"company_id={applicant_id}", f"profile + {len(flags)} flags", ms)
        await self._record_node_execution("load_company_profile", ["applicant_id"], ["company_profile"], ms)
        return {**state, "company_profile": profile, "requested_amount_usd": requested_amount}

    async def _evaluate_rule(self, state: ComplianceState, rule_id: str) -> ComplianceState:
        t = time.time()
        app_id = state["application_id"]
        reg = REGULATIONS[rule_id]
        co  = state["company_profile"] or {}

        passes = reg["check"](co)
        evidence_hash = self._sha(f"{rule_id}-{co.get('company_id', 'unknown')}-{passes}")

        rule_results = list(state.get("rule_results") or [])

        if rule_id == "REG-006":
            # Always noted
            note_event = ComplianceRuleNoted(
                application_id=app_id,
                session_id=self.session_id,
                rule_id=rule_id,
                rule_name=reg["name"],
                note_type=reg.get("note_type", "NOTE"),
                note_text=reg.get("note_text", ""),
                evaluated_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"compliance-{app_id}", [note_event])
            rule_results.append({"rule_id": rule_id, "result": "noted"})
        elif passes:
            pass_event = ComplianceRulePassed(
                application_id=app_id,
                session_id=self.session_id,
                rule_id=rule_id,
                rule_name=reg["name"],
                rule_version=reg["version"],
                evidence_hash=evidence_hash,
                evaluation_notes=f"{rule_id} passed",
                evaluated_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"compliance-{app_id}", [pass_event])
            rule_results.append({"rule_id": rule_id, "result": "passed"})
        else:
            fail_event = ComplianceRuleFailed(
                application_id=app_id,
                session_id=self.session_id,
                rule_id=rule_id,
                rule_name=reg["name"],
                rule_version=reg["version"],
                failure_reason=reg.get("failure_reason", "Rule failed"),
                is_hard_block=reg["is_hard_block"],
                remediation_available=bool(reg.get("remediation")),
                remediation_description=reg.get("remediation"),
                evidence_hash=evidence_hash,
                evaluated_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"compliance-{app_id}", [fail_event])
            rule_results.append({"rule_id": rule_id, "result": "failed", "is_hard_block": reg["is_hard_block"]})

        node_name = f"evaluate_{rule_id.lower().replace('-', '_')}"
        ms = int((time.time() - t) * 1000)
        await self._record_node_execution(node_name, ["company_profile"], [f"{rule_id}_result"], ms)

        new_state = {**state, "rule_results": rule_results}
        if not passes and reg["is_hard_block"]:
            new_state["has_hard_block"] = True
            new_state["block_rule_id"] = rule_id
        return new_state

    async def _node_write_output(self, state: ComplianceState) -> ComplianceState:
        t = time.time()
        app_id = state["application_id"]
        rule_results = state.get("rule_results") or []
        has_hard_block = state.get("has_hard_block", False)

        rules_passed = sum(1 for r in rule_results if r.get("result") == "passed")
        rules_failed = sum(1 for r in rule_results if r.get("result") == "failed")
        rules_noted  = sum(1 for r in rule_results if r.get("result") == "noted")

        if has_hard_block:
            verdict = ComplianceVerdict.BLOCKED
        elif rules_failed > 0:
            verdict = ComplianceVerdict.CONDITIONAL
        else:
            verdict = ComplianceVerdict.CLEAR

        completed_event = ComplianceCheckCompleted(
            application_id=app_id,
            session_id=self.session_id,
            rules_evaluated=len(rule_results),
            rules_passed=rules_passed,
            rules_failed=rules_failed,
            rules_noted=rules_noted,
            has_hard_block=has_hard_block,
            overall_verdict=verdict,
            completed_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"compliance-{app_id}", [completed_event])

        # Append to loan stream: DecisionRequested or ApplicationDeclined
        if has_hard_block:
            block_rule = state.get("block_rule_id", "unknown")
            reg = REGULATIONS.get(block_rule, {})
            declined_event = ApplicationDeclined(
                application_id=app_id,
                decline_reasons=[reg.get("failure_reason", f"Hard block on {block_rule}")],
                declined_by=self.session_id,
                adverse_action_notice_required=True,
                adverse_action_codes=[block_rule],
                declined_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"loan-{app_id}", [declined_event])
            loan_event_type = "ApplicationDeclined"
        else:
            decision_req = DecisionRequested(
                application_id=app_id,
                requested_at=datetime.now(),
                all_analyses_complete=True,
                triggered_by_event_id=self.session_id,
            ).to_store_dict()
            await self._append_with_retry(f"loan-{app_id}", [decision_req])
            loan_event_type = "DecisionRequested"

        events_written = [
            {"stream_id": f"compliance-{app_id}", "event_type": "ComplianceCheckCompleted"},
            {"stream_id": f"loan-{app_id}", "event_type": loan_event_type},
        ]
        await self._record_output_written(events_written, f"Compliance: {verdict.value}. {rules_passed} passed, {rules_failed} failed, {rules_noted} noted.")
        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("write_output", ["rule_results"], ["events_written"], ms)
        return {**state, "output_events": events_written, "next_agent": "decision_orchestrator" if not has_hard_block else None}


# ─── DECISION ORCHESTRATOR ────────────────────────────────────────────────────

class OrchestratorState(TypedDict):
    application_id: str
    session_id: str
    credit_result: dict | None
    fraud_result: dict | None
    compliance_result: dict | None
    recommendation: str | None
    confidence: float | None
    approved_amount: float | None
    executive_summary: str | None
    conditions: list[str] | None
    hard_constraints_applied: list[str]
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


class DecisionOrchestratorAgent(BaseApexAgent):
    """
    Synthesises all prior agent outputs into a final recommendation.
    Nodes: validate_inputs → load_credit_result → load_fraud_result
           → load_compliance_result → synthesize_decision → apply_hard_constraints → write_output
    """

    def build_graph(self):
        g = StateGraph(OrchestratorState)
        g.add_node("validate_inputs",        self._node_validate_inputs)
        g.add_node("load_credit_result",     self._node_load_credit)
        g.add_node("load_fraud_result",      self._node_load_fraud)
        g.add_node("load_compliance_result", self._node_load_compliance)
        g.add_node("synthesize_decision",    self._node_synthesize)
        g.add_node("apply_hard_constraints", self._node_constraints)
        g.add_node("write_output",           self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",        "load_credit_result")
        g.add_edge("load_credit_result",     "load_fraud_result")
        g.add_edge("load_fraud_result",      "load_compliance_result")
        g.add_edge("load_compliance_result", "synthesize_decision")
        g.add_edge("synthesize_decision",    "apply_hard_constraints")
        g.add_edge("apply_hard_constraints", "write_output")
        g.add_edge("write_output",           END)
        return g.compile()

    def _initial_state(self, application_id: str) -> OrchestratorState:
        return OrchestratorState(
            application_id=application_id, session_id=self.session_id,
            credit_result=None, fraud_result=None, compliance_result=None,
            recommendation=None, confidence=None, approved_amount=None,
            executive_summary=None, conditions=None, hard_constraints_applied=[],
            errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        app_id = state["application_id"]
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        has_trigger = any(e["event_type"] == "DecisionRequested" for e in loan_events)
        ms = int((time.time() - t) * 1000)
        if not has_trigger:
            await self._record_input_failed(["DecisionRequested"], ["No DecisionRequested event found"])
            raise ValueError("DecisionRequested event not found")
        await self._record_input_validated(["application_id", "DecisionRequested"], ms)
        await self._record_node_execution("validate_inputs", ["application_id"], ["validated"], ms)
        return state

    async def _node_load_credit(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        app_id = state["application_id"]
        credit_events = await self.store.load_stream(f"credit-{app_id}")
        # Get last CreditAnalysisCompleted
        completed = [e for e in credit_events if e["event_type"] == "CreditAnalysisCompleted"]
        credit_result = completed[-1]["payload"] if completed else {}
        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("load_event_store_stream", f"credit-{app_id}", f"CreditAnalysisCompleted: {bool(completed)}", ms)
        await self._record_node_execution("load_credit_result", ["credit_stream"], ["credit_result"], ms)
        return {**state, "credit_result": credit_result}

    async def _node_load_fraud(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        app_id = state["application_id"]
        fraud_events = await self.store.load_stream(f"fraud-{app_id}")
        completed = [e for e in fraud_events if e["event_type"] == "FraudScreeningCompleted"]
        fraud_result = completed[-1]["payload"] if completed else {}
        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("load_event_store_stream", f"fraud-{app_id}", f"FraudScreeningCompleted: {bool(completed)}", ms)
        await self._record_node_execution("load_fraud_result", ["fraud_stream"], ["fraud_result"], ms)
        return {**state, "fraud_result": fraud_result}

    async def _node_load_compliance(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        app_id = state["application_id"]
        compliance_events = await self.store.load_stream(f"compliance-{app_id}")
        completed = [e for e in compliance_events if e["event_type"] == "ComplianceCheckCompleted"]
        compliance_result = completed[-1]["payload"] if completed else {}
        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("load_event_store_stream", f"compliance-{app_id}", f"ComplianceCheckCompleted: {bool(completed)}", ms)
        await self._record_node_execution("load_compliance_result", ["compliance_stream"], ["compliance_result"], ms)
        return {**state, "compliance_result": compliance_result}

    async def _node_synthesize(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        credit     = state.get("credit_result") or {}
        fraud      = state.get("fraud_result") or {}
        compliance = state.get("compliance_result") or {}

        # Extract key values
        decision_data = credit.get("decision") or {}
        risk_tier   = decision_data.get("risk_tier", "MEDIUM")
        confidence  = float(decision_data.get("confidence", 0.5))
        limit_usd   = float(decision_data.get("recommended_limit_usd", 0))
        fraud_score = float(fraud.get("fraud_score", 0.0))
        verdict     = compliance.get("overall_verdict", "CLEAR")

        SYSTEM = """You are a senior loan officer synthesising multi-agent analysis.
Produce a recommendation (APPROVE/DECLINE/REFER), approved_amount_usd, executive_summary (3-5 sentences), and key_risks list.
Return ONLY JSON:
{
  "recommendation": "APPROVE"|"DECLINE"|"REFER",
  "approved_amount_usd": <float or null>,
  "executive_summary": "<3-5 sentences>",
  "key_risks": ["<risk>"],
  "conditions": ["<condition>"]
}"""

        USER = f"""Credit Analysis: risk_tier={risk_tier}, confidence={confidence:.2f}, recommended_limit=${limit_usd:,.0f}
Credit rationale: {decision_data.get('rationale', 'N/A')}
Fraud Screening: fraud_score={fraud_score:.3f}, risk_level={fraud.get('risk_level', 'LOW')}, recommendation={fraud.get('recommendation', 'PROCEED')}
Compliance: verdict={verdict}, has_hard_block={compliance.get('has_hard_block', False)}"""

        ti = to = 0
        cost = 0.0
        try:
            content, ti, to, cost = await self._call_llm(SYSTEM, USER, max_tokens=512)
            synthesis = self._parse_json(content)
        except Exception as exc:
            synthesis = {
                "recommendation": "REFER",
                "approved_amount_usd": limit_usd,
                "executive_summary": f"Automated synthesis failed ({exc!s:.80}). Human review required.",
                "key_risks": ["Synthesis error"],
                "conditions": [],
            }

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("synthesize_decision", ["credit_result", "fraud_result", "compliance_result"], ["recommendation", "executive_summary"], ms, ti, to, cost)
        return {
            **state,
            "recommendation": synthesis.get("recommendation", "REFER"),
            "confidence": confidence,
            "approved_amount": synthesis.get("approved_amount_usd", limit_usd),
            "executive_summary": synthesis.get("executive_summary", ""),
            "conditions": synthesis.get("conditions", []),
        }

    async def _node_constraints(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        recommendation = state.get("recommendation", "REFER")
        confidence     = float(state.get("confidence") or 0.0)
        fraud_result   = state.get("fraud_result") or {}
        credit_result  = state.get("credit_result") or {}
        compliance_result = state.get("compliance_result") or {}
        constraints_applied: list[str] = list(state.get("hard_constraints_applied") or [])

        fraud_score = float(fraud_result.get("fraud_score", 0.0))
        verdict     = compliance_result.get("overall_verdict", "CLEAR")
        decision_data = credit_result.get("decision") or {}
        risk_tier   = decision_data.get("risk_tier", "MEDIUM")

        # Hard constraint 1: compliance BLOCKED → DECLINE
        if verdict == "BLOCKED":
            recommendation = "DECLINE"
            constraints_applied.append("COMPLIANCE_BLOCKED→DECLINE")

        # Hard constraint 2: confidence < 0.60 → REFER
        if confidence < 0.60 and recommendation != "DECLINE":
            recommendation = "REFER"
            constraints_applied.append(f"LOW_CONFIDENCE({confidence:.2f})→REFER")

        # Hard constraint 3: fraud_score > 0.60 → REFER
        if fraud_score > 0.60 and recommendation != "DECLINE":
            recommendation = "REFER"
            constraints_applied.append(f"HIGH_FRAUD_SCORE({fraud_score:.3f})→REFER")

        # Hard constraint 4: HIGH risk + confidence < 0.70 → REFER
        if risk_tier == "HIGH" and confidence < 0.70 and recommendation not in ("DECLINE", "REFER"):
            recommendation = "REFER"
            constraints_applied.append(f"HIGH_RISK+LOW_CONFIDENCE({confidence:.2f})→REFER")

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("apply_hard_constraints", ["recommendation", "confidence", "fraud_score"], ["recommendation"], ms)
        return {**state, "recommendation": recommendation, "hard_constraints_applied": constraints_applied}

    async def _node_write_output(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        app_id         = state["application_id"]
        recommendation = state.get("recommendation", "REFER")
        confidence     = float(state.get("confidence") or 0.5)
        approved_amount = state.get("approved_amount")
        executive_summary = state.get("executive_summary", "")
        conditions     = state.get("conditions") or []
        constraints    = state.get("hard_constraints_applied") or []

        credit_result  = state.get("credit_result") or {}
        fraud_result   = state.get("fraud_result") or {}

        decision_event = DecisionGenerated(
            application_id=app_id,
            orchestrator_session_id=self.session_id,
            recommendation=recommendation,
            confidence=confidence,
            approved_amount_usd=Decimal(str(approved_amount)) if approved_amount else None,
            conditions=conditions,
            executive_summary=executive_summary,
            key_risks=[],
            contributing_sessions=[self.session_id],
            model_versions={"orchestrator": self.model},
            generated_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"loan-{app_id}", [decision_event])

        # Append outcome event
        if recommendation == "APPROVE":
            outcome_event = ApplicationApproved(
                application_id=app_id,
                approved_amount_usd=Decimal(str(approved_amount or 0)),
                interest_rate_pct=5.5,
                term_months=60,
                conditions=conditions,
                approved_by=self.session_id,
                effective_date=datetime.now().strftime("%Y-%m-%d"),
                approved_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"loan-{app_id}", [outcome_event])
            outcome_type = "ApplicationApproved"
        elif recommendation == "DECLINE":
            outcome_event = ApplicationDeclined(
                application_id=app_id,
                decline_reasons=constraints if constraints else ["Application declined based on analysis"],
                declined_by=self.session_id,
                adverse_action_notice_required=True,
                adverse_action_codes=[],
                declined_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"loan-{app_id}", [outcome_event])
            outcome_type = "ApplicationDeclined"
        else:  # REFER
            review_event = HumanReviewRequested(
                application_id=app_id,
                reason="; ".join(constraints) if constraints else "Referred for human review",
                decision_event_id=self.session_id,
                assigned_to=None,
                requested_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"loan-{app_id}", [review_event])
            outcome_type = "HumanReviewRequested"

        events_written = [
            {"stream_id": f"loan-{app_id}", "event_type": "DecisionGenerated"},
            {"stream_id": f"loan-{app_id}", "event_type": outcome_type},
        ]
        await self._record_output_written(events_written, f"Decision: {recommendation} (confidence={confidence:.2f}). Constraints: {constraints}.")
        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("write_output", ["recommendation", "confidence"], ["events_written"], ms)
        return {**state, "output_events": events_written, "next_agent": None}
