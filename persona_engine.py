"""
persona_engine.py — Phase 11-F (Final, Static-Load)

Phase 11-F production Persona Engine for Sara AI Core.

Behavior:
- Loads the six governance JSONs from the repository root *once at startup* and caches them in memory.
- Validates that each JSON declares `upgrade_metadata.phase == "11-F"` (warning + metric on mismatch).
- Exposes a deterministic `get_system_prompt()` that returns a fully-merged prompt dict and a textual system prompt suitable for passing to GPT as a system message.
- Emits structured logs for load success/failure, off-scope detections, and persona realignment events.
- Provides a manual `reload_persona()` for controlled refresh during maintenance (NOT used automatically in production).

Design notes:
- Static-loading approach chosen for production stability (no mid-call drift).
- Uses lazy imports for logging and metrics to avoid circular import issues at module import time.
- Fails gracefully: missing/invalid files produce warnings and safe defaults rather than raising.
"""
from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Dict, Any, Optional

# Governance filenames (located in repository root)
GOV_FILES = {
    "callflow": "Sara_CallFlow.json",
    "knowledge": "Sara_KnowledgeBase.json",
    "objections": "Sara_Objections.json",
    "opening": "Sara_Opening.json",
    "playbook": "Sara_Playbook.json",
    "systemprompt": "Sara_SystemPrompt_Production.json",
}

REQUIRED_PHASE = "11-F"
PERSONA_IDENTITY = "Sara Hayes"
ROOT_DIR = Path(__file__).resolve().parent


class PersonaEngineError(Exception):
    pass


class PersonaEngine:
    """Singleton persona engine with static startup load.

    Usage:
        PersonaEngine.initialize()  # load once at startup
        pe = PersonaEngine.get_instance()
        prompt_dict = pe.get_system_prompt()
        prompt_text = pe.get_system_prompt_text()
    """

    _instance: Optional["PersonaEngine"] = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        # Private constructor; use initialize()
        self._loaded = False
        self._raw: Dict[str, Dict[str, Any]] = {}
        self._phase_ok: Dict[str, bool] = {}

    # ----------------
    # Lifecycle
    # ----------------
    @classmethod
    def initialize(cls) -> "PersonaEngine":
        """Initialize the singleton and load all governance JSONs statically.

        Safe to call multiple times; will only load once unless reload_persona() is used.
        """
        with cls._lock:
            if cls._instance is None:
                cls._instance = PersonaEngine()
                cls._instance._do_load_all()
            return cls._instance

    @classmethod
    def get_instance(cls) -> "PersonaEngine":
        if cls._instance is None:
            raise PersonaEngineError("PersonaEngine not initialized. Call PersonaEngine.initialize() first.")
        return cls._instance

    # ----------------
    # Internal helpers (lazy imports for logging/metrics)
    # ----------------
    def _get_logger(self):
        try:
            import importlib
n            lu = importlib.import_module("logging_utils")
            return lu.get_json_logger(__name__)
        except Exception:
            # fallback simple logger
            class _Fallback:
                def info(self, *a, **k):
                    print("INFO:", *a)
                def warning(self, *a, **k):
                    print("WARN:", *a)
                def error(self, *a, **k):
                    print("ERROR:", *a)
                def debug(self, *a, **k):
                    print("DEBUG:", *a)
            return _Fallback()

    def _get_metrics_module(self):
        try:
            import importlib
            return importlib.import_module("metrics_collector")
        except Exception:
            return None

    # ----------------
    # Loading & Validation
    # ----------------
    def _do_load_all(self) -> None:
        """Load all governance JSON files from repository root ONCE.

        Records phase validity and logs structured events. Does not raise on missing/invalid files.
        """
        log = self._get_logger()
        metrics = self._get_metrics_module()

        self._raw = {}
        self._phase_ok = {}

        for key, fname in GOV_FILES.items():
            path = ROOT_DIR / fname
            if not path.exists():
                log.warning({
                    "event": "governance_missing",
                    "file": fname,
                    "source": "persona_engine",
                })
                self._raw[key] = {}
                self._phase_ok[key] = False
                continue

            try:
                with open(path, "r", encoding="utf-8") as f:
                    payload = json.load(f)
            except Exception as e:
                log.error({
                    "event": "governance_load_error",
                    "file": fname,
                    "error": str(e),
                    "source": "persona_engine",
                })
                self._raw[key] = {}
                self._phase_ok[key] = False
                # best-effort metrics snapshot
                try:
                    if metrics and hasattr(metrics, "push_snapshot_from_collector"):
                        metrics.push_snapshot_from_collector()
                except Exception:
                    log.debug({"event": "metrics_snapshot_failed", "file": fname})
                continue

            # Phase validation
            detected = payload.get("upgrade_metadata", {}).get("phase")
            ok = detected == REQUIRED_PHASE
            if not ok:
                log.warning({
                    "event": "governance_phase_mismatch",
                    "file": fname,
                    "detected_phase": detected,
                    "required_phase": REQUIRED_PHASE,
                    "source": "persona_engine",
                })
            else:
                log.info({
                    "event": "governance_loaded",
                    "file": fname,
                    "phase": detected,
                    "source": "persona_engine",
                })

            self._raw[key] = payload
            self._phase_ok[key] = ok

        # mark loaded
        self._loaded = True

        # emit final load metric
        try:
            if metrics and hasattr(metrics, "increment"):
                metrics.increment("persona_loaded_total")
        except Exception:
            log.debug({"event": "persona_metric_emit_failed"})

    def reload_persona(self) -> None:
        """Manually reload persona files. Use only for maintenance and testing.

        This will replace the cached governance payloads atomically.
        """
        with self._lock:
            self._do_load_all()

    def list_loaded(self) -> Dict[str, bool]:
        return dict(self._phase_ok)

    def validate_governance(self) -> bool:
        """Return True if all governance files exist and are marked as the required phase."""
        if not self._loaded:
            raise PersonaEngineError("PersonaEngine not initialized")
        return all(self._phase_ok.get(k, False) for k in GOV_FILES.keys())

    # ----------------
    # Prompt assembly
    # ----------------
    def get_system_prompt(self) -> Dict[str, Any]:
        """Return a merged prompt dict suitable for programmatic use.

        The dict contains: persona_identity, callflow, knowledge, objections, opening, playbook, systemprompt
        """
        if not self._loaded:
            raise PersonaEngineError("PersonaEngine not initialized")

        # Start with safe defaults
        merged: Dict[str, Any] = {
            "persona_identity": PERSONA_IDENTITY,
            "callflow": {},
            "knowledge": {},
            "objections": {},
            "opening": {},
            "playbook": {},
            "systemprompt": {},
        }

        # Merge raw payload content conservatively
        merged["callflow"] = self._raw.get("callflow", {}).get("callflow") or self._raw.get("callflow", {})
        merged["knowledge"] = self._raw.get("knowledge", {}).get("knowledge_base") or self._raw.get("knowledge", {})
        merged["objections"] = self._raw.get("objections", {}).get("objection_patterns") or self._raw.get("objections", {})
        merged["opening"] = self._raw.get("opening", {}).get("decision_maker_openers") or self._raw.get("opening", {})
        merged["playbook"] = self._raw.get("playbook", {}).get("playbook") or self._raw.get("playbook", {})
        merged["systemprompt"] = self._raw.get("systemprompt", {})

        return merged

    def get_system_prompt_text(self) -> str:
        """Return a textual system prompt built from the governance payloads suitable for sending as a system message to GPT.

        This function is intentionally deterministic and stable: it does not read files from disk at call time.
        """
        pd = self.get_system_prompt()

        # Build a concise human-readable system prompt that enforces identity, guardrails, and output format
        lines = [f"You are {pd.get('persona_identity', PERSONA_IDENTITY)}."]

        sp = pd.get("systemprompt", {})
        sara_identity = sp.get("sara_identity") or sp.get("persona_lock") or None
        if sara_identity:
            lines.append(f"Identity directive: {sara_identity}")

        # Add high-level guardrails
        guardrails = []
        # try system-level guardrails from callflow or systemprompt
        cf = pd.get("callflow", {})
        if isinstance(cf, dict):
            g = cf.get("system_guardrails") or cf.get("cognitive_restriction_protocols")
            if g:
                guardrails.append(g)
        if isinstance(sp, dict):
            g2 = sp.get("cognitive_restriction_protocols") or sp.get("system_guardrails")
            if g2:
                guardrails.append(g2)

        if guardrails:
            lines.append("Guardrails:")
            for g in guardrails:
                # if guardrail is a dict/list, attempt to make readable
                if isinstance(g, (list, dict)):
                    try:
                        lines.append(json.dumps(g, ensure_ascii=False))
                    except Exception:
                        lines.append(str(g))
                else:
                    lines.append(str(g))

        # Add short cheat-sheet from playbook
        pb = pd.get("playbook", {})
        if isinstance(pb, dict):
            tone = pb.get("tone_rules") or pb.get("persona_identity")
            if tone:
                lines.append("Tone rules:")
                if isinstance(tone, (list, dict)):
                    lines.append(json.dumps(tone, ensure_ascii=False))
                else:
                    lines.append(str(tone))

        # Enforce output format if provided
        jfmt = sp.get("json_output_format") if isinstance(sp, dict) else None
        if jfmt:
            lines.append("JSON Output Format:")
            try:
                lines.append(json.dumps(jfmt, ensure_ascii=False))
            except Exception:
                lines.append(str(jfmt))

        # Always include a final persona lock reminder
        lines.append("Always maintain the Sara Hayes persona and do not reveal you are an AI.")

        return "\n\n".join(lines)

    # ----------------
    # Off-scope detection and handling
    # ----------------
    def is_off_scope(self, user_utterance: str) -> bool:
        """Conservative off-scope detector using systemprompt deflection keywords if present."""
        if not self._loaded:
            return False
        sp = self._raw.get("systemprompt", {})
        def_patterns = sp.get("deflection_patterns") or sp.get("deflection_keywords") or []
        if not def_patterns:
            # fallback to playbook fail_safes
            pb = self._raw.get("playbook", {})
            fail = pb.get("fail_safes") or pb.get("persona_identity", {}).get("fail_safes") if isinstance(pb, dict) else None
            def_patterns = fail or []

        ut = user_utterance.lower()
        for d in def_patterns:
            kw = d
            if isinstance(d, dict):
                kw = d.get("keyword") or d.get("pattern")
            if not kw:
                continue
            if isinstance(kw, str) and kw.lower() in ut:
                return True
        return False

    def handle_off_scope(self, user_utterance: str) -> Dict[str, Any]:
        """Emit logs & metrics and return the deflection response (best-effort)."""
        log = self._get_logger()
        metrics = self._get_metrics_module()

        log.warning({
            "event": "off_scope_violation",
            "source": "Sara_JSON_Layer",
            "level": "warning",
            "utterance": user_utterance,
            "persona": PERSONA_IDENTITY,
        })

        # metrics
        try:
            if metrics and hasattr(metrics, "increment"):
                metrics.increment("cognitive_scope_violations_total")
            if metrics and hasattr(metrics, "push_snapshot_from_collector"):
                metrics.push_snapshot_from_collector()
        except Exception:
            log.debug({"event": "metrics_emit_failed"})

        # deflection response
        sp = self._raw.get("systemprompt", {})
        deflections = sp.get("deflection_responses") or {}
        default_resp = None
        if isinstance(deflections, dict):
            default_resp = deflections.get("default")
        if default_resp:
            return {"type": "deflection", "response": default_resp}
        return {"type": "deflection", "response": "I can’t help with that topic — let’s focus on business growth and how Noblecom helps."}

    # ----------------
    # Persona realignment event
    # ----------------
    def emit_persona_realign(self, reason: str) -> None:
        log = self._get_logger()
        metrics = self._get_metrics_module()
        log.info({
            "event": "persona_realign",
            "source": "persona_engine",
            "persona": PERSONA_IDENTITY,
            "reason": reason,
        })
        try:
            if metrics and hasattr(metrics, "increment"):
                metrics.increment("persona_realign_events_total")
            if metrics and hasattr(metrics, "push_snapshot_from_collector"):
                metrics.push_snapshot_from_collector()
        except Exception:
            log.debug({"event": "persona_realign_metric_failed"})


# Module-level helpers
def initialize_persona_engine() -> PersonaEngine:
    return PersonaEngine.initialize()


# Initialize on import time (static-load)
try:
    _ENGINE = PersonaEngine.initialize()
except Exception:
    _ENGINE = None


__all__ = [
    "PersonaEngine",
    "initialize_persona_engine",
]
