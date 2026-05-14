from __future__ import annotations

import json
import logging
from typing import Any

from batch_enrichment.models import EnrichmentResult, FieldConfig

logger = logging.getLogger(__name__)


# Azure / OpenAI content-policy error codes that indicate a guardrail block.
# These conversations must NOT be retried — the model has refused, and retrying
# the same input will yield the same refusal. We write them to enrichment_results
# so the enrichment_queue view excludes them on future runs.
_GUARDRAIL_ERROR_CODES = frozenset(
    {
        "content_filter",
        "content_policy_violation",
        "responsible_ai_policy_violation",
        "jailbreak",
        "hate",
        "violence",
        "self_harm",
        "sexual",
    }
)

# finish_reason values that indicate Azure stopped generation because of a
# guardrail. "content_filter" is the canonical one; we accept variants defensively.
_GUARDRAIL_FINISH_REASONS = frozenset({"content_filter", "content_filtered"})


def _is_guardrail_code(code: str | None) -> bool:
    if not code:
        return False
    return code.lower() in _GUARDRAIL_ERROR_CODES


def _extract_content(
    outer: dict[str, Any],
) -> tuple[str, int | None, dict[str, Any] | None]:
    """Return (conversation_id, response_status_code, raw_response_body)."""
    conversation_id: str = outer.get("custom_id", "")
    response = outer.get("response", {})
    status_code = response.get("status_code")
    body: dict[str, Any] | None = response.get("body")
    return conversation_id, status_code, body


def _parse_error_result(
    conversation_id: str,
    batch_tracking_id: str,
    message: str,
    raw_response: dict[str, Any] | None,
    failure_reason: str = "parse_error",
) -> EnrichmentResult:
    return EnrichmentResult(
        conversation_id=conversation_id,
        prompt_version="",
        batch_tracking_id=batch_tracking_id,
        parsed_fields={},
        parse_error=True,
        parse_error_message=message,
        raw_response=raw_response,
        failure_reason=failure_reason,
    )


def _validate_field_value(field_name: str, value: Any, fc: FieldConfig) -> None:
    """Raise ValueError if value does not satisfy the FieldConfig constraints."""
    if value is None:
        if not fc.is_nullable:
            raise ValueError(f"Field '{field_name}' is required but got null")
        return

    if fc.field_type == "string_enum":
        if fc.allowed_values and value not in fc.allowed_values:
            raise ValueError(
                f"Invalid value for '{field_name}': {value!r}. Must be one of {fc.allowed_values}"
            )

    elif fc.field_type == "boolean":
        if not isinstance(value, bool):
            raise ValueError(f"Field '{field_name}' must be boolean, got {type(value).__name__}")

    elif fc.field_type == "integer_range":
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"Field '{field_name}' must be integer, got {type(value).__name__}")
        if fc.min_value is not None and value < fc.min_value:
            raise ValueError(f"Field '{field_name}' value {value} is below minimum {fc.min_value}")
        if fc.max_value is not None and value > fc.max_value:
            raise ValueError(f"Field '{field_name}' value {value} is above maximum {fc.max_value}")

    elif fc.field_type in ("string_array", "enum_array"):
        if not isinstance(value, list):
            raise ValueError(f"Field '{field_name}' must be array, got {type(value).__name__}")
        if fc.field_type == "enum_array" and fc.allowed_values:
            invalid = [v for v in value if v not in fc.allowed_values]
            if invalid:
                raise ValueError(
                    f"Invalid items in '{field_name}': {invalid}. "
                    f"Must be a subset of {fc.allowed_values}"
                )


def parse_batch_line(
    line: str,
    batch_tracking_id: str,
    field_configs: list[FieldConfig],
) -> EnrichmentResult:
    """Parse one Azure Batch API output JSONL line into an EnrichmentResult.

    Never raises — all failure modes return parse_error=True with a message.
    Validates each field against the provided FieldConfig list.
    """
    try:
        outer = json.loads(line)
    except json.JSONDecodeError as exc:
        return _parse_error_result("", batch_tracking_id, f"Outer JSONDecodeError: {exc}", None)

    conversation_id, status_code, body = _extract_content(outer)

    # Line-level HTTP failure (Azure returned non-200 for this individual request).
    # Classify as guardrail if the body's error code matches a content-policy code,
    # otherwise treat as a generic Azure error. Neither case is retried — the
    # conversation lands in enrichment_results and is excluded by the queue view.
    if status_code is not None and status_code != 200:
        error_info = (body or {}).get("error", {}) if isinstance(body, dict) else {}
        code = error_info.get("code")
        reason = "guardrail" if _is_guardrail_code(code) else "azure_error"
        return _parse_error_result(
            conversation_id,
            batch_tracking_id,
            f"Azure HTTP {status_code} ({code or 'unknown'}): "
            f"{error_info.get('message', 'no message')}",
            body if isinstance(body, dict) else None,
            failure_reason=reason,
        )

    if body is None:
        return _parse_error_result(
            conversation_id, batch_tracking_id, "Missing response body", None
        )

    # Body-level error (status_code may still be 200 in some Azure batch responses
    # — the per-line error is reported in the body).
    if "error" in body:
        error_info = body["error"] or {}
        code = error_info.get("code")
        reason = "guardrail" if _is_guardrail_code(code) else "azure_error"
        return _parse_error_result(
            conversation_id,
            batch_tracking_id,
            f"Azure error {code}: {error_info.get('message')}",
            body,
            failure_reason=reason,
        )

    choices = body.get("choices", [])
    if not choices:
        return _parse_error_result(
            conversation_id, batch_tracking_id, "No choices in response body", body
        )

    # Output-side guardrail: Azure produced a choice but truncated generation
    # because the response itself hit a content filter.
    finish_reason = (choices[0].get("finish_reason") or "").lower()
    if finish_reason in _GUARDRAIL_FINISH_REASONS:
        return _parse_error_result(
            conversation_id,
            batch_tracking_id,
            f"Azure output blocked by content filter (finish_reason={finish_reason})",
            body,
            failure_reason="guardrail",
        )

    raw_content = choices[0].get("message", {}).get("content", "")

    try:
        parsed = json.loads(raw_content)
    except json.JSONDecodeError as exc:
        return _parse_error_result(
            conversation_id,
            batch_tracking_id,
            f"LLM content JSONDecodeError: {exc}",
            body,
        )

    # Check all required (non-nullable) fields are present
    required_fields = {fc.field_name for fc in field_configs if not fc.is_nullable}
    missing = required_fields - set(parsed.keys())
    if missing:
        return _parse_error_result(
            conversation_id,
            batch_tracking_id,
            f"Missing required fields: {sorted(missing)}",
            body,
        )

    # Validate each configured field and build parsed_fields dict
    parsed_fields: dict[str, Any] = {}
    try:
        for fc in field_configs:
            value = parsed.get(fc.field_name)
            _validate_field_value(fc.field_name, value, fc)
            parsed_fields[fc.field_name] = value
    except ValueError as exc:
        return _parse_error_result(
            conversation_id,
            batch_tracking_id,
            f"Field validation error: {exc}",
            body,
        )

    return EnrichmentResult(
        conversation_id=conversation_id,
        prompt_version="",
        batch_tracking_id=batch_tracking_id,
        parsed_fields=parsed_fields,
        raw_response=body,
        parse_error=False,
    )


def parse_batch_lines(
    output_jsonl: str,
    batch_tracking_id: str,
    field_configs: list[FieldConfig],
) -> list[EnrichmentResult]:
    """Parse all lines from an Azure Batch API output file."""
    return [
        parse_batch_line(line, batch_tracking_id, field_configs)
        for line in output_jsonl.splitlines()
        if line.strip()
    ]
