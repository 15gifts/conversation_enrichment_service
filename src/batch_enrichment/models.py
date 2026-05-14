from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict


class BatchStatus(StrEnum):
    PENDING = "PENDING"
    SUBMITTING = "SUBMITTING"
    SUBMITTED = "SUBMITTED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    # Phase 2 additions — defined here so the DDL status column is forward-compatible
    RETRYING = "RETRYING"
    PERMANENTLY_FAILED = "PERMANENTLY_FAILED"


@dataclass
class FieldConfig:
    """Defines a single LLM output field for a given config_version.

    Loaded from enrichment_field_config at runtime — drives both the system
    prompt JSON schema block and the response validation in the parser.

    field_type values:
        string_enum   — single value from allowed_values
        boolean       — true | false
        integer_range — integer within [min_value, max_value]
        string        — free text
        string_array  — array of strings (no item constraint)
        enum_array    — array of strings where each item must be in allowed_values
    """

    field_name: str
    field_type: str
    field_description: str
    display_order: int
    is_nullable: bool = False
    allowed_values: list[str] | None = None
    min_value: int | None = None
    max_value: int | None = None


@dataclass
class ContextConfig:
    """Defines a context column to include in the LLM user message.

    Loaded from enrichment_context_config at runtime.  The column must exist
    in the enrichment_queue view — the value is fetched alongside the
    transcript and appended to the user message as an additional context signal.
    """

    column_name: str
    display_label: str
    display_order: int
    value_description: str | None = None


@dataclass
class ConversationMessage:
    message_sent_by: str
    message_text_combined: str | None
    conversation_message_num: int


@dataclass
class ConversationTranscript:
    conversation_id: str
    messages: list[ConversationMessage] = field(default_factory=list)
    # Pre-built transcript text from the enrichment_queue view (used when messages=[])
    transcript_text: str = ""
    # Values of context columns fetched alongside the transcript (keyed by column_name)
    context_fields: dict[str, Any] = field(default_factory=dict)


@dataclass
class SubmitConfig:
    chunk_size: int
    max_active_batches: int
    model_deployment: str
    prompt_version: str
    analytics_db: str
    azure_endpoint: str = ""
    azure_api_key: str = ""


@dataclass
class RetrieveConfig:
    azure_endpoint: str
    azure_api_key: str
    prompt_version: str


class EnrichmentResult(BaseModel):
    """Parsed result for a single conversation.

    parsed_fields holds all dynamic LLM output fields as a plain dict —
    the schema is defined by enrichment_field_config, not hardcoded here.
    Validation happens in response_parser against the loaded FieldConfig list.
    """

    model_config = ConfigDict(extra="ignore")

    conversation_id: str
    prompt_version: str
    batch_tracking_id: str

    # All dynamic LLM output fields — empty dict when parse_error=True
    parsed_fields: dict[str, Any] = field(default_factory=dict)

    raw_response: dict[str, Any] | None = None
    parse_error: bool = False
    parse_error_message: str | None = None

    # Classification of the failure, so analytics can distinguish guardrail
    # rejections (do not retry, model refused) from transient Azure errors
    # and from downstream JSON / validation failures. None when parse_error=False.
    #   "guardrail"   — Azure content filter / jailbreak / responsible AI policy
    #   "azure_error" — other Azure-side error (rate limit, server error, etc.)
    #   "parse_error" — LLM output present but unparseable or invalid against schema
    failure_reason: str | None = None
