from __future__ import annotations

import json

from batch_enrichment.models import (
    ContextConfig,
    ConversationTranscript,
    FieldConfig,
    SubmitConfig,
)


def _field_type_hint(fc: FieldConfig) -> str:
    """Return a human-readable type hint for use inside the prompt JSON schema block."""
    nullable_suffix = " or null" if fc.is_nullable else ""

    if fc.field_type == "string_enum":
        options = "|".join(fc.allowed_values or [])
        return f'"<{options}>{nullable_suffix}"'

    if fc.field_type == "boolean":
        return "true | false"

    if fc.field_type == "integer_range":
        lo = fc.min_value if fc.min_value is not None else "?"
        hi = fc.max_value if fc.max_value is not None else "?"
        return f"<integer {lo}–{hi}>"

    if fc.field_type == "string_array":
        return '["<item1>", "<item2>"]'

    if fc.field_type == "enum_array":
        options = "|".join(fc.allowed_values or [])
        return f'["<{options}>", ...]'

    # "string" — free text
    return f'"<text>{nullable_suffix}"'


def build_system_prompt(field_configs: list[FieldConfig]) -> str:
    """Build the system prompt dynamically from a list of FieldConfig objects.

    The JSON schema block and rules section are both derived from the configs
    so no code changes are needed when fields are added, removed, or modified.
    """
    sorted_fields = sorted(field_configs, key=lambda fc: fc.display_order)

    schema_lines = [f'  "{fc.field_name}": {_field_type_hint(fc)}' for fc in sorted_fields]
    schema_block = "{\n" + ",\n".join(schema_lines) + "\n}"

    rules: list[str] = [
        "Return only the JSON object — no markdown, no explanation.",
        "For nullable fields you cannot determine with confidence, use null.",
    ]
    for fc in sorted_fields:
        if fc.field_description:
            rules.append(f"{fc.field_name}: {fc.field_description}")

    rules_text = "\n".join(f"- {r}" for r in rules)

    return (
        "You are a conversation analyst. Analyse the conversation transcript and any "
        "additional context provided, then respond with a JSON object containing "
        "exactly these fields:\n\n"
        f"{schema_block}\n\n"
        f"Rules:\n{rules_text}"
    )


def _build_user_message(
    transcript: ConversationTranscript,
    context_configs: list[ContextConfig],
) -> str:
    """Build the user message, appending context signals when configured."""
    if transcript.messages:
        ordered = sorted(transcript.messages, key=lambda m: m.conversation_message_num)
        transcript_text = "\n".join(
            f"{msg.message_sent_by}: {msg.message_text_combined or ''}" for msg in ordered
        )
    else:
        transcript_text = transcript.transcript_text

    if not context_configs:
        return transcript_text

    sorted_ctx = sorted(context_configs, key=lambda c: c.display_order)
    context_lines = "\n".join(
        f"- {c.display_label}: {transcript.context_fields.get(c.column_name, '')}"
        for c in sorted_ctx
    )
    return f"Conversation transcript:\n\n{transcript_text}\n\nAdditional context:\n{context_lines}"


def build_jsonl_line(
    transcript: ConversationTranscript,
    config: SubmitConfig,
    field_configs: list[FieldConfig],
    context_configs: list[ContextConfig] | None = None,
) -> str:
    record = {
        "custom_id": transcript.conversation_id,
        "method": "POST",
        "url": "/chat/completions",
        "body": {
            "model": config.model_deployment,
            "messages": [
                {"role": "system", "content": build_system_prompt(field_configs)},
                {"role": "user", "content": _build_user_message(transcript, context_configs or [])},
            ],
            "temperature": 0,
            "response_format": {"type": "json_object"},
        },
    }
    return json.dumps(record, ensure_ascii=False)


def build_batch_lines(
    transcripts: list[ConversationTranscript],
    config: SubmitConfig,
    field_configs: list[FieldConfig],
    context_configs: list[ContextConfig] | None = None,
) -> str:
    lines = [
        build_jsonl_line(transcript, config, field_configs, context_configs)
        for transcript in transcripts
    ]
    return "\n".join(lines)
