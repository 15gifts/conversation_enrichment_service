import json

from batch_enrichment.models import EnrichmentResult, FieldConfig
from batch_enrichment.response_parser import parse_batch_line, parse_batch_lines

BATCH_TRACKING_ID = "batch-test-001"


def make_field_configs() -> list[FieldConfig]:
    """Standard field config set used across most tests — matches VALID_LLM_RESPONSE."""
    return [
        FieldConfig(
            field_name="sentiment",
            field_type="string_enum",
            field_description="Sentiment.",
            display_order=1,
            allowed_values=["positive", "neutral", "negative"],
        ),
        FieldConfig(
            field_name="summary",
            field_type="string",
            field_description="Summary.",
            display_order=2,
        ),
        FieldConfig(
            field_name="score",
            field_type="integer_range",
            field_description="Score 1-5.",
            display_order=3,
            min_value=1,
            max_value=5,
        ),
        FieldConfig(
            field_name="resolved",
            field_type="boolean",
            field_description="Was it resolved?",
            display_order=4,
        ),
        FieldConfig(
            field_name="tags",
            field_type="enum_array",
            field_description="Tags.",
            display_order=5,
            allowed_values=["billing", "support", "sales"],
        ),
        FieldConfig(
            field_name="note",
            field_type="string",
            field_description="Optional note.",
            display_order=6,
            is_nullable=True,
        ),
    ]


VALID_LLM_RESPONSE = {
    "sentiment": "positive",
    "summary": "User resolved a billing issue quickly.",
    "score": 2,
    "resolved": True,
    "tags": ["billing", "support"],
    "note": None,
}


def make_azure_output_line(
    conversation_id: str,
    content: dict | str,
    is_error: bool = False,
    error_code: str = "content_filter",
    error_message: str = "Content filtered",
    status_code: int = 200,
    finish_reason: str = "stop",
) -> str:
    if is_error:
        body = {"error": {"code": error_code, "message": error_message}}
    else:
        body = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": json.dumps(content) if isinstance(content, dict) else content,
                    },
                    "finish_reason": finish_reason,
                }
            ],
            "usage": {"prompt_tokens": 500, "completion_tokens": 80, "total_tokens": 580},
            "model": "gpt-4-1",
        }
    return json.dumps(
        {
            "id": f"batch-{conversation_id}",
            "custom_id": conversation_id,
            "response": {
                "status_code": status_code,
                "body": body,
            },
        }
    )


class TestParseBatchLine:
    def test_valid_azure_output_parses_all_fields(self) -> None:
        # T4: valid Azure output JSONL line → EnrichmentResult with parse_error=False
        line = make_azure_output_line("conv-001", VALID_LLM_RESPONSE)
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())

        assert isinstance(result, EnrichmentResult)
        assert result.conversation_id == "conv-001"
        assert result.parsed_fields["sentiment"] == "positive"
        assert result.parsed_fields["score"] == 2
        assert result.parsed_fields["resolved"] is True
        assert result.parsed_fields["tags"] == ["billing", "support"]
        assert result.parsed_fields["note"] is None
        assert result.parse_error is False
        assert result.batch_tracking_id == BATCH_TRACKING_ID

    def test_valid_result_stores_raw_response(self) -> None:
        line = make_azure_output_line("conv-001", VALID_LLM_RESPONSE)
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())
        assert result.raw_response is not None

    def test_malformed_json_in_llm_content_returns_parse_error(self) -> None:
        # T11: Azure line with non-JSON body → parse_error=True, raw_response stored, no exception
        line = make_azure_output_line("conv-002", "this is not JSON {broken}")
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())

        assert result.parse_error is True
        assert result.conversation_id == "conv-002"
        assert result.parse_error_message is not None
        assert result.raw_response is not None

    def test_missing_required_field_returns_parse_error(self) -> None:
        # T12: Azure line missing a required field → parse_error=True
        incomplete = {k: v for k, v in VALID_LLM_RESPONSE.items() if k != "sentiment"}
        line = make_azure_output_line("conv-003", incomplete)
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())

        assert result.parse_error is True
        assert result.conversation_id == "conv-003"
        assert "sentiment" in (result.parse_error_message or "")

    def test_parse_error_never_raises(self) -> None:
        # Completely broken outer JSONL should not propagate an exception
        result = parse_batch_line("NOT_JSON_AT_ALL", BATCH_TRACKING_ID, make_field_configs())
        assert result.parse_error is True

    def test_azure_error_response_returns_parse_error(self) -> None:
        line = make_azure_output_line("conv-004", {}, is_error=True)
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())
        assert result.parse_error is True
        assert result.conversation_id == "conv-004"

    def test_content_filter_error_classified_as_guardrail(self) -> None:
        # Azure content_filter rejection → failure_reason='guardrail' so it's
        # never retried and analytics can count guardrail rates separately.
        line = make_azure_output_line(
            "conv-guard-1",
            {},
            is_error=True,
            error_code="content_filter",
            error_message="The response was filtered due to content management policy.",
        )
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())
        assert result.parse_error is True
        assert result.failure_reason == "guardrail"
        assert result.conversation_id == "conv-guard-1"

    def test_jailbreak_error_classified_as_guardrail(self) -> None:
        line = make_azure_output_line("conv-guard-2", {}, is_error=True, error_code="jailbreak")
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())
        assert result.failure_reason == "guardrail"

    def test_responsible_ai_policy_violation_classified_as_guardrail(self) -> None:
        line = make_azure_output_line(
            "conv-guard-3",
            {},
            is_error=True,
            error_code="responsible_ai_policy_violation",
        )
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())
        assert result.failure_reason == "guardrail"

    def test_non_guardrail_azure_error_classified_as_azure_error(self) -> None:
        # A non-policy Azure error (e.g. rate limit) is classified separately.
        line = make_azure_output_line(
            "conv-az-1",
            {},
            is_error=True,
            error_code="rate_limit_exceeded",
            error_message="Too many requests",
        )
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())
        assert result.parse_error is True
        assert result.failure_reason == "azure_error"

    def test_http_non_200_with_guardrail_code_classified_as_guardrail(self) -> None:
        # Line-level HTTP failure: response.status_code != 200, body has the
        # guardrail error code. Must be classified as 'guardrail', not retried.
        line = make_azure_output_line(
            "conv-http-guard",
            {},
            is_error=True,
            error_code="content_filter",
            status_code=400,
        )
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())
        assert result.parse_error is True
        assert result.failure_reason == "guardrail"

    def test_http_non_200_without_guardrail_code_classified_as_azure_error(self) -> None:
        line = make_azure_output_line(
            "conv-http-az",
            {},
            is_error=True,
            error_code="server_error",
            status_code=500,
        )
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())
        assert result.failure_reason == "azure_error"

    def test_finish_reason_content_filter_classified_as_guardrail(self) -> None:
        # Output-side guardrail: the model started responding but Azure's output
        # filter cut it off. The choice exists but finish_reason='content_filter'.
        line = make_azure_output_line(
            "conv-output-guard",
            VALID_LLM_RESPONSE,
            finish_reason="content_filter",
        )
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())
        assert result.parse_error is True
        assert result.failure_reason == "guardrail"

    def test_successful_result_has_no_failure_reason(self) -> None:
        line = make_azure_output_line("conv-ok", VALID_LLM_RESPONSE)
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())
        assert result.parse_error is False
        assert result.failure_reason is None

    def test_malformed_json_classified_as_parse_error(self) -> None:
        # The LLM produced output but it doesn't parse → 'parse_error', not retried.
        line = make_azure_output_line("conv-bad-json", "not valid json")
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())
        assert result.parse_error is True
        assert result.failure_reason == "parse_error"

    def test_nullable_field_accepts_null_value(self) -> None:
        # 'note' is nullable — None value must not trigger parse_error
        response = {**VALID_LLM_RESPONSE, "note": None}
        line = make_azure_output_line("conv-005", response)
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())
        assert result.parse_error is False
        assert result.parsed_fields["note"] is None

    def test_required_field_with_null_returns_parse_error(self) -> None:
        # 'sentiment' is not nullable — null must trigger parse_error
        response = {**VALID_LLM_RESPONSE, "sentiment": None}
        line = make_azure_output_line("conv-006", response)
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())
        assert result.parse_error is True
        assert "sentiment" in (result.parse_error_message or "")

    def test_invalid_enum_value_returns_parse_error(self) -> None:
        response = {**VALID_LLM_RESPONSE, "sentiment": "great"}
        line = make_azure_output_line("conv-007", response)
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())
        assert result.parse_error is True
        assert "sentiment" in (result.parse_error_message or "")

    def test_integer_out_of_range_returns_parse_error(self) -> None:
        response = {**VALID_LLM_RESPONSE, "score": 10}
        line = make_azure_output_line("conv-008", response)
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())
        assert result.parse_error is True
        assert "score" in (result.parse_error_message or "")

    def test_non_boolean_for_boolean_field_returns_parse_error(self) -> None:
        response = {**VALID_LLM_RESPONSE, "resolved": "yes"}
        line = make_azure_output_line("conv-009", response)
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())
        assert result.parse_error is True
        assert "resolved" in (result.parse_error_message or "")

    def test_invalid_enum_array_item_returns_parse_error(self) -> None:
        response = {**VALID_LLM_RESPONSE, "tags": ["billing", "unknown_tag"]}
        line = make_azure_output_line("conv-010", response)
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())
        assert result.parse_error is True
        assert "tags" in (result.parse_error_message or "")

    def test_empty_enum_array_is_valid(self) -> None:
        # An empty array is a valid enum_array response
        response = {**VALID_LLM_RESPONSE, "tags": []}
        line = make_azure_output_line("conv-011", response)
        result = parse_batch_line(line, BATCH_TRACKING_ID, make_field_configs())
        assert result.parse_error is False
        assert result.parsed_fields["tags"] == []


class TestParseBatchLines:
    def test_batch_of_10_responses_returns_10_results(self) -> None:
        # T5: 10 Azure output lines → 10 EnrichmentResult objects
        lines = [make_azure_output_line(f"conv-{i:03d}", VALID_LLM_RESPONSE) for i in range(10)]
        output_jsonl = "\n".join(lines)
        results = parse_batch_lines(output_jsonl, BATCH_TRACKING_ID, make_field_configs())

        assert len(results) == 10
        assert all(isinstance(r, EnrichmentResult) for r in results)
        assert all(r.parse_error is False for r in results)

    def test_batch_skips_blank_lines(self) -> None:
        lines = [make_azure_output_line(f"conv-{i:03d}", VALID_LLM_RESPONSE) for i in range(3)]
        output_jsonl = "\n".join(lines) + "\n\n"
        results = parse_batch_lines(output_jsonl, BATCH_TRACKING_ID, make_field_configs())
        assert len(results) == 3

    def test_mixed_valid_and_error_lines_returns_all_results(self) -> None:
        # T17 coverage: 8 OK + 2 error → 10 results, 2 with parse_error=True
        lines = [make_azure_output_line(f"conv-{i:03d}", VALID_LLM_RESPONSE) for i in range(8)] + [
            make_azure_output_line(f"conv-err-{i}", "broken JSON") for i in range(2)
        ]
        output_jsonl = "\n".join(lines)
        results = parse_batch_lines(output_jsonl, BATCH_TRACKING_ID, make_field_configs())

        assert len(results) == 10
        error_results = [r for r in results if r.parse_error]
        assert len(error_results) == 2
