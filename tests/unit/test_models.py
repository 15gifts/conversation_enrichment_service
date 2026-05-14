from batch_enrichment.models import (
    BatchStatus,
    ContextConfig,
    ConversationMessage,
    ConversationTranscript,
    EnrichmentResult,
    FieldConfig,
    RetrieveConfig,
    SubmitConfig,
)


class TestBatchStatus:
    def test_phase1_statuses_all_defined(self) -> None:
        expected = {"PENDING", "SUBMITTING", "SUBMITTED", "IN_PROGRESS", "COMPLETED", "FAILED"}
        actual = {s.value for s in BatchStatus}
        assert expected.issubset(actual)


class TestFieldConfig:
    def test_field_config_stores_required_fields(self) -> None:
        fc = FieldConfig(
            field_name="sentiment",
            field_type="string_enum",
            field_description="Overall sentiment.",
            display_order=1,
            allowed_values=["positive", "negative", "neutral"],
        )
        assert fc.field_name == "sentiment"
        assert fc.field_type == "string_enum"
        assert fc.allowed_values == ["positive", "negative", "neutral"]
        assert fc.is_nullable is False

    def test_field_config_nullable_defaults_to_false(self) -> None:
        fc = FieldConfig(
            field_name="f", field_type="string", field_description="d", display_order=1
        )
        assert fc.is_nullable is False

    def test_integer_range_stores_min_max(self) -> None:
        fc = FieldConfig(
            field_name="score",
            field_type="integer_range",
            field_description="Score 1-5.",
            display_order=2,
            min_value=1,
            max_value=5,
        )
        assert fc.min_value == 1
        assert fc.max_value == 5


class TestContextConfig:
    def test_context_config_stores_fields(self) -> None:
        cc = ContextConfig(
            column_name="sale",
            display_label="sale",
            display_order=1,
            value_description="1 if purchased.",
        )
        assert cc.column_name == "sale"
        assert cc.display_label == "sale"
        assert cc.value_description == "1 if purchased."

    def test_context_config_description_optional(self) -> None:
        cc = ContextConfig(column_name="col", display_label="col", display_order=1)
        assert cc.value_description is None


class TestConversationMessage:
    def test_message_stores_required_fields(self) -> None:
        msg = ConversationMessage(
            message_sent_by="user",
            message_text_combined="Hello",
            conversation_message_num=1,
        )
        assert msg.message_sent_by == "user"
        assert msg.message_text_combined == "Hello"
        assert msg.conversation_message_num == 1

    def test_message_accepts_none_text(self) -> None:
        msg = ConversationMessage(
            message_sent_by="assistant",
            message_text_combined=None,
            conversation_message_num=2,
        )
        assert msg.message_text_combined is None


class TestConversationTranscript:
    def test_transcript_stores_conversation_id_and_messages(self) -> None:
        messages = [
            ConversationMessage(
                message_sent_by="user",
                message_text_combined="Hi",
                conversation_message_num=1,
            )
        ]
        transcript = ConversationTranscript(conversation_id="conv-001", messages=messages)
        assert transcript.conversation_id == "conv-001"
        assert len(transcript.messages) == 1

    def test_transcript_stores_context_fields(self) -> None:
        transcript = ConversationTranscript(
            conversation_id="conv-002",
            context_fields={"sale": 1, "exit_to_purchase": 0},
        )
        assert transcript.context_fields["sale"] == 1
        assert transcript.context_fields["exit_to_purchase"] == 0

    def test_transcript_stores_transcript_text(self) -> None:
        transcript = ConversationTranscript(
            conversation_id="conv-003",
            transcript_text="user: Hello\nassistant: Hi",
        )
        assert "Hello" in transcript.transcript_text

    def test_transcript_context_fields_default_empty(self) -> None:
        transcript = ConversationTranscript(conversation_id="conv-004")
        assert transcript.context_fields == {}


class TestSubmitConfig:
    def test_submit_config_stores_all_fields(self) -> None:
        config = SubmitConfig(
            chunk_size=10,
            max_active_batches=1,
            model_deployment="gpt-4-1-batch",
            prompt_version="v1.0",
            analytics_db="prod_analytics",
        )
        assert config.chunk_size == 10
        assert config.max_active_batches == 1
        assert config.model_deployment == "gpt-4-1-batch"
        assert config.prompt_version == "v1.0"
        assert config.analytics_db == "prod_analytics"


class TestRetrieveConfig:
    def test_retrieve_config_stores_fields(self) -> None:
        config = RetrieveConfig(
            azure_endpoint="https://example.openai.azure.com",
            azure_api_key="test-key",
            prompt_version="v1.0",
        )
        assert config.azure_endpoint == "https://example.openai.azure.com"
        assert config.prompt_version == "v1.0"


class TestEnrichmentResult:
    def test_valid_result_stores_parsed_fields(self) -> None:
        result = EnrichmentResult(
            conversation_id="conv-001",
            prompt_version="v1.0",
            batch_tracking_id="batch-abc",
            parsed_fields={"sentiment": "positive", "summary": "Issue resolved."},
            raw_response={"usage": {"total_tokens": 100}},
            parse_error=False,
        )
        assert result.parsed_fields["sentiment"] == "positive"
        assert result.parse_error is False

    def test_parse_error_result_has_empty_parsed_fields(self) -> None:
        result = EnrichmentResult(
            conversation_id="conv-002",
            prompt_version="v1.0",
            batch_tracking_id="batch-abc",
            parse_error=True,
            parse_error_message="JSONDecodeError: invalid JSON",
            raw_response={"raw": "broken text"},
        )
        assert result.parse_error is True
        assert result.parsed_fields == {}
        assert result.parse_error_message == "JSONDecodeError: invalid JSON"

    def test_parsed_fields_defaults_to_empty_dict(self) -> None:
        result = EnrichmentResult(
            conversation_id="conv-003",
            prompt_version="v1.0",
            batch_tracking_id="batch-abc",
        )
        assert result.parsed_fields == {}
