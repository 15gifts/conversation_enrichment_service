import json

from batch_enrichment.jsonl_builder import (
    build_batch_lines,
    build_jsonl_line,
    build_system_prompt,
)
from batch_enrichment.models import (
    ContextConfig,
    ConversationMessage,
    ConversationTranscript,
    FieldConfig,
    SubmitConfig,
)


def make_config() -> SubmitConfig:
    return SubmitConfig(
        chunk_size=10,
        max_active_batches=1,
        model_deployment="gpt-4-1-batch",
        prompt_version="v1.0",
        analytics_db="prod_analytics",
    )


def make_transcript(
    conversation_id: str,
    messages: list[tuple[str, str | None, int]],
    context_fields: dict | None = None,
) -> ConversationTranscript:
    return ConversationTranscript(
        conversation_id=conversation_id,
        messages=[
            ConversationMessage(
                message_sent_by=sent_by,
                message_text_combined=text,
                conversation_message_num=num,
            )
            for sent_by, text, num in messages
        ],
        context_fields=context_fields or {},
    )


def make_field_configs() -> list[FieldConfig]:
    """Minimal field config set used across tests."""
    return [
        FieldConfig(
            field_name="sentiment",
            field_type="string_enum",
            field_description="Overall sentiment of the conversation.",
            display_order=1,
            allowed_values=["positive", "neutral", "negative"],
        ),
        FieldConfig(
            field_name="summary",
            field_type="string",
            field_description="2-3 sentence summary.",
            display_order=2,
        ),
    ]


class TestBuildSystemPrompt:
    def test_system_prompt_includes_all_field_names(self) -> None:
        # Prompt JSON schema block must include every configured field name
        configs = make_field_configs()
        prompt = build_system_prompt(configs)
        assert '"sentiment"' in prompt
        assert '"summary"' in prompt

    def test_string_enum_shows_pipe_separated_options(self) -> None:
        configs = [
            FieldConfig(
                field_name="state",
                field_type="string_enum",
                field_description="State.",
                display_order=1,
                allowed_values=["open", "closed", "pending"],
            )
        ]
        prompt = build_system_prompt(configs)
        assert "open|closed|pending" in prompt

    def test_boolean_field_shows_true_false(self) -> None:
        configs = [
            FieldConfig(
                field_name="resolved",
                field_type="boolean",
                field_description="Was it resolved?",
                display_order=1,
            )
        ]
        prompt = build_system_prompt(configs)
        assert "true | false" in prompt

    def test_integer_range_shows_bounds(self) -> None:
        configs = [
            FieldConfig(
                field_name="score",
                field_type="integer_range",
                field_description="Score.",
                display_order=1,
                min_value=1,
                max_value=5,
            )
        ]
        prompt = build_system_prompt(configs)
        assert "1" in prompt and "5" in prompt

    def test_enum_array_shows_allowed_items(self) -> None:
        configs = [
            FieldConfig(
                field_name="friction_types",
                field_type="enum_array",
                field_description="Friction observed.",
                display_order=1,
                allowed_values=["price", "confidence", "trust"],
            )
        ]
        prompt = build_system_prompt(configs)
        assert "price|confidence|trust" in prompt

    def test_nullable_field_shows_or_null(self) -> None:
        configs = [
            FieldConfig(
                field_name="note",
                field_type="string",
                field_description="Optional note.",
                display_order=1,
                is_nullable=True,
            )
        ]
        prompt = build_system_prompt(configs)
        assert "null" in prompt

    def test_field_descriptions_appear_in_rules_section(self) -> None:
        configs = make_field_configs()
        prompt = build_system_prompt(configs)
        assert "Overall sentiment" in prompt
        assert "2-3 sentence summary" in prompt

    def test_fields_ordered_by_display_order(self) -> None:
        configs = [
            FieldConfig(
                field_name="z_field", field_type="string", field_description="z", display_order=2
            ),
            FieldConfig(
                field_name="a_field", field_type="string", field_description="a", display_order=1
            ),
        ]
        prompt = build_system_prompt(configs)
        assert prompt.index('"a_field"') < prompt.index('"z_field"')


class TestBuildJsonlLine:
    def test_single_turn_conversation_produces_valid_line(self) -> None:
        # T1: 1 conversation, 2 messages → 1 valid JSONL line with custom_id = conversation_id
        transcript = make_transcript(
            "conv-001",
            [("user", "What is my bill?", 1), ("assistant", "Your bill is £30.", 2)],
        )
        line = build_jsonl_line(transcript, make_config(), make_field_configs())
        data = json.loads(line)

        assert data["custom_id"] == "conv-001"
        assert data["method"] == "POST"
        assert data["url"] == "/chat/completions"
        body = data["body"]
        assert body["model"] == "gpt-4-1-batch"
        assert body["temperature"] == 0
        assert body["response_format"] == {"type": "json_object"}
        messages = body["messages"]
        assert messages[0]["role"] == "system"
        assert messages[1]["role"] == "user"
        assert "user: What is my bill?" in messages[1]["content"]
        assert "assistant: Your bill is £30." in messages[1]["content"]

    def test_multi_turn_conversation_preserves_message_order(self) -> None:
        # T2: 6 messages in mixed order → ordered by conversation_message_num ascending
        transcript = make_transcript(
            "conv-002",
            [
                ("assistant", "How can I help?", 2),
                ("user", "I need support.", 1),
                ("user", "About my contract.", 3),
                ("assistant", "I can help with that.", 4),
                ("user", "Great, thanks.", 5),
                ("assistant", "You are welcome.", 6),
            ],
        )
        line = build_jsonl_line(transcript, make_config(), make_field_configs())
        data = json.loads(line)
        content = data["body"]["messages"][1]["content"]

        first_pos = content.index("user: I need support.")
        second_pos = content.index("assistant: How can I help?")
        assert first_pos < second_pos, "Messages must be ordered by conversation_message_num"

    def test_null_message_text_uses_empty_string(self) -> None:
        # T18: message with null message_text_combined → empty string used, no exception
        transcript = make_transcript(
            "conv-003",
            [("user", None, 1), ("assistant", "Hello!", 2)],
        )
        line = build_jsonl_line(transcript, make_config(), make_field_configs())
        data = json.loads(line)
        content = data["body"]["messages"][1]["content"]
        assert "user: " in content

    def test_single_message_conversation_produces_valid_line(self) -> None:
        # T19: 1 user message only (no assistant turn) → valid JSONL line
        transcript = make_transcript("conv-004", [("user", "Just one message.", 1)])
        line = build_jsonl_line(transcript, make_config(), make_field_configs())
        data = json.loads(line)
        assert data["custom_id"] == "conv-004"
        content = data["body"]["messages"][1]["content"]
        assert "user: Just one message." in content

    def test_transcript_text_used_when_messages_empty(self) -> None:
        # When transcript has no messages, transcript_text is used as the conversation
        transcript = ConversationTranscript(
            conversation_id="conv-005",
            transcript_text="user: Hi\nassistant: Hello",
        )
        line = build_jsonl_line(transcript, make_config(), make_field_configs())
        data = json.loads(line)
        content = data["body"]["messages"][1]["content"]
        assert "user: Hi" in content

    def test_context_fields_appended_to_user_message(self) -> None:
        # Context columns appear after the transcript with their display_label
        transcript = make_transcript(
            "conv-006",
            [("user", "Interested in buying.", 1)],
            context_fields={"sale": 1, "exit_to_purchase": 0},
        )
        context_configs = [
            ContextConfig(column_name="sale", display_label="sale", display_order=1),
            ContextConfig(
                column_name="exit_to_purchase",
                display_label="exit_to_purchase",
                display_order=2,
            ),
        ]
        line = build_jsonl_line(transcript, make_config(), make_field_configs(), context_configs)
        data = json.loads(line)
        content = data["body"]["messages"][1]["content"]
        assert "sale: 1" in content
        assert "exit_to_purchase: 0" in content
        assert "Additional context" in content

    def test_context_fields_ordered_by_display_order(self) -> None:
        transcript = make_transcript(
            "conv-007",
            [("user", "Hello", 1)],
            context_fields={"z_col": "z", "a_col": "a"},
        )
        context_configs = [
            ContextConfig(column_name="z_col", display_label="z_col", display_order=2),
            ContextConfig(column_name="a_col", display_label="a_col", display_order=1),
        ]
        line = build_jsonl_line(transcript, make_config(), make_field_configs(), context_configs)
        data = json.loads(line)
        content = data["body"]["messages"][1]["content"]
        assert content.index("a_col") < content.index("z_col")

    def test_no_context_configs_omits_additional_context_section(self) -> None:
        transcript = make_transcript("conv-008", [("user", "Hello", 1)])
        line = build_jsonl_line(transcript, make_config(), make_field_configs(), [])
        data = json.loads(line)
        content = data["body"]["messages"][1]["content"]
        assert "Additional context" not in content

    def test_system_prompt_contains_configured_fields(self) -> None:
        transcript = make_transcript("conv-009", [("user", "Hi", 1)])
        line = build_jsonl_line(transcript, make_config(), make_field_configs())
        data = json.loads(line)
        system_content = data["body"]["messages"][0]["content"]
        assert '"sentiment"' in system_content
        assert '"summary"' in system_content


class TestBuildBatchLines:
    def test_batch_of_10_produces_10_lines(self) -> None:
        # T3: 10 conversations → 10 JSONL lines, each unique custom_id
        transcripts = [
            make_transcript(f"conv-{i:03d}", [("user", f"Message {i}", 1)]) for i in range(10)
        ]
        output = build_batch_lines(transcripts, make_config(), make_field_configs())
        lines = [line for line in output.splitlines() if line.strip()]
        assert len(lines) == 10

        custom_ids = [json.loads(line)["custom_id"] for line in lines]
        assert len(set(custom_ids)) == 10, "All custom_ids must be unique"

    def test_each_line_in_batch_is_valid_json(self) -> None:
        transcripts = [
            make_transcript(f"conv-{i:03d}", [("user", f"Msg {i}", 1)]) for i in range(3)
        ]
        output = build_batch_lines(transcripts, make_config(), make_field_configs())
        for line in output.splitlines():
            json.loads(line)  # must not raise
