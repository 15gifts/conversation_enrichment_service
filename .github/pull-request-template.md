## Summary

<!-- Briefly describe what this PR does and why -->

## Plan Reference

<!-- Link to the plan document that governs this work -->
Plan document: `halo-artefacts/{{namespace}}/{{ticket-id}}/plan.md`

---

## Pre-merge Checklist

The author and reviewer must verify every item before this PR can be merged.

### Planning Gate
- [ ] A plan document exists in `halo-artefacts/{{namespace}}/{{ticket-id}}/plan.md` for this work
- [ ] The plan was completed using `/halo-plan`
- [ ] A second opinion review exists in `halo-artefacts/{{namespace}}/{{ticket-id}}/plan_second-opinion.md`
- [ ] The implementation matches the agreed plan (no scope creep, no unplanned changes)

### Code Quality
- [ ] `/halo-review-code` has been run and all BLOCKING issues are resolved
- [ ] `/halo-check-standards` has been run and all failures are resolved
- [ ] No new linter warnings introduced
- [ ] No secrets, credentials, or sensitive data committed

### Testing
- [ ] `/halo-test-review` has been run and coverage is adequate
- [ ] All new logic has corresponding tests
- [ ] All existing tests pass
- [ ] Edge cases identified in the plan are covered by tests

### Process
- [ ] PR description references the plan document
- [ ] Commits are logical and have clear messages
- [ ] No unrelated changes included

---

## Reviewer Notes

<!-- Anything the reviewer should pay particular attention to -->
