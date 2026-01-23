# E2E Testing Guide

This guide explains how to run end-to-end (E2E) tests for the Anthropic integration that use a real Temporal server and real Anthropic API calls.

---

## Overview

The E2E tests validate the complete integration:
- Real Temporal server connection
- Real Anthropic API calls (requires API key)
- Full workflow execution
- Deterministic replay verification
- Error handling with real API responses

**Test File:** `tests/contrib/anthropic/test_e2e.py`

---

## Prerequisites

### 1. Temporal Server

Start a local Temporal development server:

```bash
temporal server start-dev
```

The server will run on `localhost:7233` by default.

### 2. Anthropic API Key

Set your Anthropic API key as an environment variable:

```bash
export ANTHROPIC_API_KEY="your_api_key_here"
```

Get an API key from: https://console.anthropic.com/

### 3. Install Dependencies

```bash
pip install -e ".[dev]"
pip install anthropic pytest pytest-asyncio
```

---

## Running E2E Tests

### Run All E2E Tests

```bash
ANTHROPIC_API_KEY=your_key pytest tests/contrib/anthropic/test_e2e.py -v
```

### Run Specific E2E Test

```bash
ANTHROPIC_API_KEY=your_key pytest tests/contrib/anthropic/test_e2e.py::test_simple_workflow_with_real_api -v
```

### Run E2E Tests with Custom Temporal Address

```bash
ANTHROPIC_API_KEY=your_key TEMPORAL_ADDRESS=my-server:7233 pytest tests/contrib/anthropic/test_e2e.py -v
```

### Skip E2E Tests in Normal Test Runs

```bash
# Run all tests EXCEPT E2E tests
pytest tests/contrib/anthropic/ -v -k "not e2e"
```

---

## E2E Test Suite

### Test 1: Simple Workflow with Real API

**File:** `test_simple_workflow_with_real_api`

**Purpose:** Basic integration test with single prompt

**What it tests:**
- Worker can start with AnthropicAgentsPlugin
- TemporalTransport can make real API calls
- invoke_llm_activity executes successfully
- Response is received and returned from workflow

**Expected behavior:**
- Sends prompt: "What is 2+2? Answer with just the number."
- Receives response containing "4"
- Completes without errors

### Test 2: Multi-Turn Conversation

**File:** `test_multi_turn_conversation`

**Purpose:** Validate conversation history is maintained

**What it tests:**
- Multiple prompts in sequence
- Conversation context preservation
- History passed between turns

**Expected behavior:**
- Turn 1: "My favorite number is 7."
- Turn 2: "What is my favorite number?"
- Response should reference "7" from context

### Test 3: Workflow Replay Determinism

**File:** `test_workflow_replay_determinism`

**Purpose:** Verify deterministic execution and history recording

**What it tests:**
- Workflow completes and generates history
- History events are recorded correctly
- Workflow can be retrieved and inspected

**Expected behavior:**
- Workflow executes with API call
- History contains multiple events
- History can be fetched from server

### Test 4: Error Handling with Real API

**File:** `test_error_handling_with_real_api`

**Purpose:** Validate error propagation with invalid API calls

**What it tests:**
- Invalid model name triggers error
- Activity errors are caught by workflow
- Error messages are meaningful

**Expected behavior:**
- Uses invalid model "claude-invalid-model-xyz"
- Workflow catches error gracefully
- Returns error message (not crash)

---

## Manual Testing

You can run the E2E tests manually using the `__main__` block:

```bash
# Set API key
export ANTHROPIC_API_KEY=your_key

# Start Temporal server
temporal server start-dev

# Run tests directly
python tests/contrib/anthropic/test_e2e.py
```

Output:
```
Running E2E tests against localhost:7233...
Make sure Temporal server is running: temporal server start-dev

Test 1: Simple workflow...
✓ Simple workflow result: 4

Test 2: Multi-turn conversation...
✓ Multi-turn conversation:
  Turn 1: I've noted that your favorite number is 7...
  Turn 2: Your favorite number is 7...

Test 3: Workflow replay...
✓ Workflow completed with 15 history events
✓ Result: test

Test 4: Error handling...
✓ Error handling result: Error caught: ActivityError

✅ All E2E tests passed!
```

---

## Troubleshooting

### Error: "ANTHROPIC_API_KEY environment variable not set"

**Solution:** Set the API key:
```bash
export ANTHROPIC_API_KEY=your_key_here
```

### Error: "Connection refused to localhost:7233"

**Solution:** Start Temporal server:
```bash
temporal server start-dev
```

### Error: "API rate limit exceeded"

**Solution:** Wait a few seconds between test runs. E2E tests make real API calls.

### Error: "Invalid API key"

**Solution:** Verify your API key is correct:
```bash
# Test with curl
curl https://api.anthropic.com/v1/messages \
  -H "x-api-key: $ANTHROPIC_API_KEY" \
  -H "anthropic-version: 2023-06-01" \
  -H "content-type: application/json" \
  -d '{
    "model": "claude-sonnet-4-5",
    "max_tokens": 10,
    "messages": [{"role": "user", "content": "test"}]
  }'
```

### Tests Are Slow

**Reason:** E2E tests make real API calls which take time.

**Normal Duration:**
- Simple workflow: ~2-3 seconds
- Multi-turn: ~5-7 seconds
- All E2E tests: ~15-20 seconds

---

## CI/CD Integration

### Skip E2E Tests in CI

If you don't want to run E2E tests in CI (to avoid API costs):

```yaml
# .github/workflows/test.yml
- name: Run unit and integration tests
  run: pytest tests/contrib/anthropic/ -v -k "not e2e"
```

### Run E2E Tests in CI (Optional)

If you want to run E2E tests in CI:

```yaml
# .github/workflows/test.yml
- name: Start Temporal server
  run: |
    temporal server start-dev &
    sleep 5

- name: Run E2E tests
  env:
    ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
  run: pytest tests/contrib/anthropic/test_e2e.py -v
```

---

## Test Markers

E2E tests use pytest markers for organization:

```python
@pytest.mark.e2e           # Marks test as E2E test
@pytest.mark.asyncio       # Marks test as async
```

Filter by marker:
```bash
# Run only E2E tests
pytest -m e2e -v

# Run everything except E2E
pytest -m "not e2e" -v
```

---

## Cost Considerations

**API Costs:**
- Each E2E test makes 1-3 real API calls
- Simple prompts use ~50-200 tokens
- Full test suite: ~500-1000 tokens total
- Cost: ~$0.01-0.05 per full test run (with Claude Sonnet 4.5)

**Recommendations:**
- Run E2E tests sparingly during development
- Use unit tests (with mocks) for fast iteration
- Run E2E tests before committing changes
- Consider disabling E2E tests in CI to save costs

---

## Test Coverage

### What E2E Tests Cover

✅ **Covered by E2E tests:**
- Real Temporal server connection
- Real Anthropic API calls
- Full workflow execution lifecycle
- Worker startup with plugin
- Activity execution with retry handling
- Multi-turn conversation flow
- Workflow history recording
- Error propagation from API to workflow

❌ **NOT covered by E2E tests (use unit tests instead):**
- Message format conversion edge cases
- Retry header parsing logic
- Status code classification
- Transport lifecycle methods in isolation
- Parameter validation

### Test Pyramid

```
      /\
     /  \  E2E Tests (4 tests)
    /____\  - Real Temporal + API
   /      \  - 15-20 seconds
  /________\ - ~500-1000 tokens
 /          \
/____________\ Integration Tests (2 tests)
|            | - Mocked Temporal environment
|            | - ~5 seconds
|____________|

|            | Unit Tests (36 tests)
|            | - All mocks
|            | - <1 second
|____________|
```

---

## Next Steps

After E2E tests pass:

1. **Full SDK Integration** - Test with real Claude Agent SDK `query()` function
2. **Tool Calling E2E** - Test with activity_as_tool() and real tool execution
3. **MCP E2E** - Test with MCP servers when implemented
4. **Performance Testing** - Measure workflow replay speed with long conversations

---

**Status:** E2E test suite ready for execution
**Prerequisites:** Temporal server + ANTHROPIC_API_KEY
**Test Count:** 4 E2E tests
**Coverage:** Full integration from worker to API

