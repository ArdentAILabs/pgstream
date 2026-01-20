// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	sonicjson "github.com/xataio/pgstream/internal/json"
	"github.com/xataio/pgstream/pkg/wal"
)

// TestFilterRowColumnsJSONBHandling verifies JSONB columns are pre-serialized
// with Sonic to prevent encoding mismatches with pgx (which uses encoding/json).
func TestFilterRowColumnsJSONBHandling(t *testing.T) {
	t.Parallel()

	// JSONB with emoji, unicode, and quotes - the problematic cases
	var jsonbValue map[string]any
	err := sonicjson.Unmarshal([]byte(`{
		"name": "David Richard 🏳️‍🌈",
		"location": "São Paulo",
		"quote": "said \"hello\""
	}`), &jsonbValue)
	require.NoError(t, err)

	cols := []wal.Column{
		{Name: "id", Type: "integer", Value: 1},
		{Name: "data", Type: "jsonb", Value: jsonbValue},
	}

	adapter := &dmlAdapter{forCopy: false}
	_, values := adapter.filterRowColumns(cols, schemaInfo{})

	// After fix: JSONB should be []byte (pre-serialized), not map[string]any
	jsonbResult, ok := values[1].([]byte)
	require.True(t, ok, "JSONB should be pre-serialized to []byte, got %T", values[1])

	// Verify it's valid JSON
	var parsed map[string]any
	require.NoError(t, json.Unmarshal(jsonbResult, &parsed))
	require.Equal(t, "David Richard 🏳️‍🌈", parsed["name"])
}

// TestFilterRowColumnsJSONBStringNotDoubleEncoded verifies that when JSONB value
// is already a string (like from schemalog snapshot generator), it should NOT
// be double-encoded. This tests the bug reported by bugbot.
func TestFilterRowColumnsJSONBStringNotDoubleEncoded(t *testing.T) {
	t.Parallel()

	// This simulates what schemalog snapshot generator does:
	// It marshals Schema to JSON bytes, then passes string(schema) as the value
	originalJSON := `{"tables":[{"name":"users"}]}`

	cols := []wal.Column{
		{Name: "id", Type: "integer", Value: 1},
		{Name: "schema", Type: "jsonb", Value: originalJSON}, // string, not map!
	}

	adapter := &dmlAdapter{forCopy: false}
	_, values := adapter.filterRowColumns(cols, schemaInfo{})

	// The value should NOT be double-encoded
	// If buggy: it would become `"{\"tables\":[{\"name\":\"users\"}]}"`
	// If correct: it should remain as-is or be []byte of the same JSON

	switch v := values[1].(type) {
	case []byte:
		// If converted to []byte, it should be the SAME JSON, not double-encoded
		require.Equal(t, originalJSON, string(v),
			"JSONB string should not be double-encoded")
	case string:
		// If kept as string, it should be unchanged
		require.Equal(t, originalJSON, v,
			"JSONB string should not be modified")
	default:
		t.Fatalf("Unexpected type for JSONB value: %T", values[1])
	}

	// Extra check: the result should be valid JSON that parses to the original structure
	var resultBytes []byte
	switch v := values[1].(type) {
	case []byte:
		resultBytes = v
	case string:
		resultBytes = []byte(v)
	}

	var parsed map[string]any
	err := json.Unmarshal(resultBytes, &parsed)
	require.NoError(t, err, "Result should be valid JSON, got: %s", string(resultBytes))

	tables, ok := parsed["tables"].([]any)
	require.True(t, ok, "Should have tables array")
	require.Len(t, tables, 1)
}
