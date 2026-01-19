// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	sonicjson "github.com/xataio/pgstream/internal/json"
	"github.com/xataio/pgstream/pkg/wal"
)

// TestJSONBSerializationConsistency tests that JSONB values parsed with Sonic
// can be correctly re-serialized for PostgreSQL insertion.
//
// This test reproduces a bug where:
// 1. wal2json outputs JSONB data as nested JSON
// 2. Sonic parses it into map[string]any
// 3. pgx re-serializes with encoding/json (different library)
// 4. The output can differ, causing "invalid input syntax for type json" errors
func TestJSONBSerializationConsistency(t *testing.T) {
	t.Parallel()

	// This is similar to what wal2json outputs for a JSONB column
	// containing LinkedIn profile data with emojis and special characters
	wal2jsonOutput := `{
		"action": "I",
		"schema": "public",
		"table": "profiles",
		"columns": [
			{"name": "id", "type": "integer", "value": 69},
			{"name": "person_name", "type": "text", "value": "\"Matheus Macedo\""},
			{"name": "profile_data", "type": "jsonb", "value": {
				"name": "David Richard 🏳️‍🌈",
				"location": "São Paulo, SP",
				"about": "Software Engineer with 8+ years…",
				"url": "https://www.linkedin.com/in/test",
				"nested": {
					"key": "value with \"quotes\" inside",
					"unicode": "Pontifícia Universidade"
				}
			}}
		]
	}`

	// Step 1: Parse with Sonic (like pgstream does with wal2json)
	var walData wal.Data
	err := sonicjson.Unmarshal([]byte(wal2jsonOutput), &walData)
	require.NoError(t, err)

	// Find the JSONB column
	var jsonbValue any
	for _, col := range walData.Columns {
		if col.Type == "jsonb" {
			jsonbValue = col.Value
			break
		}
	}
	require.NotNil(t, jsonbValue, "JSONB column should be present")

	// Step 2: Re-serialize with encoding/json (what pgx does internally)
	stdJSONBytes, err := json.Marshal(jsonbValue)
	require.NoError(t, err)

	// Step 3: Re-serialize with Sonic (what we should use for consistency)
	sonicJSONBytes, err := sonicjson.Marshal(jsonbValue)
	require.NoError(t, err)

	// Step 4: Parse both back and compare
	// The key issue is that different JSON libraries may encode special
	// characters differently, which can cause PostgreSQL to reject the JSON
	var stdParsed, sonicParsed map[string]any
	err = json.Unmarshal(stdJSONBytes, &stdParsed)
	require.NoError(t, err)
	err = json.Unmarshal(sonicJSONBytes, &sonicParsed)
	require.NoError(t, err)

	// Both should produce logically equivalent JSON
	// If this test fails, it means there's a serialization mismatch
	t.Logf("Standard JSON output: %s", string(stdJSONBytes))
	t.Logf("Sonic JSON output: %s", string(sonicJSONBytes))

	// The outputs should be equivalent (semantically equal JSON)
	require.Equal(t, stdParsed, sonicParsed, "JSON libraries should produce equivalent output")
}

// TestFilterRowColumnsJSONBHandling tests that filterRowColumns correctly
// handles JSONB columns by pre-serializing them with Sonic.
func TestFilterRowColumnsJSONBHandling(t *testing.T) {
	t.Parallel()

	// Simulate JSONB data that came from wal2json via Sonic parsing
	wal2jsonOutput := `{
		"name": "Test User 🏳️‍🌈",
		"data": {
			"nested": "value with \"quotes\"",
			"unicode": "São Paulo"
		}
	}`

	// Parse with Sonic (simulating what happens when wal2json data arrives)
	var jsonbValue map[string]any
	err := sonicjson.Unmarshal([]byte(wal2jsonOutput), &jsonbValue)
	require.NoError(t, err)

	// Create WAL columns including a JSONB column
	cols := []wal.Column{
		{Name: "id", Type: "integer", Value: 1},
		{Name: "name", Type: "text", Value: "Test"},
		{Name: "profile_data", Type: "jsonb", Value: jsonbValue},
	}

	// Create adapter
	adapter := &dmlAdapter{
		forCopy: false,
	}

	// Filter columns
	colNames, values := adapter.filterRowColumns(cols, schemaInfo{})

	require.Len(t, colNames, 3)
	require.Len(t, values, 3)

	// The JSONB value should be usable by PostgreSQL
	// After our fix, it should be []byte (pre-serialized JSON)
	jsonbResult := values[2]

	// Check if the value is properly handled for PostgreSQL
	// With the fix, it should be []byte; without fix, it's map[string]any
	switch v := jsonbResult.(type) {
	case []byte:
		// This is what we want after the fix - pre-serialized JSON
		t.Logf("JSONB value is pre-serialized bytes: %s", string(v))
		// Verify it's valid JSON
		var parsed map[string]any
		err := json.Unmarshal(v, &parsed)
		require.NoError(t, err, "Pre-serialized JSONB should be valid JSON")
	case map[string]any:
		// This is what happens without the fix - pgx will re-serialize
		t.Logf("JSONB value is map[string]any (not pre-serialized)")
		// This can cause issues if pgx uses encoding/json which differs from Sonic
		t.Error("JSONB should be pre-serialized to []byte to ensure consistent encoding")
	default:
		t.Errorf("Unexpected JSONB value type: %T", jsonbResult)
	}
}
