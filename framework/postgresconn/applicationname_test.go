package postgresconn

import (
	"strings"
	"testing"
)

// TestWithApplicationName covers the opt-in default, the role suffix, quoting,
// and the 63-byte cap keeping the role intact.
func TestWithApplicationName(t *testing.T) {
	t.Cleanup(func() { SetApplicationName("") })
	const dsn = "host='h' port='5432'"

	SetApplicationName("")
	if got := WithApplicationName(dsn, "config"); got != dsn {
		t.Fatalf("unset base changed the DSN: %q", got)
	}

	SetApplicationName("bifrost-node-1")
	if got := WithApplicationName(dsn, "logs"); got != dsn+" application_name='bifrost-node-1-logs'" {
		t.Fatalf("got %q", got)
	}

	SetApplicationName("bifrost-" + strings.Repeat("x", 80))
	got := WithApplicationName(dsn, "config")
	name := strings.TrimSuffix(strings.TrimPrefix(got, dsn+" application_name='"), "'")
	if len(name) != maxApplicationNameLen || !strings.HasSuffix(name, "-config") {
		t.Fatalf("long base not capped with role kept: %q (%d bytes)", name, len(name))
	}
}
