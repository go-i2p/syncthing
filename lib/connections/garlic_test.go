package connections

import (
	"testing"
)

func TestGarlicDialerAlwaysWAN(t *testing.T) {
	f := garlicDialerFactory{}
	if !f.AlwaysWAN() {
		t.Fatal("expected AlwaysWAN true")
	}
}

func TestGarlicDialerString(t *testing.T) {
	f := garlicDialerFactory{}
	if f.String() != "Garlic Dialer" {
		t.Fatal("unexpected string")
	}
}
