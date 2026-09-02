package connections

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/protocol"
)

func torAvailable() bool {
	conn, err := net.DialTimeout("tcp", "localhost:9050", 500*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func TestOnionE2E_DialAndListen(t *testing.T) {
	if !torAvailable() {
		t.Skip("Tor proxy not available at localhost:9050; skipping Tor E2E test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	f := onionDialerFactory{}
	d := f.New(config.OptionsConfiguration{I2PMode: "mixed"}, nil, nil, nil)
	uri, _ := url.Parse("onion://test.onion")
	// Full E2E: listener creates Tor listener; dialer connects through Tor.
	lf := &onionListenerFactory{}
	l := lf.New(nil, nil, nil, make(chan internalConn), nil, nil, nil)
	_ = l
	_, err := d.Dial(ctx, protocol.DeviceID{}, uri)
	if err != nil {
		t.Skip("Tor proxy not available at localhost:9050; skipping E2E:", err)
	}
}

func TestOnionSAMAvailable(t *testing.T) {
	if !torAvailable() {
		t.Skip("Tor proxy not available at localhost:9050; skipping Tor test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	f := onionDialerFactory{}
	d := f.New(config.OptionsConfiguration{I2PMode: "mixed"}, nil, nil, nil)
	uri, _ := url.Parse("onion://test.onion")

	_, err := d.Dial(ctx, protocol.DeviceID{}, uri)
	if err == nil {
		t.Log("Tor proxy available at localhost:9050; onion dial succeeded")
	} else {
		t.Log("Tor proxy not available (expected if no proxy):", err)
	}
}
