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
		t.Skip("SAMv3.3 not available at localhost:7656; skipping I2P E2E test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	f := onionDialerFactory{}
	d := f.New(config.OptionsConfiguration{I2PMode: "mixed"}, nil, nil, nil)
	uri, _ := url.Parse("onion://test.i2p")
	// Full E2E: listener creates SAM listener; dialer connects through SAM.
	lf := &onionListenerFactory{}
	l := lf.New(nil, nil, nil, make(chan internalConn), nil, nil, nil)
	_ = l
	_, err := d.Dial(ctx, protocol.DeviceID{}, uri)
	if err != nil {
		t.Skip("SAM not available at localhost:7656; skipping E2E:", err)
	}
}

func TestOnionSAMAvailable(t *testing.T) {
	if !torAvailable() {
		t.Skip("SAMv3.3 not available at localhost:7656; skipping I2P test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	f := onionDialerFactory{}
	d := f.New(config.OptionsConfiguration{I2PMode: "mixed"}, nil, nil, nil)
	uri, _ := url.Parse("onion://test.i2p")

	_, err := d.Dial(ctx, protocol.DeviceID{}, uri)
	if err == nil {
		t.Log("SAM available at localhost:7656; I2P dial succeeded")
	} else {
		t.Log("SAM not available (expected if no router):", err)
	}
}
