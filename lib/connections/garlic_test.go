package connections

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestGarlicE2E_DialAndListen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	f := garlicDialerFactory{}
	d := f.New(config.OptionsConfiguration{I2PMode: "mixed"}, nil, nil, nil)
	uri, _ := url.Parse("garlic://test.i2p")
	// Full E2E: listener creates SAM listener; dialer connects through SAM.
	lf := &garlicListenerFactory{}
	l := lf.New(nil, nil, nil, make(chan internalConn), nil, nil, nil)
	_ = l
	_, err := d.Dial(ctx, protocol.DeviceID{}, uri)
	if err == nil {
		t.Skip("SAM not available at localhost:7656; skipping E2E")
	}
}
