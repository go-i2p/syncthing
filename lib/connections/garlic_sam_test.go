package connections

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestGarlicSAMAvailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	f := garlicDialerFactory{}
	d := f.New(config.OptionsConfiguration{I2PMode: "mixed"}, nil, nil, nil)
	uri, _ := url.Parse("garlic://test.i2p")

	_, err := d.Dial(ctx, protocol.DeviceID{}, uri)
	if err == nil {
		t.Log("SAM available at localhost:7656; I2P dial succeeded")
	} else {
		t.Log("SAM not available (expected if no router):", err)
	}
}
