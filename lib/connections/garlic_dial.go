// Copyright (C) 2016 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package connections

import (
	"context"
	"crypto/tls"
	"net"
	"net/url"
	"time"

	"github.com/go-i2p/onramp"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/connections/registry"
	"github.com/syncthing/syncthing/lib/dialer"
	"github.com/syncthing/syncthing/lib/protocol"
)

func init() {
	factory := &garlicDialerFactory{}
	for _, scheme := range []string{"garlic", "garlic4", "garlic6"} {
		dialers[scheme] = factory
	}
}

type garlicDialer struct {
	commonDialer

	garlic *onramp.Garlic
}

func (d *garlicDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return d.garlic.DialContext(ctx, network, addr)
}

func (d *garlicDialer) Dial(ctx context.Context, _ protocol.DeviceID, uri *url.URL) (internalConn, error) {
	// For I2P, we don't want a port in the address passed to the SAM API.
	host := uri.Hostname()

	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := d.DialContext(timeoutCtx, uri.Scheme, host)
	if err != nil {
		return internalConn{}, err
	}

	// Apply traffic class if configured (matching listener behavior)
	if tc := d.trafficClass; tc != 0 {
		if err := dialer.SetTrafficClass(conn, tc); err != nil {
			l.Debugln("Dial (garlic): setting traffic class:", err)
		}
	}

	tc := tls.Client(conn, d.tlsCfg)
	err = tlsTimedHandshake(tc)
	if err != nil {
		tc.Close()
		return internalConn{}, err
	}

	priority := d.wanPriority
	isLocal := false

	return newInternalConn(tc, connTypeGarlicClient, isLocal, priority), nil
}

type garlicDialerFactory struct {
	invalidated error
}

func (g *garlicDialerFactory) New(opts config.OptionsConfiguration, tlsCfg *tls.Config, registry *registry.Registry, lanChecker *lanChecker) genericDialer {
	garlic, err := onramp.NewGarlic("syncthing-dial", onramp.SAM_ADDR, onramp.OPT_WIDE)
	if err != nil {
		// TODO: learn syncthing's standard logging practices and implement them here
		l.Debugln("Failed to create garlic dialer:", err)
		g.invalidated = err
	}
	return &garlicDialer{
		commonDialer: commonDialer{
			trafficClass:      opts.TrafficClass,
			reconnectInterval: time.Duration(opts.ReconnectIntervalS) * time.Second,
			tlsCfg:            tlsCfg,
			lanChecker:        lanChecker,
			lanPriority:       opts.ConnectionPriorityGarlicLAN,
			wanPriority:       opts.ConnectionPriorityGarlicWAN,
			allowsMultiConns:  true,
		},
		garlic: garlic,
	}
}

func (garlicDialerFactory) AlwaysWAN() bool {
	return true
}

func (g *garlicDialerFactory) Valid(_ config.Configuration) error {
	return g.invalidated
}

func (garlicDialerFactory) String() string {
	return "Garlic Dialer"
}
