// Copyright (C) 2016 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package connections

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/go-i2p/onramp"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/connections/registry"
	"github.com/syncthing/syncthing/lib/dialer"
	"github.com/syncthing/syncthing/lib/protocol"
)

func init() {
	factory := &onionDialerFactory{}
	for _, scheme := range []string{"onion", "onion4", "onion6"} {
		dialers[scheme] = factory
	}
}

type onionDialer struct {
	commonDialer

	onion *onramp.Onion
}

func (d *onionDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if d.onion == nil {
		return nil, fmt.Errorf("onion dialer not initialized (SAM unavailable)")
	}
	return d.onion.Dial(network, addr)
}

func (d *onionDialer) Dial(ctx context.Context, _ protocol.DeviceID, uri *url.URL) (internalConn, error) {
	if uri == nil {
		return internalConn{}, fmt.Errorf("onion dialer: nil URI")
	}
	// For I2P, addresses are cryptographic hashes; we pass the full host
	// (without port) to the SAM API. There is never a configured hostname.
	host := uri.Host
	if colon := strings.LastIndex(host, ":"); colon != -1 {
		host = host[:colon]
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := d.DialContext(timeoutCtx, "onion", host)
	if err != nil {
		return internalConn{}, err
	}

	// Apply traffic class if configured (matching listener behavior)
	if tc := d.trafficClass; tc != 0 {
		if err := dialer.SetTrafficClass(conn, tc); err != nil {
			l.Debugln("Dial (onion): setting traffic class:", err)
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
	if d.lanChecker != nil && conn != nil {
		isLocal = d.lanChecker.isLAN(conn.RemoteAddr())
		if isLocal {
			priority = d.lanPriority
		}
	}

	return newInternalConn(tc, connTypeOnionClient, isLocal, priority), nil
}

type onionDialerFactory struct {
	invalidated error
}

func (g *onionDialerFactory) New(opts config.OptionsConfiguration, tlsCfg *tls.Config, registry *registry.Registry, lanChecker *lanChecker) genericDialer {
	onion, err := onramp.NewOnion("syncthing-dial")
	if err != nil {
		l.Debugln("Failed to create onion dialer:", err)
		g.invalidated = err
		return &onionDialer{
			commonDialer: commonDialer{
				trafficClass:      opts.TrafficClass,
				reconnectInterval: time.Duration(opts.ReconnectIntervalS) * time.Second,
				tlsCfg:            tlsCfg,
				lanChecker:        lanChecker,
				lanPriority:       opts.ConnectionPriorityOnionLAN,
				wanPriority:       opts.ConnectionPriorityOnionWAN,
				allowsMultiConns:  true,
			},
			onion: nil,
		}
	}
	return &onionDialer{
		commonDialer: commonDialer{
			trafficClass:      opts.TrafficClass,
			reconnectInterval: time.Duration(opts.ReconnectIntervalS) * time.Second,
			tlsCfg:            tlsCfg,
			lanChecker:        lanChecker,
			lanPriority:       opts.ConnectionPriorityOnionLAN,
			wanPriority:       opts.ConnectionPriorityOnionWAN,
			allowsMultiConns:  true,
		},
		onion: onion,
	}
}

func (onionDialerFactory) AlwaysWAN() bool {
	return true
}

func (g *onionDialerFactory) Valid(_ config.Configuration) error {
	return g.invalidated
}

func (onionDialerFactory) String() string {
	return "Onion Dialer"
}
