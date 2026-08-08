// Copyright (C) 2016 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package connections

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/go-i2p/i2pkeys"
	"github.com/go-i2p/onramp"
	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/connections/registry"
	"github.com/syncthing/syncthing/lib/dialer"
	"github.com/syncthing/syncthing/lib/nat"
	"github.com/syncthing/syncthing/lib/svcutil"
)

func init() {
	factory := &garlicListenerFactory{}
	for _, scheme := range []string{"garlic", "garlic4", "garlic6"} {
		listeners[scheme] = factory
	}
}

type garlicListener struct {
	svcutil.ServiceWithError
	onAddressesChangedNotifier

	uri      *url.URL
	cfg      config.Wrapper
	tlsCfg   *tls.Config
	conns    chan internalConn
	factory  listenerFactory
	registry *registry.Registry

	// laddr net.Addr
	listener net.Listener

	mut sync.RWMutex
}

func (t *garlicListener) serve(ctx context.Context) error {
	gaddr, err := i2pkeys.Lookup(t.uri.Host)
	if err != nil {
		slog.WarnContext(ctx, "Failed to listen (Garlic)", slogutil.Error(err))
		return err
	}

	lc := net.ListenConfig{
		Control: dialer.ReusePortControl,
	}

	listener, err := lc.Listen(context.TODO(), t.uri.Scheme, gaddr.String())
	if err != nil {
		slog.WarnContext(ctx, "Failed to listen (Garlic)", slogutil.Error(err))
		return err
	}
	defer listener.Close()

	// We might bind to :0, so use the port we've been given.
	gaddr = listener.Addr().(*i2pkeys.I2PAddr)

	t.notifyAddressesChanged(t)
	defer t.clearAddresses(t)

	t.registry.Register(t.uri.Scheme, gaddr)
	defer t.registry.Unregister(t.uri.Scheme, gaddr)

	slog.InfoContext(ctx, "Garlic listener starting", slogutil.Address(gaddr))
	defer slog.InfoContext(ctx, "Garlic listener shutting down", slogutil.Address(gaddr))

	acceptFailures := 0
	const maxAcceptFailures = 10

	// :(, but what can you do.
	//tcpListener := listener.(*)

	for {
		conn, err := listener.Accept()
		select {
		case <-ctx.Done():
			if err == nil {
				conn.Close()
			}
			return nil
		default:
		}
		if err != nil {
			var ne *net.OpError
			if ok := errors.As(err, &ne); !ok || !ne.Timeout() {
				slog.WarnContext(ctx, "Failed to accept Garlic connection", slogutil.Error(err))

				acceptFailures++
				if acceptFailures > maxAcceptFailures {
					// Return to restart the listener, because something
					// seems permanently damaged.
					return err
				}

				// Slightly increased delay for each failure.
				time.Sleep(time.Duration(acceptFailures) * time.Second)
			}
			continue
		}

		acceptFailures = 0
		l.Debugln("Listen (Garlic): connect from", conn.RemoteAddr())

		if tc := t.cfg.Options().TrafficClass; tc != 0 {
			if err := dialer.SetTrafficClass(conn, tc); err != nil {
				l.Debugln("Listen (Garlic): setting traffic class:", err)
			}
		}

		tc := tls.Server(conn, t.tlsCfg)
		if err := tlsTimedHandshake(tc); err != nil {
			slog.WarnContext(ctx, "Failed TLS handshake", slogutil.Address(tc.RemoteAddr()), slogutil.Error(err))
			tc.Close()
			continue
		}

		priority := t.cfg.Options().ConnectionPriorityGarlicWAN
		isLocal := false
		t.conns <- newInternalConn(tc, connTypeGarlicServer, isLocal, priority)
	}
}

func (t *garlicListener) URI() *url.URL {
	// craft the URL from the listener
	if t.listener != nil {
		host := fmt.Sprintf("%s:%s", t.listener.Addr().String(), config.DefaultGarlicPort)
		scheme := "garlic"
		uri := &url.URL{
			Scheme: scheme,
			Host:   host,
		}
		return uri
	}
	return nil
}

func (t *garlicListener) WANAddresses() []*url.URL {
	addrs := []*url.URL{}
	return addrs
}

func (t *garlicListener) LANAddresses() []*url.URL {
	addrs := []*url.URL{}
	return addrs
}

func (t *garlicListener) String() string {
	return t.uri.String()
}

func (t *garlicListener) Factory() listenerFactory {
	return t.factory
}

func (*garlicListener) NATType() string {
	return "unknown"
}

type garlicListenerFactory struct {
	Garlic      *onramp.Garlic
	invalidated error
}

func (f *garlicListenerFactory) New(uri *url.URL, cfg config.Wrapper, tlsCfg *tls.Config, conns chan internalConn, natService *nat.Service, registry *registry.Registry, lanChecker *lanChecker) genericListener {
	if f.Garlic == nil {
		var err error
		f.Garlic, err = onramp.NewGarlic("syncthing-listen", onramp.SAM_ADDR, onramp.OPT_WIDE)
		if err != nil {
			f.invalidated = err
			l.Debugf("SAMv3 connection to I2P failed: %s", err)
		}
	}
	listener, err := f.Garlic.Listen()
	if err != nil {
		l.Debugf("SAMv3 Listener setup failed, cannot listen on I2P: %s", err)
	}
	// Likely design:
	// URL must be nil, we compute it from the listener.
	// No nat.Service, NAT traversal is handled by I2P
	l := &garlicListener{
		uri:      fixupPort(uri, config.DefaultGarlicPort),
		cfg:      cfg,
		tlsCfg:   tlsCfg,
		conns:    conns,
		factory:  f,
		listener: listener,
		registry: registry,
	}
	l.ServiceWithError = svcutil.AsService(l.serve, l.String())
	return l
}

func (f *garlicListenerFactory) Valid(_ config.Configuration) error {
	// Garlic must not be nil
	if f.Garlic == nil {
		return fmt.Errorf("Garlic is nil, garlicListenerFactory was not instantiated: %s", f.invalidated)
	}
	// Garlic setup failed earlier and the transport was invalidated
	if f.invalidated != nil {
		return f.invalidated
	}
	return nil
}
