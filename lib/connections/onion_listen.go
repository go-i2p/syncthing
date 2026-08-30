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

	"github.com/go-i2p/onramp"
	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/connections/registry"
	"github.com/syncthing/syncthing/lib/dialer"
	"github.com/syncthing/syncthing/lib/nat"
	"github.com/syncthing/syncthing/lib/svcutil"
)

func init() {
	factory := &onionListenerFactory{}
	for _, scheme := range []string{"onion", "onion4", "onion6"} {
		listeners[scheme] = factory
	}
}

type onionListener struct {
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

func (t *onionListener) serve(ctx context.Context) error {
	var listener net.Listener

	// Tor onion listener creates its own .onion address via the Tor client.
	if t.listener == nil {
		return fmt.Errorf("onion listener: factory did not create a listener")
	}

	// Use the factory-created listener (Tor generates its own onion address)
	t.mut.RLock()
	listener = t.listener
	t.mut.RUnlock()
	if listener == nil {
		slog.WarnContext(ctx, "Onion listener disabled: factory listener is nil")
		return fmt.Errorf("onion listener: factory listener is nil")
	}
	addr := listener.Addr()
	if addr == nil {
		slog.WarnContext(ctx, "Onion listener disabled: listener has no address")
		return fmt.Errorf("onion listener: listener has no address")
	}
	host := addr.String()
	t.mut.Lock()
	t.uri = &url.URL{
		Scheme: "onion",
		Host:   host,
	}
	t.mut.Unlock()

	t.notifyAddressesChanged(t)
	defer t.clearAddresses(t)

	t.registry.Register(t.uri.String(), addr)
	defer t.registry.Unregister(t.uri.String(), addr)

	slog.InfoContext(ctx, "Onion listener starting", slogutil.Address(addr))
	defer slog.InfoContext(ctx, "Onion listener shutting down", slogutil.Address(addr))

	acceptFailures := 0
	const maxAcceptFailures = 10

	for {
		// Tor listener is not a TCPListener; SetDeadline doesn't apply.
		// Rely on ctx cancellation via Accept blocking behavior.
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
				slog.WarnContext(ctx, "Failed to accept Onion connection", slogutil.Error(err))

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
		l.Debugln("Listen (Onion): connect from", conn.RemoteAddr())

		if tc := t.cfg.Options().TrafficClass; tc != 0 {
			if err := dialer.SetTrafficClass(conn, tc); err != nil {
				l.Debugln("Listen (Onion): setting traffic class:", err)
			}
		}

		tc := tls.Server(conn, t.tlsCfg)
		if err := tlsTimedHandshake(tc); err != nil {
			slog.WarnContext(ctx, "Failed TLS handshake", slogutil.Address(tc.RemoteAddr()), slogutil.Error(err))
			tc.Close()
			continue
		}

		priority := t.cfg.Options().ConnectionPriorityOnionWAN
		isLocal := false
		t.conns <- newInternalConn(tc, connTypeOnionServer, isLocal, priority)
	}
}

func (t *onionListener) URI() *url.URL {
	t.mut.RLock()
	defer t.mut.RUnlock()
	if t.listener != nil {
		if addr := t.listener.Addr(); addr != nil {
			host := addr.String()
			return &url.URL{
				Scheme: "onion",
				Host:   host,
			}
		}
	}
	if t.uri != nil {
		return t.uri
	}
	return nil
}

func (t *onionListener) WANAddresses() []*url.URL {
	if t.URI() != nil {
		return []*url.URL{t.URI()}
	}
	return nil
}

func (t *onionListener) LANAddresses() []*url.URL {
	return nil // Tor has no LAN concept
}

func (t *onionListener) String() string {
	if t.uri == nil {
		return "onion://disabled"
	}
	return t.uri.String()
}

func (t *onionListener) Factory() listenerFactory {
	return t.factory
}

func (*onionListener) NATType() string {
	return "unknown"
}

type onionListenerFactory struct {
	Onion       *onramp.Onion
	invalidated error
}

func (f *onionListenerFactory) New(uri *url.URL, cfg config.Wrapper, tlsCfg *tls.Config, conns chan internalConn, natService *nat.Service, registry *registry.Registry, lanChecker *lanChecker) genericListener {
	if f.Onion == nil {
		var err error
		f.Onion, err = onramp.NewOnion("syncthing-listen")
		if err != nil {
			f.invalidated = err
			l.Debugf("SAMv3 connection to I2P failed: %s", err)
			return &onionListener{
				uri:      nil,
				cfg:      cfg,
				tlsCfg:   tlsCfg,
				conns:    conns,
				factory:  f,
				listener: nil,
				registry: registry,
				ServiceWithError: svcutil.AsService(func(ctx context.Context) error {
					slog.WarnContext(ctx, "Onion listener disabled: Tor unavailable")
					<-ctx.Done()
					return ctx.Err()
				}, "onion-disabled"),
			}
		}
	}
	var listener net.Listener
	var listenerURI *url.URL
	if f.invalidated == nil {
		var err error
		listener, err = f.Onion.Listen()
		if err != nil {
			f.invalidated = err
			l.Debugf("Tor listener setup failed, cannot listen on onion: %s", err)
		} else if listener != nil {
			host := listener.Addr().String()
			listenerURI = &url.URL{
				Scheme: "onion",
				Host:   host,
			}
		}
	}
	l := &onionListener{
		uri:      listenerURI,
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

func (f *onionListenerFactory) Valid(_ config.Configuration) error {
	if f.invalidated != nil {
		return f.invalidated
	}
	return nil
}
