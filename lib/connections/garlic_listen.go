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
	var gaddr *i2pkeys.I2PAddr
	var listener net.Listener

	// I2P addresses are cryptographic hashes; we cannot listen on a
	// configured hostname. The factory creates the listener via SAM,
	// which generates its own I2P address. We rely on that listener.
	if t.listener == nil {
		return fmt.Errorf("garlic listener: factory did not create a listener")
	}

	// Use the factory-created listener (SAM generates its own I2P address)
	t.mut.RLock()
	listener = t.listener
	t.mut.RUnlock()
	if listener == nil {
		slog.WarnContext(ctx, "Garlic listener disabled: factory listener is nil")
		return fmt.Errorf("garlic listener: factory listener is nil")
	}
	addr := listener.Addr()
	if addr == nil {
		slog.WarnContext(ctx, "Garlic listener disabled: listener has no address")
		return fmt.Errorf("garlic listener: listener has no address")
	}
	i2pAddr, ok := addr.(*i2pkeys.I2PAddr)
	if !ok {
		slog.WarnContext(ctx, "Garlic listener address is not I2PAddr", slogutil.Address(addr))
		return fmt.Errorf("garlic listener: unexpected address type %T", addr)
	}
	gaddr = i2pAddr
	t.mut.Lock()
	t.uri = &url.URL{
		Scheme: "garlic",
		Host:   gaddr.String(),
	}
	t.mut.Unlock()

	t.notifyAddressesChanged(t)
	defer t.clearAddresses(t)

	t.registry.Register(t.uri.String(), gaddr)
	defer t.registry.Unregister(t.uri.String(), gaddr)

	slog.InfoContext(ctx, "Garlic listener starting", slogutil.Address(gaddr))
	defer slog.InfoContext(ctx, "Garlic listener shutting down", slogutil.Address(gaddr))

	acceptFailures := 0
	const maxAcceptFailures = 10

	for {
		// I2P listener is not a TCPListener; SetDeadline doesn't apply.
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
	t.mut.RLock()
	defer t.mut.RUnlock()
	if t.listener != nil {
		if addr := t.listener.Addr(); addr != nil {
			if i2pAddr, ok := addr.(*i2pkeys.I2PAddr); ok {
				return &url.URL{
					Scheme: "garlic",
					Host:   i2pAddr.String(),
				}
			}
		}
	}
	if t.uri != nil {
		return t.uri
	}
	return nil
}

func (t *garlicListener) WANAddresses() []*url.URL {
	if t.URI() != nil {
		return []*url.URL{t.URI()}
	}
	return nil
}

func (t *garlicListener) LANAddresses() []*url.URL {
	return nil // I2P has no LAN concept
}

func (t *garlicListener) String() string {
	if t.uri == nil {
		return "garlic://disabled"
	}
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
			return &garlicListener{
				uri:      nil,
				cfg:      cfg,
				tlsCfg:   tlsCfg,
				conns:    conns,
				factory:  f,
				listener: nil,
				registry: registry,
				ServiceWithError: svcutil.AsService(func(ctx context.Context) error {
					slog.WarnContext(ctx, "Garlic listener disabled: SAM unavailable")
					<-ctx.Done()
					return ctx.Err()
				}, "garlic-disabled"),
			}
		}
	}
	var listener net.Listener
	var listenerURI *url.URL
	if f.invalidated == nil {
		var err error
		listener, err = f.Garlic.Listen()
		if err != nil {
			f.invalidated = err
			l.Debugf("SAMv3 Listener setup failed, cannot listen on I2P: %s", err)
		} else if listener != nil {
			i2pAddr, ok := listener.Addr().(*i2pkeys.I2PAddr)
			if !ok {
				f.invalidated = fmt.Errorf("garlic listener: unexpected address type %T", listener.Addr())
				l.Debugf("SAMv3 Listener setup failed, unexpected address type: %v", f.invalidated)
			} else {
				host := i2pAddr.String()
				listenerURI = &url.URL{
					Scheme: "garlic",
					Host:   host,
				}
			}
		}
	}
	l := &garlicListener{
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
