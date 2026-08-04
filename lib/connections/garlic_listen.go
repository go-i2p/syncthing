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
	"sync"

	"github.com/go-i2p/onramp"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/connections/registry"
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

	uri     *url.URL
	cfg     config.Wrapper
	tlsCfg  *tls.Config
	conns   chan internalConn
	factory listenerFactory
	//registry *registry.Registry

	// laddr net.Addr
	listener net.Listener

	mut sync.RWMutex
}

func (t *garlicListener) serve(ctx context.Context) error {
	return nil
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
	// tls.Config must not verify keys, we have to use self-signed TLS.
	// No nat.Service, NAT traversal is handled by I2P
	// No registry, in the I2P context it will be counterproductive
	l := &garlicListener{
		uri:      fixupPort(uri, config.DefaultTCPPort),
		cfg:      cfg,
		tlsCfg:   tlsCfg,
		conns:    conns,
		factory:  f,
		listener: listener,
	}
	l.ServiceWithError = svcutil.AsService(l.serve, l.String())
	return l
}

func (f *garlicListenerFactory) Valid(_ config.Configuration) error {
	// Garlic must not be nil
	// Garlic must not be closed
	if f.invalidated != nil {
		return f.invalidated
	}
	return nil
}
