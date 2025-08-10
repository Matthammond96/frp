// Copyright 2018 fatedier, fatedier@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package group

import (
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	gerr "github.com/fatedier/golib/errors"

	"github.com/fatedier/frp/server/ports"
)

// TCPGroupCtl manage all TCPGroups
type TCPGroupCtl struct {
	groups map[string]*TCPGroup

	// portManager is used to manage port
	portManager *ports.Manager
	mu          sync.Mutex
}

// NewTCPGroupCtl return a new TcpGroupCtl
func NewTCPGroupCtl(portManager *ports.Manager) *TCPGroupCtl {
	return &TCPGroupCtl{
		groups:      make(map[string]*TCPGroup),
		portManager: portManager,
	}
}

// Listen is the wrapper for TCPGroup's Listen
// If there are no group, we will create one here
func (tgc *TCPGroupCtl) Listen(proxyName string, group string, groupKey string,
	addr string, port int,
) (l net.Listener, realPort int, err error) {
	tgc.mu.Lock()
	tcpGroup, ok := tgc.groups[group]
	if !ok {
		tcpGroup = NewTCPGroup(tgc)
		tgc.groups[group] = tcpGroup
	}
	tgc.mu.Unlock()

	return tcpGroup.Listen(proxyName, group, groupKey, addr, port)
}

// RemoveGroup remove TCPGroup from controller
func (tgc *TCPGroupCtl) RemoveGroup(group string) {
	tgc.mu.Lock()
	defer tgc.mu.Unlock()
	delete(tgc.groups, group)
}

// TCPGroup route connections to different proxies
type TCPGroup struct {
	group    string
	groupKey string
	addr     string
	port     int
	realPort int

	// Deprecated: acceptCh is kept for backward compatibility but no longer used for
	// distributing new connections. Each listener now owns an internal channel
	// and we pick one via round-robin for fair load balancing.
	acceptCh chan net.Conn
	tcpLn    net.Listener
	lns      []*TCPGroupListener
	ctl      *TCPGroupCtl
	mu       sync.Mutex
	// random selection among active listeners (seed randomized at first listener creation)
}

// NewTCPGroup return a new TCPGroup
func NewTCPGroup(ctl *TCPGroupCtl) *TCPGroup {
	return &TCPGroup{
		lns:      make([]*TCPGroupListener, 0),
		ctl:      ctl,
		acceptCh: make(chan net.Conn),
	}
}

// Listen will return a new TCPGroupListener
// if TCPGroup already has a listener, just add a new TCPGroupListener to the queues
// otherwise, listen on the real address
func (tg *TCPGroup) Listen(proxyName string, group string, groupKey string, addr string, port int) (ln *TCPGroupListener, realPort int, err error) {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	if len(tg.lns) == 0 {
		// the first listener, listen on the real address
		realPort, err = tg.ctl.portManager.Acquire(proxyName, port)
		if err != nil {
			return
		}
		tcpLn, errRet := net.Listen("tcp", net.JoinHostPort(addr, strconv.Itoa(port)))
		if errRet != nil {
			err = errRet
			return
		}
	ln = newTCPGroupListener(group, tg, tcpLn.Addr())

		tg.group = group
		tg.groupKey = groupKey
		tg.addr = addr
		tg.port = port
		tg.realPort = realPort
		tg.tcpLn = tcpLn
		tg.lns = append(tg.lns, ln)
		if tg.acceptCh == nil {
			tg.acceptCh = make(chan net.Conn)
		}
	// seed random for selection distribution
	rand.Seed(time.Now().UnixNano())
		go tg.worker()
	} else {
		// address and port in the same group must be equal
		if tg.group != group || tg.addr != addr {
			err = ErrGroupParamsInvalid
			return
		}
		if tg.port != port {
			err = ErrGroupDifferentPort
			return
		}
		if tg.groupKey != groupKey {
			err = ErrGroupAuthFailed
			return
		}
		ln = newTCPGroupListener(group, tg, tg.lns[0].Addr())
		realPort = tg.realPort
		tg.lns = append(tg.lns, ln)
	}
	return
}

// worker is called when the real tcp listener has been created
func (tg *TCPGroup) worker() {
	for {
		c, err := tg.tcpLn.Accept()
		if err != nil {
			return
		}
	// choose a listener randomly
		tg.mu.Lock()
		lCount := len(tg.lns)
		if lCount == 0 {
			tg.mu.Unlock()
			c.Close()
			return
		}
	idx := rand.Intn(lCount)
		target := tg.lns[idx]
		tg.mu.Unlock()

		// deliver connection to target listener's channel; block if full
		// (channel is buffered so short bursts are queued fairly)
		_ = gerr.PanicToError(func() {
			target.acceptCh <- c
		})
	}
}

// Accept is deprecated and only kept so existing code paths using the group directly (if any) still receive connections.
// New load balancing occurs in worker() distributing to per-listener channels.
func (tg *TCPGroup) Accept() <-chan net.Conn { return tg.acceptCh }

// CloseListener remove the TCPGroupListener from the TCPGroup
func (tg *TCPGroup) CloseListener(ln *TCPGroupListener) {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	for i, tmpLn := range tg.lns {
		if tmpLn == ln {
			tg.lns = append(tg.lns[:i], tg.lns[i+1:]...)
			break
		}
	}
	if len(tg.lns) == 0 {
		close(tg.acceptCh)
		tg.tcpLn.Close()
		tg.ctl.portManager.Release(tg.realPort)
		tg.ctl.RemoveGroup(tg.group)
	}
}

// TCPGroupListener
type TCPGroupListener struct {
	groupName string
	group     *TCPGroup

	addr    net.Addr
	closeCh chan struct{}
	acceptCh chan net.Conn
}

func newTCPGroupListener(name string, group *TCPGroup, addr net.Addr) *TCPGroupListener {
	return &TCPGroupListener{
		groupName: name,
		group:     group,
		addr:      addr,
		closeCh:   make(chan struct{}),
		// buffer a few connections to improve fairness when one proxy is busy establishing work conns
		acceptCh:  make(chan net.Conn, 64),
	}
}

// Accept will accept connections from TCPGroup
func (ln *TCPGroupListener) Accept() (c net.Conn, err error) {
	select {
	case <-ln.closeCh:
		return nil, ErrListenerClosed
	case c, ok := <-ln.acceptCh:
		if !ok {
			return nil, ErrListenerClosed
		}
		return c, nil
	}
}

func (ln *TCPGroupListener) Addr() net.Addr {
	return ln.addr
}

// Close close the listener
func (ln *TCPGroupListener) Close() (err error) {
	close(ln.closeCh)

	// close accept channel so waiting Accept() returns
	close(ln.acceptCh)

	// remove self from TcpGroup
	ln.group.CloseListener(ln)
	return
}
