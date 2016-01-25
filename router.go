// Copyright 2015 tsuru authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"planb/vendor/github.com/hashicorp/golang-lru"
)

var (
	noRouteData       = []byte("no such route")
	emptyBufferReader = ioutil.NopCloser(&bytes.Buffer{})
)

type requestData struct {
	backendLen int
	backend    string
	backendIdx int
	backendKey string
	host       string
	debug      bool
	startTime  time.Time
}

func (r *requestData) String() string {
	back := r.backend
	if back == "" {
		back = "?"
	}
	return r.host + " -> " + back
}

type Router struct {
	http.Transport
	ReadRedisHost   string
	ReadRedisPort   int
	WriteRedisHost  string
	WriteRedisPort  int
	LogPath         string
	DialTimeout     time.Duration
	RequestTimeout  time.Duration
	DeadBackendTTL  int
	FlushInterval   time.Duration
	rp              *httputil.ReverseProxy
	dialer          *net.Dialer
	readRedisPool   *redis.Client
	writeRedisPool  *redis.Client
	logger          *Logger
	ctxMutex        sync.Mutex
	reqCtx          map[*http.Request]*requestData
	rrMutex         sync.RWMutex
	roundRobin      map[string]*uint64
	cache           *lru.Cache
	appDomainsCache *lru.Cache // domains we are configured to serve (avoids hammering redis with non-existing apps)
}

func redisDialer(host string, port int) func() (redis.Conn, error) {
	readTimeout := time.Second
	writeTimeout := time.Second
	dialTimeout := time.Second
	if host == "" {
		host = "127.0.0.1"
	}
	if port == 0 {
		port = 6379
	}
	redisAddr := fmt.Sprintf("%s:%d", host, port)
	return func() (redis.Conn, error) {
		return redis.DialTimeout("tcp", redisAddr, dialTimeout, readTimeout, writeTimeout)
	}
}

func (router *Router) Init() error {
	var err error
	if router.LogPath == "" {
		router.LogPath = "./access.log"
	}

	rr := fmt.Sprintf("%s:%s", router.ReadRedisRouter, router.ReadRedisPort)
	rw := fmt.Sprintf("%s:%s", router.WriteRedisRouter, router.WriteRedisPort)
	router.readRedisPool = redis.New(rr)
	router.writeRedisPool = redis.New(rw)

	if router.logger == nil {
		router.logger, err = NewFileLogger(router.LogPath)
		if err != nil {
			return err
		}
	}
	if router.DeadBackendTTL == 0 {
		router.DeadBackendTTL = 30
	}
	if router.cache == nil {
		router.cache, err = lru.New(100)
		if err != nil {
			return err
		}
	}

	if router.appDomainsCache == nil {
		router.appDomainsCache, err = lru.New(1024) // we can serve up to 1024 apps
		if err != nil {
			return err
		}
	}

	go func() {
		// this is a watchdog coroutine that scans redis for a list of frontend:<domains>
		// and builds a cache to be queried near the point the http requests gets in.
		// this way we can 404 the request w/o spending much processing or roundtrip to redis.
		// the vanilla planb will hog both redis and its process in case you fall for some bot
		// thinking it has a proxy signature.
		for {
			for {
				cursor, keys, err := rc.Scan(c, "match", "frontend:*")
				if err != nil {
					logError("Error Scanning redis for frontend", err)
					time.Sleep(1000 * time.Millisecond)
					continue
				}

				for _, k := range keys {
					router.appDomainsCache.Add(k)
				}

				if cursor == "0" {
					break
				}

				c = cursor // update cursor
			}
			logInfo("Frontend watchdog: %d frontends", router.appDomainsCache.Len())
			time.Sleep(500 * time.Millisecond)
		}
	}()

	router.reqCtx = make(map[*http.Request]*requestData)
	router.dialer = &net.Dialer{
		Timeout:   router.DialTimeout,
		KeepAlive: 30 * time.Second,
	}
	router.Transport = http.Transport{
		Dial:                router.dialer.Dial,
		TLSHandshakeTimeout: router.DialTimeout,
		MaxIdleConnsPerHost: 100,
	}
	router.roundRobin = make(map[string]*uint64)
	router.rp = &httputil.ReverseProxy{
		Director:      router.Director,
		Transport:     router,
		FlushInterval: router.FlushInterval,
	}
	return nil
}

func (router *Router) Stop() {
	router.logger.Stop()
}

type backendSet struct {
	id       string
	backends []string
	dead     map[uint64]struct{}
	expires  time.Time
}

func (s *backendSet) Expired() bool {
	return time.Now().After(s.expires)
}

func (router *Router) getBackends(host string) (*backendSet, error) {
	if data, ok := router.cache.Get(host); ok {
		set := data.(backendSet)
		if !set.Expired() {
			return &set, nil
		}
	}
	var set backendSet

	backends, err := router.readRedisPool.LRange("frontend:"+host, 0, -1)
	if err != nil {
		return nil, fmt.Errorf("Redis error fetching frontend %s list : %s %s", host, err)
	}

	deadMembers, err := router.readRedisPool.SMembers("dead:" + host)
	if err != nil {
		return nil, fmt.Errorf("Redis error fetching dead frontend %s set : %s %s", host, err)
	}

	if len(backends) < 2 {
		return nil, errors.New("no backends available")
	}

	set.id = backends[0]
	backends = backends[1:]
	set.backends = make([]string, len(backends))
	for i, backend := range backends {
		set.backends[i] = backend
	}

	deadMap := map[uint64]struct{}{}
	for _, dead := range deadMembers {
		deadIdx, _ := strconv.ParseUint(dead, 10, 64)
		deadMap[deadIdx] = struct{}{}
	}
	set.dead = deadMap
	set.expires = time.Now().Add(2 * time.Second)
	router.cache.Add(host, set)
	return &set, nil
}

func (router *Router) getRequestData(req *http.Request, save bool) (*requestData, error) {
	host, _, _ := net.SplitHostPort(req.Host)

	// check if we answer for this app (host)
	if !router.appDomainsCache.Contains(host) {
		return nil, fmt.Errorf("No application %s configured", host)
	}

	if host == "" {
		host = req.Host
	}
	reqData := &requestData{
		debug:     req.Header.Get("X-Debug-Router") != "",
		startTime: time.Now(),
		host:      host,
	}
	req.Header.Del("X-Debug-Router")
	if save {
		router.ctxMutex.Lock()
		router.reqCtx[req] = reqData
		router.ctxMutex.Unlock()
	}
	set, err := router.getBackends(host)
	if err != nil {
		return reqData, err
	}
	reqData.backendKey = set.id
	reqData.backendLen = len(set.backends)
	router.rrMutex.RLock()
	roundRobin := router.roundRobin[host]
	if roundRobin == nil {
		router.rrMutex.RUnlock()
		router.rrMutex.Lock()
		roundRobin = router.roundRobin[host]
		if roundRobin == nil {
			roundRobin = new(uint64)
			router.roundRobin[host] = roundRobin
		}
		router.rrMutex.Unlock()
	} else {
		router.rrMutex.RUnlock()
	}
	// We always add, it will eventually overflow to zero which is fine.
	initialNumber := atomic.AddUint64(roundRobin, 1)
	initialNumber = (initialNumber - 1) % uint64(reqData.backendLen)
	toUseNumber := -1
	for chosenNumber := initialNumber; ; {
		_, isDead := set.dead[chosenNumber]
		if !isDead {
			toUseNumber = int(chosenNumber)
			break
		}
		chosenNumber = (chosenNumber + 1) % uint64(reqData.backendLen)
		if chosenNumber == initialNumber {
			break
		}
	}
	if toUseNumber == -1 {
		return reqData, errors.New("all backends are dead")
	}
	reqData.backendIdx = toUseNumber
	reqData.backend = set.backends[toUseNumber]
	return reqData, nil
}

func (router *Router) Director(req *http.Request) {
	reqData, err := router.getRequestData(req, true)
	if err != nil {
		logError(reqData.String(), req.URL.Path, err)
		return
	}
	url, err := url.Parse(reqData.backend)
	if err != nil {
		logError(reqData.String(), req.URL.Path, fmt.Errorf("invalid backend url: %s", err))
		return
	}

	req.URL.Scheme = url.Scheme
	req.URL.Host = url.Host
}

func (router *Router) RoundTrip(req *http.Request) (*http.Response, error) {
	router.ctxMutex.Lock()
	reqData := router.reqCtx[req]
	delete(router.reqCtx, req)
	router.ctxMutex.Unlock()
	var rsp *http.Response
	var err error
	var backendDuration time.Duration
	var timedout int32
	if router.RequestTimeout > 0 {
		timer := time.AfterFunc(router.RequestTimeout, func() {
			router.Transport.CancelRequest(req)
			atomic.AddInt32(&timedout, 1)
		})
		defer timer.Stop()
	}
	if req.URL.Scheme == "" || req.URL.Host == "" {
		closerBuffer := ioutil.NopCloser(bytes.NewBuffer(noRouteData))
		rsp = &http.Response{
			Request:       req,
			StatusCode:    http.StatusBadRequest,
			ProtoMajor:    req.ProtoMajor,
			ProtoMinor:    req.ProtoMinor,
			ContentLength: int64(len(noRouteData)),
			Body:          closerBuffer,
		}
	} else {
		t0 := time.Now().UTC()
		rsp, err = router.Transport.RoundTrip(req)
		backendDuration = time.Since(t0)
		if err != nil {
			markAsDead := false
			if netErr, ok := err.(net.Error); ok {
				markAsDead = !netErr.Temporary()
			}
			isTimeout := atomic.LoadInt32(&timedout) == int32(1)
			if isTimeout {
				markAsDead = false
				err = fmt.Errorf("request timed out after %v: %s", router.RequestTimeout, err)
			} else {
				err = fmt.Errorf("error in backend request: %s", err)
			}
			if markAsDead {
				err = fmt.Errorf("%s *DEAD*", err)
			}
			logError(reqData.String(), req.URL.Path, err)
			if markAsDead {
				err := router.writeRedisPool.SAdd("dead:"+reqData.host, reqData.backendIdx)
				if redisErr != nil {
					logError(reqData.String(), req.URL.Path, fmt.Errorf("Redis SADD: error marking dead backend: %s", redisErr))
				}

				err := router.writeRedisPool.Expire("dead:"+reqData.host, router.DeadBackendTTL)
				if redisErr != nil {
					logError(reqData.String(), req.URL.Path, fmt.Errorf("Redis EXPIRE: error marking dead backend: %s", redisErr))
				}

				err := router.writeRedisPool.Publish("dead", fmt.Sprintf("%s;%s;%d;%d", reqData.host, reqData.backend, reqData.backendIdx, reqData.backendLen))
				if redisErr != nil {
					logError(reqData.String(), req.URL.Path, fmt.Errorf("Redis PUBLISH: error markind dead backend: %s", redisErr))
				}

			}

			rsp = &http.Response{
				Request:    req,
				StatusCode: http.StatusServiceUnavailable,
				ProtoMajor: req.ProtoMajor,
				ProtoMinor: req.ProtoMinor,
				Header:     http.Header{},
				Body:       emptyBufferReader,
			}
		}
	}
	if reqData.debug {
		rsp.Header.Set("X-Debug-Backend-Url", reqData.backend)
		rsp.Header.Set("X-Debug-Backend-Id", strconv.FormatUint(uint64(reqData.backendIdx), 10))
		rsp.Header.Set("X-Debug-Frontend-Key", reqData.host)
	}
	router.logger.MessageRaw(&logEntry{
		now:             time.Now(),
		req:             req,
		rsp:             rsp,
		backendDuration: backendDuration,
		totalDuration:   time.Since(reqData.startTime),
		backendKey:      reqData.backendKey,
	})
	return rsp, nil
}

func (router *Router) serveWebsocket(rw http.ResponseWriter, req *http.Request) (*requestData, error) {
	reqData, err := router.getRequestData(req, false)
	if err != nil {
		return reqData, err
	}
	url, err := url.Parse(reqData.backend)
	if err != nil {
		return reqData, err
	}
	req.Host = url.Host
	dstConn, err := router.dialer.Dial("tcp", url.Host)
	if err != nil {
		return reqData, err
	}
	defer dstConn.Close()
	hj, ok := rw.(http.Hijacker)
	if !ok {
		return reqData, errors.New("not a hijacker")
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		return reqData, err
	}
	defer conn.Close()
	if clientIP, _, err := net.SplitHostPort(req.RemoteAddr); err == nil {
		if prior, ok := req.Header["X-Forwarded-For"]; ok {
			clientIP = strings.Join(prior, ", ") + ", " + clientIP
		}
		req.Header.Set("X-Forwarded-For", clientIP)
	}
	err = req.Write(dstConn)
	if err != nil {
		return reqData, err
	}
	errc := make(chan error, 2)
	cp := func(dst io.Writer, src io.Reader) {
		_, err := io.Copy(dst, src)
		errc <- err
	}
	go cp(dstConn, conn)
	go cp(conn, dstConn)
	<-errc
	return reqData, nil
}

func (router *Router) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if req.Host == "__ping__" && req.URL.Path == "/" {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("OK"))
		return
	}
	upgrade := req.Header.Get("Upgrade")
	if upgrade != "" && strings.ToLower(upgrade) == "websocket" {
		reqData, err := router.serveWebsocket(rw, req)
		if err != nil {
			logError(reqData.String(), req.URL.Path, err)
			http.Error(rw, "", http.StatusBadGateway)
		}
		return
	}
	router.rp.ServeHTTP(rw, req)
}
