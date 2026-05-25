package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var url string
	var concurrency int
	var requests int
	var insecure bool
	var noProxy bool
	var http1 bool
	var freshConnect bool
	var bodyBytes int
	var warmupPerURL int
	var urlAlt string
	var mixed bool
	var mixedShuffle bool
	flag.StringVar(&url, "url", "http://127.0.0.1:8080/ping", "request url")
	flag.IntVar(&concurrency, "concurrency", 4, "concurrency")
	flag.IntVar(&requests, "requests", 16, "requests")
	flag.BoolVar(&insecure, "insecure", false, "skip TLS certificate verification")
	flag.BoolVar(&noProxy, "no-proxy", false, "disable proxy use")
	flag.BoolVar(&http1, "http1", false, "force HTTP/1.1 by disabling HTTP/2")
	flag.BoolVar(&freshConnect, "fresh-connect", false, "disable keep-alive for every request")
	flag.IntVar(&bodyBytes, "body-bytes", 0, "send this many request body bytes")
	flag.IntVar(&warmupPerURL, "warmup-per-url", 0, "warm each url before timing")
	flag.StringVar(&urlAlt, "url-alt", "", "alternate request url")
	flag.BoolVar(&mixed, "mixed", false, "alternate requests between url and url-alt")
	flag.BoolVar(&mixedShuffle, "mixed-shuffle", false, "use a stable shuffled mixed request order")
	flag.Parse()

	transport := &http.Transport{
		MaxIdleConns:        128,
		MaxIdleConnsPerHost: 128,
		MaxConnsPerHost:     concurrency,
		ForceAttemptHTTP2:   true,
	}
	if http1 {
		transport.ForceAttemptHTTP2 = false
		transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	}
	if insecure {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	if noProxy {
		transport.Proxy = nil
	}
	if freshConnect {
		transport.DisableKeepAlives = true
		transport.MaxIdleConnsPerHost = 0
	}

	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: transport,
	}

	urls := []string{url}
	if mixed && urlAlt != "" {
		urls = append(urls, urlAlt)
	}
	for round := 0; round < warmupPerURL; round++ {
		for _, targetURL := range urls {
			method := http.MethodGet
			var body io.Reader
			if bodyBytes > 0 {
				method = http.MethodPost
				body = strings.NewReader(strings.Repeat("x", bodyBytes))
			}
			req, err := http.NewRequest(method, targetURL, body)
			if err != nil {
				continue
			}
			resp, err := client.Do(req)
			if err != nil {
				continue
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}

	start := time.Now()

	var ok int64
	var fail int64
	var h1 int64
	var h2 int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)

	for i := 0; i < requests; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(id int) {
			defer wg.Done()
			defer func() { <-sem }()

			method := http.MethodGet
			var body io.Reader
			if bodyBytes > 0 {
				method = http.MethodPost
				body = strings.NewReader(strings.Repeat("x", bodyBytes))
			}
			targetURL := url
			if mixed && urlAlt != "" {
				if mixedUsesAlt(id, mixedShuffle) {
					targetURL = urlAlt
				}
			}
			req, err := http.NewRequest(method, targetURL, body)
			if err != nil {
				atomic.AddInt64(&fail, 1)
				return
			}
			resp, err := client.Do(req)
			if err != nil {
				atomic.AddInt64(&fail, 1)
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			switch {
			case resp.ProtoMajor == 1:
				atomic.AddInt64(&h1, 1)
			case resp.ProtoMajor == 2:
				atomic.AddInt64(&h2, 1)
			}
			if resp.StatusCode >= 200 && resp.StatusCode < 500 {
				atomic.AddInt64(&ok, 1)
			} else {
				atomic.AddInt64(&fail, 1)
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("requests=%d ok=%d fail=%d h1=%d h2=%d wall_ms=%d\n", requests, ok, fail, h1, h2, time.Since(start).Milliseconds())
}

func mixedUsesAlt(id int, shuffle bool) bool {
	if !shuffle {
		return id%2 == 1
	}
	x := uint64(id) + 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x = x ^ (x >> 31)
	return x&1 != 0
}
