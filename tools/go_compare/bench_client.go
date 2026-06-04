package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
	var gather bool
	var idempotencyKey bool
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
	flag.BoolVar(&gather, "gather", false, "run requests in fixed-size goroutine batches and gather results")
	flag.BoolVar(&idempotencyKey, "idempotency-key", false, "send an Idempotency-Key header")
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
			if idempotencyKey {
				req.Header.Set("Idempotency-Key", "httpclient-bench")
			}
			resp, err := client.Do(req)
			if err != nil {
				continue
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}

	cpuBefore := readProcessCPU()
	start := time.Now()

	var ok int64
	var fail int64
	var h1 int64
	var h2 int64
	latencies := make([]uint64, requests)

	if gather {
		for base := 0; base < requests; base += concurrency {
			n := concurrency
			if remain := requests - base; remain < n {
				n = remain
			}
			results := make([]requestResult, n)
			var wg sync.WaitGroup
			wg.Add(n)
			for i := 0; i < n; i++ {
				go func(slot int) {
					defer wg.Done()
				results[slot] = doRequest(client, makeTarget(url, urlAlt, mixed, mixedShuffle, base+slot), bodyBytes, idempotencyKey)
				}(i)
			}
			wg.Wait()
			for i := range results {
				addResult(&ok, &fail, &h1, &h2, results[i])
				latencies[base+i] = results[i].latencyUs
			}
		}
	} else {
		var wg sync.WaitGroup
		sem := make(chan struct{}, concurrency)

		for i := 0; i < requests; i++ {
			wg.Add(1)
			sem <- struct{}{}
			go func(id int) {
				defer wg.Done()
				defer func() { <-sem }()
				result := doRequest(client, makeTarget(url, urlAlt, mixed, mixedShuffle, id), bodyBytes, idempotencyKey)
				latencies[id] = result.latencyUs
				addResultAtomic(&ok, &fail, &h1, &h2, result)
			}(i)
		}

		wg.Wait()
	}
	wallMs := time.Since(start).Milliseconds()
	cpuAfter := readProcessCPU()
	latency := summarizeLatencies(latencies)
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	fmt.Printf("requests=%d ok=%d fail=%d h1=%d h2=%d wall_ms=%d p50_us=%d p95_us=%d p99_us=%d cpu_user_ms=%d cpu_system_ms=%d alloc_kb=%d sys_kb=%d heap_alloc_kb=%d stack_inuse_kb=%d num_gc=%d\n",
		requests, ok, fail, h1, h2, wallMs, latency.p50Us, latency.p95Us, latency.p99Us,
		(cpuAfter.userUs-cpuBefore.userUs)/1000,
		(cpuAfter.systemUs-cpuBefore.systemUs)/1000,
		mem.Alloc/1024, mem.Sys/1024, mem.HeapAlloc/1024, mem.StackInuse/1024, mem.NumGC)
}

type requestResult struct {
	status     int
	protoMajor int
	ok         bool
	latencyUs  uint64
}

type processCPU struct {
	userUs   uint64
	systemUs uint64
}

type latencyStats struct {
	p50Us uint64
	p95Us uint64
	p99Us uint64
}

func makeTarget(url string, urlAlt string, mixed bool, mixedShuffle bool, id int) string {
	if mixed && urlAlt != "" && mixedUsesAlt(id, mixedShuffle) {
		return urlAlt
	}
	return url
}

func doRequest(client *http.Client, targetURL string, bodyBytes int, idempotencyKey bool) requestResult {
	start := time.Now()
	method := http.MethodGet
	var body io.Reader
	if bodyBytes > 0 {
		method = http.MethodPost
		body = strings.NewReader(strings.Repeat("x", bodyBytes))
	}
	req, err := http.NewRequest(method, targetURL, body)
	if err != nil {
		return requestResult{latencyUs: uint64(time.Since(start).Microseconds())}
	}
	if idempotencyKey {
		req.Header.Set("Idempotency-Key", "httpclient-bench")
	}
	resp, err := client.Do(req)
	if err != nil {
		return requestResult{latencyUs: uint64(time.Since(start).Microseconds())}
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return requestResult{
		status:     resp.StatusCode,
		protoMajor: resp.ProtoMajor,
		ok:         resp.StatusCode >= 200 && resp.StatusCode < 500,
		latencyUs:  uint64(time.Since(start).Microseconds()),
	}
}

func readProcessCPU() processCPU {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return processCPU{}
	}
	return processCPU{
		userUs:   uint64(usage.Utime.Sec)*1000000 + uint64(usage.Utime.Usec),
		systemUs: uint64(usage.Stime.Sec)*1000000 + uint64(usage.Stime.Usec),
	}
}

func summarizeLatencies(values []uint64) latencyStats {
	if len(values) == 0 {
		return latencyStats{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	percentile := func(p float64) uint64 {
		index := int(p*float64(len(values))+0.999999999) - 1
		if index < 0 {
			index = 0
		}
		if index >= len(values) {
			index = len(values) - 1
		}
		return values[index]
	}
	return latencyStats{
		p50Us: percentile(0.50),
		p95Us: percentile(0.95),
		p99Us: percentile(0.99),
	}
}

func addResult(ok *int64, fail *int64, h1 *int64, h2 *int64, result requestResult) {
	switch result.protoMajor {
	case 1:
		*h1++
	case 2:
		*h2++
	}
	if result.ok {
		*ok++
	} else {
		*fail++
	}
}

func addResultAtomic(ok *int64, fail *int64, h1 *int64, h2 *int64, result requestResult) {
	switch result.protoMajor {
	case 1:
		atomic.AddInt64(h1, 1)
	case 2:
		atomic.AddInt64(h2, 1)
	}
	if result.ok {
		atomic.AddInt64(ok, 1)
	} else {
		atomic.AddInt64(fail, 1)
	}
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
