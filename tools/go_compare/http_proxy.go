package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var connectCount atomic.Uint64
var forwardCount atomic.Uint64
var expectedAuth string

func main() {
	port := getenv("PROXY_PORT", "8899")
	expectedAuth = getenv("PROXY_AUTH", "")
	if getenv("SOCKS5", "") == "1" {
		runSocks5(port)
		return
	}
	proxy := &http.Server{
		Addr:              ":" + port,
		Handler:           http.HandlerFunc(handleProxy),
		ReadHeaderTimeout: 5 * time.Second,
	}
	if getenv("HTTPS_PROXY", "") == "1" {
		cert, err := generateSelfSignedCert()
		if err != nil {
			log.Fatal(err)
		}
		ln, err := tls.Listen("tcp", ":"+port, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("https proxy listening on https://127.0.0.1:%s", port)
		log.Fatal(proxy.Serve(ln))
	}
	log.Printf("proxy listening on http://127.0.0.1:%s", port)
	log.Fatal(proxy.ListenAndServe())
}

func runSocks5(port string) {
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("socks5 proxy listening on 127.0.0.1:%s", port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go handleSocks5(conn)
	}
}

func handleSocks5(client net.Conn) {
	defer client.Close()
	header := make([]byte, 2)
	if _, err := io.ReadFull(client, header); err != nil {
		return
	}
	if header[0] != 0x05 {
		return
	}
	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(client, methods); err != nil {
		return
	}
	method := byte(0x00)
	if expectedAuth != "" {
		method = 0x02
	}
	if _, err := client.Write([]byte{0x05, method}); err != nil {
		return
	}
	if method == 0x02 {
		if !handleSocks5Auth(client) {
			return
		}
	}
	req := make([]byte, 4)
	if _, err := io.ReadFull(client, req); err != nil {
		return
	}
	if req[0] != 0x05 || req[1] != 0x01 {
		return
	}
	var host string
	switch req[3] {
	case 0x01:
		addr := make([]byte, 4)
		if _, err := io.ReadFull(client, addr); err != nil {
			return
		}
		host = net.IP(addr).String()
	case 0x03:
		lenBuf := make([]byte, 1)
		if _, err := io.ReadFull(client, lenBuf); err != nil {
			return
		}
		name := make([]byte, int(lenBuf[0]))
		if _, err := io.ReadFull(client, name); err != nil {
			return
		}
		host = string(name)
	case 0x04:
		addr := make([]byte, 16)
		if _, err := io.ReadFull(client, addr); err != nil {
			return
		}
		host = net.IP(addr).String()
	default:
		_, _ = client.Write([]byte{0x05, 0x08, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	portBuf := make([]byte, 2)
	if _, err := io.ReadFull(client, portBuf); err != nil {
		return
	}
	port := binary.BigEndian.Uint16(portBuf)
	target, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(int(port))), 5*time.Second)
	if err != nil {
		_, _ = client.Write([]byte{0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	defer target.Close()
	connectCount.Add(1)
	_, _ = client.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(target, client)
		_ = target.Close()
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(client, target)
		done <- struct{}{}
	}()
	<-done
}

func handleSocks5Auth(conn net.Conn) bool {
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return false
	}
	user := make([]byte, int(header[1]))
	if _, err := io.ReadFull(conn, user); err != nil {
		return false
	}
	passLen := make([]byte, 1)
	if _, err := io.ReadFull(conn, passLen); err != nil {
		return false
	}
	pass := make([]byte, int(passLen[0]))
	if _, err := io.ReadFull(conn, pass); err != nil {
		return false
	}
	if string(user) == "user" && string(pass) == "pass" {
		_, _ = conn.Write([]byte{0x01, 0x00})
		return true
	}
	_, _ = conn.Write([]byte{0x01, 0x01})
	return false
}

func handleProxy(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		handleConnect(w, r)
		return
	}
	if r.URL.Path == "/__proxy_stats" && r.URL.Scheme == "" {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]uint64{
			"connect": connectCount.Load(),
			"forward": forwardCount.Load(),
		})
		return
	}
	handleForward(w, r)
}

func handleConnect(w http.ResponseWriter, r *http.Request) {
	if !authorized(w, r) {
		return
	}
	connectCount.Add(1)
	targetConn, err := net.DialTimeout("tcp", r.Host, 5*time.Second)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		_ = targetConn.Close()
		http.Error(w, "hijacking unsupported", http.StatusInternalServerError)
		return
	}
	clientConn, _, err := hijacker.Hijack()
	if err != nil {
		_ = targetConn.Close()
		return
	}

	_, _ = clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	go tunnel(targetConn, clientConn)
	go tunnel(clientConn, targetConn)
}

func handleForward(w http.ResponseWriter, r *http.Request) {
	if !authorized(w, r) {
		return
	}
	if r.URL.Scheme == "" || r.URL.Host == "" {
		http.Error(w, "proxy request must use absolute-form URI", http.StatusBadRequest)
		return
	}
	forwardCount.Add(1)

	outReq := r.Clone(r.Context())
	outReq.RequestURI = ""
	outReq.Header = r.Header.Clone()
	outReq.Header.Set("X-Proxy-Used", "1")
	removeHopByHop(outReq.Header)

	transport := &http.Transport{
		Proxy:                 nil,
		ResponseHeaderTimeout: 5 * time.Second,
	}
	resp, err := transport.RoundTrip(outReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	removeHopByHop(resp.Header)
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func authorized(w http.ResponseWriter, r *http.Request) bool {
	if expectedAuth == "" {
		return true
	}
	if r.Header.Get("Proxy-Authorization") == expectedAuth {
		return true
	}
	w.Header().Set("Proxy-Authenticate", "Basic realm=\"httpclient-test\"")
	http.Error(w, "proxy authentication required", http.StatusProxyAuthRequired)
	return false
}

func tunnel(dst net.Conn, src net.Conn) {
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
}

func removeHopByHop(header http.Header) {
	for _, name := range []string{
		"Connection",
		"Proxy-Connection",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"Te",
		"Trailer",
		"Transfer-Encoding",
		"Upgrade",
	} {
		header.Del(name)
	}
	for _, value := range header.Values("Connection") {
		for _, part := range strings.Split(value, ",") {
			header.Del(strings.TrimSpace(part))
		}
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func generateSelfSignedCert() (tls.Certificate, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	return tls.X509KeyPair(certPEM, keyPEM)
}
