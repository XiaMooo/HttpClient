package main

import (
	"compress/gzip"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	port := getenv("PORT", "8443")
	delayMs := getenvInt("DELAY_MS", 0)
	responseBytes := getenvInt("RESPONSE_BYTES", 4)
	http1Only := getenv("HTTP1_ONLY", "") == "1"
	certFile := getenv("CERT_FILE", "")
	keyFile := getenv("KEY_FILE", "")

	var cert tls.Certificate
	var err error
	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			log.Fatal("CERT_FILE and KEY_FILE must be set together")
		}
		cert, err = tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		cert, err = generateSelfSignedCert()
		if err != nil {
			log.Fatal(err)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		if requestDelayMs := delayForRequest(r, delayMs); requestDelayMs > 0 {
			time.Sleep(time.Duration(requestDelayMs) * time.Millisecond)
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(fixedBody(responseBytes))
	})
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if requestDelayMs := delayForRequest(r, delayMs); requestDelayMs > 0 {
			time.Sleep(time.Duration(requestDelayMs) * time.Millisecond)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"method":        r.Method,
			"content_type":  r.Header.Get("Content-Type"),
			"accept":        r.Header.Get("Accept"),
			"authorization": r.Header.Get("Authorization"),
			"cookie":        r.Header.Get("Cookie"),
			"query":         r.URL.RawQuery,
			"proxy":         r.Header.Get("X-Proxy-Used"),
			"proxy_auth":    r.Header.Get("Proxy-Authorization"),
			"body":          string(body),
		})
	})
	mux.HandleFunc("/redirect", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/echo", http.StatusFound)
	})
	mux.HandleFunc("/set-cookie", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		value := r.URL.Query().Get("value")
		if name == "" {
			name = "session"
		}
		if value == "" {
			value = "abc"
		}
		http.SetCookie(w, &http.Cookie{Name: name, Value: value, Path: "/"})
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/status/500", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("error"))
	})
	mux.HandleFunc("/gzip", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "text/plain")
		gz := gzip.NewWriter(w)
		_, _ = gz.Write([]byte("compressed response"))
		_ = gz.Close()
	})

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("listening on https://127.0.0.1:%s", port)
	nextProtos := []string{"h2", "http/1.1"}
	if http1Only {
		nextProtos = []string{"http/1.1"}
	}
	ln, err := tls.Listen("tcp", ":"+port, &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   nextProtos,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(srv.Serve(ln))
}

func delayForRequest(r *http.Request, fallback int) int {
	value := r.URL.Query().Get("delay_ms")
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return fallback
	}
	return parsed
}

func fixedBody(n int) []byte {
	if n <= 0 {
		return nil
	}
	body := make([]byte, n)
	for i := range body {
		body[i] = 'x'
	}
	return body
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
