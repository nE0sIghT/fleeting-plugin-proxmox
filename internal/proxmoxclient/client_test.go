package proxmoxclient

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClientAcceptsCertificateForConfiguredNode(t *testing.T) {
	cert, caFile := newTestServerCertificate(t, "pve01.example.test")
	server := newTestTLSServer(t, cert)
	defer server.Close()

	client, err := New(Config{
		BaseURL:            server.URL,
		TokenID:            "user@pam!token",
		TokenSecret:        "secret",
		TLSCAFile:          caFile,
		AllowedServerNames: []string{"pve01.example.test"},
	})
	require.NoError(t, err)

	version, err := client.GetVersion(context.Background())
	require.NoError(t, err)
	require.Equal(t, "8.2", version.Release)
}

func TestClientStillRequiresTrustedCertificateForConfiguredNode(t *testing.T) {
	cert, _ := newTestServerCertificate(t, "pve01.example.test")
	server := newTestTLSServer(t, cert)
	defer server.Close()

	client, err := New(Config{
		BaseURL:            server.URL,
		TokenID:            "user@pam!token",
		TokenSecret:        "secret",
		AllowedServerNames: []string{"pve01.example.test"},
	})
	require.NoError(t, err)

	_, err = client.GetVersion(context.Background())
	require.ErrorContains(t, err, "certificate signed by unknown authority")
}

func TestClientRejectsCertificateOutsideAPIHostAndConfiguredNodes(t *testing.T) {
	cert, caFile := newTestServerCertificate(t, "pve01.example.test")
	server := newTestTLSServer(t, cert)
	defer server.Close()

	client, err := New(Config{
		BaseURL:            server.URL,
		TokenID:            "user@pam!token",
		TokenSecret:        "secret",
		TLSCAFile:          caFile,
		AllowedServerNames: []string{"pve02.example.test"},
	})
	require.NoError(t, err)

	_, err = client.GetVersion(context.Background())
	require.ErrorContains(t, err, "certificate is not valid for api_url host or configured nodes")
}

func newTestTLSServer(t *testing.T, cert tls.Certificate) *httptest.Server {
	t.Helper()

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api2/json/version", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"data":{"release":"8.2"}}`))
		require.NoError(t, err)
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	server.StartTLS()
	return server
}

func newTestServerCertificate(t *testing.T, dnsName string) (tls.Certificate, string) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: dnsName,
		},
		DNSNames:              []string{dnsName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)

	caFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caFile, certPEM, 0o600))

	return cert, caFile
}
