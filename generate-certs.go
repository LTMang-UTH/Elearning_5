package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"
)

func main() {
	fmt.Println("🔐 Đang tạo chứng chỉ SSL/TLS...")
	fmt.Println("=====================================")

	// Tạo thư mục
	dirs := []string{"certs/ca", "certs/server", "certs/client"}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Printf("❌ Không thể tạo thư mục %s: %v\n", dir, err)
			return
		}
	}

	// 1. Tạo CA
	fmt.Println("\n📋 Bước 1: Tạo Certificate Authority (CA)...")
	caKey, caCert, err := generateCA()
	if err != nil {
		fmt.Printf("❌ Không thể tạo CA: %v\n", err)
		return
	}

	if err := saveCertificate("certs/ca/ca-cert.pem", "certs/ca/ca-key.pem", caCert, caKey); err != nil {
		fmt.Printf("❌ Không thể lưu CA: %v\n", err)
		return
	}
	fmt.Println("   ✅ Đã tạo CA certificate")

	// 2. Tạo Server Certificate
	fmt.Println("\n📋 Bước 2: Tạo Server Certificate...")
	serverKey, serverCert, err := generateServerCert(caCert, caKey)
	if err != nil {
		fmt.Printf("❌ Không thể tạo server certificate: %v\n", err)
		return
	}

	if err := saveCertificate("certs/server/server-cert.pem", "certs/server/server-key.pem", serverCert, serverKey); err != nil {
		fmt.Printf("❌ Không thể lưu server certificate: %v\n", err)
		return
	}
	fmt.Println("   ✅ Đã tạo server certificate")

	// 3. Tạo Client Certificate
	fmt.Println("\n📋 Bước 3: Tạo Client Certificate...")
	clientKey, clientCert, err := generateClientCert(caCert, caKey)
	if err != nil {
		fmt.Printf("❌ Không thể tạo client certificate: %v\n", err)
		return
	}

	if err := saveCertificate("certs/client/client-cert.pem", "certs/client/client-key.pem", clientCert, clientKey); err != nil {
		fmt.Printf("❌ Không thể lưu client certificate: %v\n", err)
		return
	}
	fmt.Println("   ✅ Đã tạo client certificate")

	fmt.Println("\n=====================================")
	fmt.Println("✅ Đã tạo thành công tất cả các chứng chỉ!")
	fmt.Println("\nVị trí lưu chứng chỉ:")
	fmt.Println("  CA:     certs/ca/")
	fmt.Println("  Server: certs/server/")
	fmt.Println("  Client: certs/client/")
	fmt.Println("\n⚠️  Đây là chứng chỉ self-signed chỉ dùng cho phát triển!")
	fmt.Println("\nCác bước tiếp theo:")
	fmt.Println("  1. cd websocket\\server && go run main.go")
	fmt.Println("  2. cd websocket\\client && go run main.go")
	fmt.Println("  3. cd websocket\\load-test && go run main.go")
}

func generateCA() (*rsa.PrivateKey, *x509.Certificate, error) {
	// Generate private key
	key, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, err
	}

	// Create certificate template
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Country:            []string{"VN"},
			Province:           []string{"HaNoi"},
			Locality:           []string{"HaNoi"},
			Organization:       []string{"Elearning5"},
			OrganizationalUnit: []string{"Education"},
			CommonName:         "Elearning5 Root CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0), // 10 years
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            2,
	}

	// Self-sign certificate
	certBytes, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}

	cert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		return nil, nil, err
	}

	return key, cert, nil
}

func generateServerCert(caCert *x509.Certificate, caKey *rsa.PrivateKey) (*rsa.PrivateKey, *x509.Certificate, error) {
	// Generate private key
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}

	// Create certificate template
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Country:            []string{"VN"},
			Province:           []string{"HaNoi"},
			Locality:           []string{"HaNoi"},
			Organization:       []string{"Elearning5"},
			OrganizationalUnit: []string{"Server"},
			CommonName:         "localhost",
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().AddDate(2, 0, 0), // 2 years
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:    []string{"localhost", "*.local"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	// Sign with CA
	certBytes, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}

	cert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		return nil, nil, err
	}

	return key, cert, nil
}

func generateClientCert(caCert *x509.Certificate, caKey *rsa.PrivateKey) (*rsa.PrivateKey, *x509.Certificate, error) {
	// Generate private key
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}

	// Create certificate template
	template := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			Country:            []string{"VN"},
			Province:           []string{"HaNoi"},
			Locality:           []string{"HaNoi"},
			Organization:       []string{"Elearning5"},
			OrganizationalUnit: []string{"Client"},
			CommonName:         "client1",
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().AddDate(1, 0, 0), // 1 year
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	// Sign with CA
	certBytes, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}

	cert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		return nil, nil, err
	}

	return key, cert, nil
}

func saveCertificate(certPath, keyPath string, cert *x509.Certificate, key *rsa.PrivateKey) error {
	// Save certificate
	certOut, err := os.Create(certPath)
	if err != nil {
		return err
	}
	defer certOut.Close()

	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}); err != nil {
		return err
	}

	// Save private key
	keyOut, err := os.Create(keyPath)
	if err != nil {
		return err
	}
	defer keyOut.Close()

	keyBytes := x509.MarshalPKCS1PrivateKey(key)
	if err := pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: keyBytes}); err != nil {
		return err
	}

	return nil
}
