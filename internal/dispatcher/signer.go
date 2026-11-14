package dispatcher

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// ComputeSignature generates an HMAC-SHA256 signature header value formatted as:
// "sha256=<hex_digest>" over "<timestamp>.<raw_payload>".
func ComputeSignature(secret string, timestamp int64, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(fmt.Sprintf("%d.", timestamp)))
	mac.Write(payload)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// VerifySignature performs a constant-time comparison of an X-Signature-256 header
// against the expected HMAC-SHA256 digest.
func VerifySignature(secret string, timestamp int64, payload []byte, signatureHeader string) bool {
	if !strings.HasPrefix(signatureHeader, "sha256=") {
		return false
	}
	expected := ComputeSignature(secret, timestamp, payload)
	return hmac.Equal([]byte(expected), []byte(signatureHeader))
}
