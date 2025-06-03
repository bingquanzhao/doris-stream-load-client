package load

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/apache/doris-stream-load-client/pkg/log"
	"io"
	"net/http"
)

// HttpPutBuilder helps construct HTTP PUT requests for Doris stream load
type HttpPutBuilder struct {
	url     string
	headers map[string]string
	body    io.Reader
}

// NewHttpPutBuilder creates a new HttpPutBuilder instance
func NewHttpPutBuilder() *HttpPutBuilder {
	return &HttpPutBuilder{
		headers: make(map[string]string),
	}
}

// SetUrl sets the URL for the request
func (h *HttpPutBuilder) SetUrl(url string) *HttpPutBuilder {
	h.url = url
	return h
}

// AddCommonHeader adds common headers required for stream load
func (h *HttpPutBuilder) AddCommonHeader() *HttpPutBuilder {
	h.headers["Expect"] = "100-continue"
	return h
}

// AddFileName adds a fileName header
func (h *HttpPutBuilder) AddFileName(fileName string) *HttpPutBuilder {
	h.headers["fileName"] = fileName
	return h
}

// Enable2PC enables two-phase commit
func (h *HttpPutBuilder) Enable2PC() *HttpPutBuilder {
	h.headers["two_phase_commit"] = "true"
	return h
}

// BaseAuth adds basic authentication header
func (h *HttpPutBuilder) BaseAuth(user, password string) *HttpPutBuilder {
	authInfo := fmt.Sprintf("%s:%s", user, password)
	encodedAuth := base64.StdEncoding.EncodeToString([]byte(authInfo))
	h.headers["Authorization"] = "Basic " + encodedAuth
	return h
}

// AddTxnId adds a transaction ID header
func (h *HttpPutBuilder) AddTxnId(txnID int64) *HttpPutBuilder {
	h.headers["txn_id"] = fmt.Sprintf("%d", txnID)
	return h
}

// Commit sets the transaction operation to commit
func (h *HttpPutBuilder) Commit() *HttpPutBuilder {
	h.headers["txn_operation"] = "commit"
	return h
}

// Abort sets the transaction operation to abort
func (h *HttpPutBuilder) Abort() *HttpPutBuilder {
	h.headers["txn_operation"] = "abort"
	return h
}

// SetEntity sets the request body
func (h *HttpPutBuilder) SetEntity(body []byte) *HttpPutBuilder {
	h.body = bytes.NewReader(body)
	return h
}

// SetReader sets an io.Reader as the request body (for streaming large data)
func (h *HttpPutBuilder) SetReader(reader io.Reader) *HttpPutBuilder {
	h.body = reader
	return h
}

// SetStringEntity sets a string as the request body
func (h *HttpPutBuilder) SetStringEntity(body string) *HttpPutBuilder {
	h.body = bytes.NewReader([]byte(body))
	return h
}

// SetEmptyEntity sets an empty request body
func (h *HttpPutBuilder) SetEmptyEntity() *HttpPutBuilder {
	h.body = bytes.NewReader([]byte(""))
	return h
}

// AddProperties adds multiple headers from a map
func (h *HttpPutBuilder) AddProperties(properties map[string]string) *HttpPutBuilder {
	for key, value := range properties {
		h.headers[key] = value
	}
	return h
}

// SetLabel sets the label header
func (h *HttpPutBuilder) SetLabel(label string) *HttpPutBuilder {
	h.headers["label"] = label
	return h
}

// Build creates an http.Request object from the configured settings
func (h *HttpPutBuilder) Build() (*http.Request, error) {
	if h.url == "" {
		return nil, fmt.Errorf("url cannot be empty")
	}
	if h.body == nil {
		return nil, fmt.Errorf("request body cannot be nil")
	}

	// Create the HTTP PUT request
	req, err := http.NewRequest(http.MethodPut, h.url, h.body)
	if err != nil {
		return nil, err
	}

	// Check if we need to remove the label header when using group commit
	if _, exists := h.headers["group_commit"]; exists {
		log.Warnf("label and group_commit can't be set at the same time, will be automatically removed!")
		delete(h.headers, "label")
	}

	// Add all headers to the request
	for key, value := range h.headers {
		req.Header.Set(key, value)
	}

	return req, nil
}
