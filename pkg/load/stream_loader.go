package load

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/apache/doris-stream-load-client/pkg/config"
	"github.com/apache/doris-stream-load-client/pkg/exception"
	"github.com/apache/doris-stream-load-client/pkg/log"
	"github.com/apache/doris-stream-load-client/pkg/util"
	jsoniter "github.com/json-iterator/go"
)

const (
	// LoadURLPattern is the URL pattern for stream load API
	LoadURLPattern = "http://%s/api/%s/%s/_stream_load"
)

// StreamLoader handles loading data into Doris via HTTP stream load
type StreamLoader struct {
	httpClient   *http.Client
	json         jsoniter.API
	loadURL      string
	loadSettings *config.LoadSetting
}

// NewStreamLoader creates a new StreamLoader instance
func NewStreamLoader(loadSettings *config.LoadSetting) *StreamLoader {
	// Construct the load URL
	loadURL := fmt.Sprintf(LoadURLPattern, loadSettings.GetEndpoint(), loadSettings.GetDatabase(), loadSettings.GetTable())

	// Get shared HTTP client
	httpClient := util.GetHttpClient()

	return &StreamLoader{
		httpClient:   httpClient,
		json:         jsoniter.ConfigCompatibleWithStandardLibrary,
		loadURL:      loadURL,
		loadSettings: loadSettings,
	}
}

// Load sends data to Doris via HTTP stream load
func (s *StreamLoader) Load(reader io.Reader) (*LoadResponse, error) {
	// Create request
	req, err := s.createRequest(reader)
	if err != nil {
		log.Errorf("Failed to create HTTP request: %v", err)
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Execute the request
	resp, err := s.httpClient.Do(req)
	if err != nil {
		log.Errorf("Failed to execute HTTP request: %v", err)
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// Handle the response
	return s.handleResponse(resp)
}

// createRequest creates an HTTP request
func (s *StreamLoader) createRequest(body io.Reader) (*http.Request, error) {
	options := s.loadSettings.GetOptions()

	// Create a new HTTP PUT builder for each request to ensure thread safety
	httpPutBuilder := NewHttpPutBuilder()
	httpPutBuilder.SetUrl(s.loadURL)
	httpPutBuilder.BaseAuth(s.loadSettings.GetUser(), s.loadSettings.GetPassword())
	httpPutBuilder.AddCommonHeader()
	httpPutBuilder.SetLabel(s.loadSettings.GetLabel()) // Generate unique label for each request

	// Add headers from the snapshot instead of calling GetOptions() again
	httpPutBuilder.AddProperties(options)

	// Use streaming approach - no need to read all data into memory
	httpPutBuilder.SetReader(body)

	// Build request
	req, err := httpPutBuilder.Build()
	if err != nil {
		return nil, err
	}

	return req, nil
}

// handleResponse processes the HTTP response from a stream load request
func (s *StreamLoader) handleResponse(resp *http.Response) (*LoadResponse, error) {
	statusCode := resp.StatusCode
	log.Debugf("Received HTTP response with status code: %d", statusCode)

	if statusCode == http.StatusOK && resp.Body != nil {
		// Read the response body with limited buffer
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024)) // 1MB limit
		if err != nil {
			log.Errorf("Failed to read response body: %v", err)
			return nil, fmt.Errorf("failed to read response body: %w", err)
		}

		log.Infof("Stream Load Response: %s", string(body))

		// Parse the response
		var respContent RespContent
		if err := s.json.Unmarshal(body, &respContent); err != nil {
			log.Errorf("Failed to unmarshal JSON response: %v", err)
			return nil, fmt.Errorf("failed to unmarshal response: %w", err)
		}

		// Check status and return result
		if isSuccessStatus(respContent.Status) {
			log.Infof("Load operation completed successfully")
			return &LoadResponse{
				Status: SUCCESS,
				Resp:   respContent,
			}, nil
		} else {
			log.Errorf("Load operation failed with status: %s", respContent.Status)
			errorMessage := ""
			if respContent.Message != "" {
				errorMessage = fmt.Sprintf("load failed. cause by: %s, please check more detail from url: %s",
					respContent.Message, respContent.ErrorURL)
			} else {
				errorMessage = string(body)
			}
			return &LoadResponse{
				Status:       FAILURE,
				Resp:         respContent,
				ErrorMessage: errorMessage,
			}, nil
		}
	}

	// For non-200 status codes, return an error that can be retried
	log.Errorf("Stream load failed with HTTP status: %s", resp.Status)
	return nil, exception.NewStreamLoadError(fmt.Sprintf("stream load error: %s", resp.Status))
}

// isSuccessStatus checks if the status indicates success
func isSuccessStatus(status string) bool {
	return strings.EqualFold(status, "success")
}
