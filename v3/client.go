package gorange

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// defaultQueryLengthThreshold defines the maximum length of the URI for an
// outgoing GET query.  Queries that require a longer URI will automatically be
// sent out via a PUT query.
const defaultQueryLengthThreshold = 4096

// Client attempts to resolve range queries to a list of strings or an error.
type Client struct {
	httpClient    *http.Client
	servers       *roundRobinStrings
	retryCallback func(error) bool
	retryCount    int
	retryPause    time.Duration
}

// Close cleans up resources held by Client.  Calling Query method after Close
// will result in a panic.
func (c *Client) Close() error {
	c.httpClient = nil
	c.servers = nil
	c.retryCallback = nil
	c.retryCount = 0
	c.retryPause = 0
	return nil
}

// Query sends the specified query string to one or more of the configured
// servers, and converts a non-error result into a list of strings.
//
// If the response includes a RangeException header, it returns
// ErrRangeException.  If the status code is not okay, it returns
// ErrStatusNotOK.  Finally, if it cannot parse the lines in the response body,
// it returns ErrParseException.
//
//     lines, err := querier.Query("%someQuery")
//     if err != nil {
//         fmt.Fprintf(os.Stderr, "%s", err)
//         os.Exit(1)
//     }
//     for _, line := range lines {
//         fmt.Println(line)
//     }
func (rq *Client) Query(expression string) ([]string, error) {
	iorc, err := rq.getFromRangeServers(expression)
	if err != nil {
		return nil, err
	}

	var lines []string
	scanner := bufio.NewScanner(iorc)

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	err = scanner.Err()  // always check for scan error
	cerr := iorc.Close() // always close the stream

	// scan error has more context than close error
	if err != nil {
		return nil, ErrParseException{Err: err}
	}
	if cerr != nil {
		return nil, ErrParseException{Err: cerr}
	}
	return lines, nil
}

// getFromRangeServers iterates through the round robin list of servers, sending
// query to each server, one after the other, until a non-error result is
// obtained. It returns an io.ReadCloser for reading the HTTP response body, or
// an error when all the servers return an error for that query.
func (rq *Client) getFromRangeServers(expression string) (io.ReadCloser, error) {
	var attempts int
	for {
		iorc, err := rq.getFromRangeServer(expression)
		if err == nil || attempts == rq.retryCount || rq.retryCallback(err) == false {
			return iorc, err
		}
		attempts++
		if rq.retryPause > 0 {
			time.Sleep(rq.retryPause)
		}
	}
}

// getFromRangeServer sends to server the query and returns either a
// io.ReadCloser for reading the valid server response, or an error. This
// function attempts to send the query using both GET and PUT HTTP methods. It
// defaults to using GET first, then trying PUT, unless the query length is
// longer than a program constant, in which case it first tries PUT then will
// try GET.
func (rq *Client) getFromRangeServer(expression string) (io.ReadCloser, error) {
	var err error
	var response *http.Response

	endpoint := fmt.Sprintf("http://%s/range/list", rq.servers.Next())
	uri := fmt.Sprintf("%s?%s", endpoint, url.QueryEscape(expression))

	// Default to using GET request because most servers support it. However,
	// opt for PUT when extremely long query length.
	var method string
	if len(uri) > defaultQueryLengthThreshold {
		method = http.MethodPut
	} else {
		method = http.MethodGet
	}

	var herr error

	// At least 2 tries so we can try GET or POST if server gives us 405 or 414.
	for triesRemaining := 2; triesRemaining > 0; triesRemaining-- {
		switch method {
		case http.MethodGet:
			response, err = rq.httpClient.Get(uri)
		case http.MethodPut:
			response, err = rq.putQuery(endpoint, expression)
		default:
			panic(fmt.Errorf("cannot use unsupported HTTP method: %q", method))
		}
		if err != nil {
			return nil, err // could not even make network request
		}

		// Network round trip completed successfully, but there still might be
		// an error condition encoded in the response.

		switch response.StatusCode {
		case http.StatusOK:
			if message := response.Header.Get("RangeException"); message != "" {
				return nil, ErrRangeException{Message: message}
			}
			return response.Body, nil // range server provided non-error response
		case http.StatusRequestURITooLong:
			method = http.MethodPut // try again using PUT
			herr = ErrStatusNotOK{
				Status:     response.Status,
				StatusCode: response.StatusCode,
			}
		case http.StatusMethodNotAllowed:
			method = http.MethodGet // try again using GET
			herr = ErrStatusNotOK{
				Status:     response.Status,
				StatusCode: response.StatusCode,
			}
		default:
			herr = ErrStatusNotOK{
				Status:     response.Status,
				StatusCode: response.StatusCode,
			}
		}
	}

	return nil, herr
}

func (rq *Client) putQuery(endpoint, query string) (*http.Response, error) {
	form := url.Values{"query": []string{query}}
	request, err := http.NewRequest(http.MethodPut, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	return rq.httpClient.Do(request)
}

// ErrRangeException is returned when the response headers includes
// 'RangeException'.
type ErrRangeException struct {
	Message string
}

func (err ErrRangeException) Error() string {
	return "RangeException: " + err.Message
}

// ErrStatusNotOK is returned when the response status code is not Ok.
type ErrStatusNotOK struct {
	Status     string
	StatusCode int
}

func (err ErrStatusNotOK) Error() string {
	return err.Status
}

// ErrParseException is returned by Client.Query method when an error occurs
// while reading the io.ReadCloser from the response.
type ErrParseException struct {
	Err error
}

func (err ErrParseException) Error() string {
	return "cannot parse response: " + err.Err.Error()
}
