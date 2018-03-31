package gorange

import (
	"bufio"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	gogetter "github.com/karrick/gogetter/v2"
)

// Client attempts to resolve range queries to a list of strings or an error.
type Client struct {
	Getter gogetter.Getter
}

// Close returns nil error.
func (c *Client) Close() error {
	return nil
}

// Expand sends the specified query string to the Client's Getter, and converts a non-error result
// into a slice of bytes.
//
// If the response includes a RangeException header, it returns ErrRangeException. If the status
// code is not okay, it returns ErrStatusNotOK. Finally, if it cannot parse the lines in the
// response body, it returns ErrParseException.
//
//	// use the range querier
//	result, err := querier.Expand("%someQuery")
//	if err != nil {
//		fmt.Fprintf(os.Stderr, "%s", err)
//		os.Exit(1)
//	}
//	fmt.Printf("%s\n", result)
func (c *Client) Expand(query string) (string, error) {
	resp, err := c.Getter.Get("/range/expand?" + url.QueryEscape(query))
	if err != nil {
		return "", err
	}

	// got a response from this server, so commit to reading entire body (needed when re-using
	// Keep-Alive connections)
	defer func(iorc io.ReadCloser) {
		io.Copy(ioutil.Discard, iorc) // so we can reuse connections via Keep-Alive
		iorc.Close()
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return "", ErrStatusNotOK{resp.Status, resp.StatusCode}
	}
	if rangeException := resp.Header.Get("RangeException"); rangeException != "" {
		return "", ErrRangeException{rangeException}
	}

	bb, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(bb), nil
}

// List sends the specified query string to the Client's Getter, and converts a non-error result
// into a list of strings.
//
// If the response includes a RangeException header, it returns ErrRangeException. If the status
// code is not okay, it returns ErrStatusNotOK. Finally, if it cannot parse the lines in the
// response body, it returns ErrParseException.
//
//	// use the range querier
//	list, err := querier.List("%someQuery")
//	if err != nil {
//		fmt.Fprintf(os.Stderr, "%s", err)
//		os.Exit(1)
//	}
//	for _, line := range list {
//		fmt.Println(line)
//	}
func (c *Client) List(query string) ([]string, error) {
	iorc, err := c.Raw(query)
	if err != nil {
		return nil, err
	}

	// got a response from this server, so commit to reading entire body (needed
	// when re-using Keep-Alive connections)
	defer func(iorc io.ReadCloser) {
		_, _ = io.Copy(ioutil.Discard, iorc) // so we can reuse connections via Keep-Alive
		_ = iorc.Close()
	}(iorc)

	var lines []string

	scanner := bufio.NewScanner(iorc)
	for scanner.Scan() {
		lines = append(lines, strings.TrimSpace(scanner.Text()))
	}

	if err = scanner.Err(); err != nil {
		return nil, ErrParseException{err}
	}

	return lines, nil
}

// Query sends the specified query string to the Client's Getter, and converts a non-error result
// into a list of strings.
//
// If the response includes a RangeException header, it returns ErrRangeException. If the status
// code is not okay, it returns ErrStatusNotOK. Finally, if it cannot parse the lines in the
// response body, it returns ErrParseException.
//
//	// use the range querier
//	lines, err := querier.Query("%someQuery")
//	if err != nil {
//		fmt.Fprintf(os.Stderr, "%s", err)
//		os.Exit(1)
//	}
//	for _, line := range lines {
//		fmt.Println(line)
//	}
func (c *Client) Query(query string) ([]string, error) {
	return c.List(query)
}

// Raw sends the range request and checks for invalid responses from
// downstream. If the response is valid, this returns the response body as an
// io.ReadCloser for the client to use. It is the client's responsibility to
// invoke the Close method on the returned io.ReadCloser.
func (c *Client) Raw(query string) (io.ReadCloser, error) {
	resp, err := c.Getter.Get("/range/list?" + url.QueryEscape(query))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, ErrStatusNotOK{resp.Status, resp.StatusCode}
	}
	if rangeException := resp.Header.Get("RangeException"); rangeException != "" {
		return nil, ErrRangeException{rangeException}
	}
	return resp.Body, nil
}

// ErrRangeException is returned when the response headers includes 'RangeException'.
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

// ErrParseException is returned by Client.Query method when an error occurs while parsing the Get
// response.
type ErrParseException struct {
	Err error
}

func (err ErrParseException) Error() string {
	return "cannot parse response: " + err.Err.Error()
}
