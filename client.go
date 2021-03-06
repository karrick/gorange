package gorange

import (
	"bufio"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/karrick/gogetter"
)

// Client attempts to resolve range queries to a list of strings or an error.
type Client struct {
	Getter gogetter.Getter
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
	resp, err := c.Getter.Get(url.QueryEscape(query))
	if err != nil {
		return nil, err
	}

	// got a response from this server, so commit to reading entire body (needed when re-using
	// Keep-Alive connections)
	defer func(iorc io.ReadCloser) {
		io.Copy(ioutil.Discard, iorc) // so we can reuse connections via Keep-Alive
		iorc.Close()
	}(resp.Body)

	// NOTE: wrap known range exceptions
	rangeException := resp.Header.Get("RangeException")
	if rangeException != "" {
		return nil, ErrRangeException{rangeException}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, ErrStatusNotOK{resp.Status, resp.StatusCode}
	}

	var lines []string

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		lines = append(lines, strings.TrimSpace(scanner.Text()))
	}

	if err = scanner.Err(); err != nil {
		return nil, ErrParseException{err}
	}

	return lines, nil
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
	return "response status code: " + strconv.Itoa(err.StatusCode)
}

// ErrParseException is returned by Client.Query method when an error occurs while parsing the Get
// response.
type ErrParseException struct {
	Err error
}

func (err ErrParseException) Error() string {
	return "cannot parse response: " + err.Err.Error()
}
