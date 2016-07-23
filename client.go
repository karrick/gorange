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

// Client attempts to resolve range queries to list of strings or an error.
type Client struct {
	Getter gogetter.Getter
}

// Query sends the specified query string to the Client's Getter, and converts a non-error result
// into a list of strings.
//
// If the response includes a RangeException header, it returns ErrRangeException. If the status
// code is not okay, it returns ErrStatusNotOK. Finally, if it cannot parse the lines in the
// response body, it returns ErrParseException.
func (rc *Client) Query(query string) ([]string, error) {
	resp, err := rc.Getter.Get(url.QueryEscape(query))
	if err != nil {
		return nil, err
	}

	// got a response from this server, so commit to reading entire body (needed when re-using
	// connections)
	defer func(iorc io.ReadCloser) {
		io.Copy(ioutil.Discard, iorc) // so we can reuse connections via Keep-Alive
		iorc.Close()
	}(resp.Body)

	// NOTE: wrap known range exceptions
	rangeException := resp.Header.Get("RangeException")
	if rangeException != "" {
		// if strings.HasPrefix(rangeException, "NOCLUSTERDEF") {
		// 	return nil, ErrNoClusterDef{rangeException}
		// } else if strings.HasPrefix(rangeException, "NOCLUSTER") {
		// 	return nil, ErrNoCluster{rangeException}
		// } else if strings.HasPrefix(rangeException, "NO_COLO") {
		// 	return nil, ErrNoColo{rangeException}
		// } else if strings.HasPrefix(rangeException, "NOTINYDNS") {
		// 	return nil, ErrNoTinyDNS{rangeException}
		// } else if strings.HasPrefix(rangeException, "HOST_NO_NETBLOCK") {
		// 	return nil, ErrHostNoNetblock{rangeException}
		// } else if strings.HasPrefix(rangeException, "NETBLOCK_NOT_FOUND") {
		// 	return nil, ErrNetblockNotFound{rangeException}
		// } else if strings.HasPrefix(rangeException, "DC_NOT_FOUND") {
		// 	return nil, ErrDCNotFound{rangeException}
		// } else if strings.HasPrefix(rangeException, "NOVIPS") {
		// 	return nil, ErrNoVIPs{rangeException}
		// }
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

// type ErrNoClusterDef struct {
// 	Message string
// }

// func (err ErrNoClusterDef) Error() string {
// 	return err.Message
// }

// type ErrNoTinyDNS struct {
// 	Message string
// }

// func (err ErrNoTinyDNS) Error() string {
// 	return err.Message
// }

// type ErrHostNoNetblock struct {
// 	Message string
// }

// func (err ErrHostNoNetblock) Error() string {
// 	return err.Message
// }

// type ErrNoColo struct {
// 	Message string
// }

// func (err ErrNoColo) Error() string {
// 	return err.Message
// }

// type ErrNetblockNotFound struct {
// 	Message string
// }

// func (err ErrNetblockNotFound) Error() string {
// 	return err.Message
// }

// type ErrDCNotFound struct {
// 	Message string
// }

// func (err ErrDCNotFound) Error() string {
// 	return err.Message
// }

// type ErrNoVIPs struct {
// 	Message string
// }

// func (err ErrNoVIPs) Error() string {
// 	return err.Message
// }

// type ErrNoCluster struct {
// 	Message string
// }

// func (err ErrNoCluster) Error() string {
// 	return err.Message
// }
