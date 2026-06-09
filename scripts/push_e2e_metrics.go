// push_e2e_metrics reads `go test -json` output from stdin, pretty-prints it,
// then pushes per-test pass/fail and duration metrics to a Prometheus Pushgateway.
//
// Usage (via make test-e2e):
//
//	go test ./test/e2e/... -tags=e2e -v -json 2>&1 | go run scripts/push_e2e_metrics.go
//
// Set PUSHGATEWAY_URL to override the default (http://localhost:9091).
// If the pushgateway is unreachable the script logs a warning but still exits
// with code 1 when any test failed.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

type testEvent struct {
	Action  string  `json:"Action"`
	Test    string  `json:"Test"`
	Elapsed float64 `json:"Elapsed"`
	Output  string  `json:"Output"`
}

type testResult struct {
	passed   bool
	duration float64
}

// loadNotifCounts maps each load test to the number of notifications it exercises.
var loadNotifCounts = map[string]int{
	"TestE2E_Load_ConcurrentCreates":     50,
	"TestE2E_Load_HighVolumeBatch":       200,
	"TestE2E_Load_ConcurrentIdempotency": 25,
	"TestE2E_Load_PriorityOrdering":      30,
}

func main() {
	pushURL := os.Getenv("PUSHGATEWAY_URL")
	if pushURL == "" {
		pushURL = "http://localhost:9091"
	}

	results := make(map[string]testResult)

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		var ev testEvent
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			fmt.Println(scanner.Text())
			continue
		}
		if ev.Action == "output" {
			fmt.Print(ev.Output)
		}
		if ev.Test == "" {
			continue
		}
		switch ev.Action {
		case "pass":
			results[ev.Test] = testResult{passed: true, duration: ev.Elapsed}
		case "fail":
			results[ev.Test] = testResult{passed: false, duration: ev.Elapsed}
		}
	}

	if len(results) == 0 {
		return
	}

	var buf bytes.Buffer
	passed, failed := 0, 0
	loadPassed, loadFailed := 0, 0
	totalLoadNotifs := 0

	for name, r := range results {
		v := 0
		if r.passed {
			v = 1
			passed++
		} else {
			failed++
		}
		fmt.Fprintf(&buf, "e2e_test_result{test=%q} %d\n", name, v)
		fmt.Fprintf(&buf, "e2e_test_duration_seconds{test=%q} %g\n", name, r.duration)

		// Extra load-test specific metrics.
		if strings.HasPrefix(name, "TestE2E_Load_") {
			fmt.Fprintf(&buf, "e2e_load_test_result{test=%q} %d\n", name, v)
			fmt.Fprintf(&buf, "e2e_load_test_duration_seconds{test=%q} %g\n", name, r.duration)
			if n, ok := loadNotifCounts[name]; ok {
				fmt.Fprintf(&buf, "e2e_load_notifications_total{test=%q} %d\n", name, n)
				if r.passed {
					totalLoadNotifs += n
				}
			}
			if r.passed {
				loadPassed++
			} else {
				loadFailed++
			}
		}
	}

	total := passed + failed
	fmt.Fprintf(&buf, "e2e_tests_total %d\n", total)
	fmt.Fprintf(&buf, "e2e_tests_passed %d\n", passed)
	fmt.Fprintf(&buf, "e2e_tests_failed %d\n", failed)
	fmt.Fprintf(&buf, "e2e_load_tests_passed %d\n", loadPassed)
	fmt.Fprintf(&buf, "e2e_load_tests_failed %d\n", loadFailed)
	fmt.Fprintf(&buf, "e2e_load_notifications_delivered_total %d\n", totalLoadNotifs)
	fmt.Fprintf(&buf, "e2e_last_run_timestamp_seconds %d\n", time.Now().Unix())

	url := pushURL + "/metrics/job/e2e_tests"
	// Use PUT to atomically replace all metrics for this job,
	// so stale values from a previous failed run are cleared.
	req, err := http.NewRequest(http.MethodPut, url, &buf) // #nosec G107 -- internal dev pushgateway
	if err != nil {
		fmt.Fprintf(os.Stderr, "pushgateway request build failed: %v\n", err)
		if failed > 0 {
			os.Exit(1)
		}
		return
	}
	req.Header.Set("Content-Type", "text/plain; version=0.0.4")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pushgateway unavailable (skipping metrics push): %v\n", err)
	} else {
		resp.Body.Close()
		fmt.Fprintf(os.Stderr, "\ne2e metrics pushed → %s (%s)  passed=%d failed=%d  load-notifs=%d\n",
			url, resp.Status, passed, failed, totalLoadNotifs)
	}

	if failed > 0 {
		os.Exit(1)
	}
}
