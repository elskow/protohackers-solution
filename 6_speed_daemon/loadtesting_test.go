package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

func BenchmarkIncrementalLoadTest(b *testing.B) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var totalLatency time.Duration
	var results []plotter.XY

	step := 100
	numConnections := step

	for {
		totalLatency = 0

		// Simulate multiple camera connections
		for i := 0; i < numConnections; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				start := time.Now()
				conn := &mockConn{}
				reader := bufio.NewReader(bytes.NewReader([]byte{0x00, 0x01, 0x00, 0x02, 0x00, 0x50}))
				handleIAmCamera(conn, reader)
				latency := time.Since(start)
				mu.Lock()
				totalLatency += latency
				mu.Unlock()
			}()
		}

		// Simulate multiple dispatcher connections
		for i := 0; i < numConnections; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				start := time.Now()
				conn := &mockConn{}
				reader := bufio.NewReader(bytes.NewReader([]byte{0x01, 0x00, 0x01}))
				handleIAmDispatcher(conn, reader)
				latency := time.Since(start)
				mu.Lock()
				totalLatency += latency
				mu.Unlock()
			}()
		}

		// Simulate multiple plate observations
		for i := 0; i < numConnections; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				start := time.Now()
				conn := &mockConn{}
				reader := bufio.NewReader(bytes.NewReader([]byte{0x07, 'A', 'B', 'C', '1', '2', '3', 0x00, 0x00, 0x00, 0x01}))
				handleIAmCamera(conn, bufio.NewReader(bytes.NewReader([]byte{0x00, 0x01, 0x00, 0x02, 0x00, 0x50})))
				handlePlate(conn, reader)
				latency := time.Since(start)
				mu.Lock()
				totalLatency += latency
				mu.Unlock()
			}()
		}

		// Wait for all goroutines to finish
		wg.Wait()

		// Calculate average latency
		averageLatency := totalLatency / time.Duration(numConnections*3)
		fmt.Printf("Connections: %d, Average latency: %v\n", numConnections, averageLatency)

		// Record the result
		results = append(results, plotter.XY{X: float64(numConnections), Y: float64(averageLatency.Milliseconds())})

		// Stop if latency exceeds 50 ms
		if averageLatency > 50*time.Millisecond {
			break
		}

		numConnections += step
	}

	// Plot the results
	p := plot.New()
	p.Title.Text = "System Load Test"
	p.X.Label.Text = "Number of Connections"
	p.Y.Label.Text = "Average Latency (ms)"

	// Add system information to the plot
	systemInfo := fmt.Sprintf("Go version: %s\nCPU cores: %d\n", runtime.Version(), runtime.NumCPU())
	p.Title.Text += "\n" + systemInfo

	pts := make(plotter.XYs, len(results))
	for i, result := range results {
		pts[i].X = result.X
		pts[i].Y = result.Y
	}

	line, err := plotter.NewLine(pts)
	if err != nil {
		panic(err)
	}
	p.Add(line)

	if err := p.Save(8*vg.Inch, 6*vg.Inch, "load_test.png"); err != nil {
		panic(err)
	}
}

func BenchmarkCheckSpeedLoadTest(b *testing.B) {
	road := uint16(1)
	plate := "ABC123"
	observations[road] = map[string][]Observation{
		plate: {
			{plate: plate, timestamp: 1, mile: 1},
			{plate: plate, timestamp: 2, mile: 2},
		},
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var totalLatency time.Duration
	var results []plotter.XY

	step := 100
	numChecks := step

	for {
		totalLatency = 0

		// Simulate multiple speed checks
		for i := 0; i < numChecks; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				start := time.Now()
				checkSpeed(road, plate)
				latency := time.Since(start)
				mu.Lock()
				totalLatency += latency
				mu.Unlock()
			}()
		}

		// Wait for all goroutines to finish
		wg.Wait()

		// Calculate average latency
		averageLatency := totalLatency / time.Duration(numChecks)
		fmt.Printf("Speed Checks: %d, Average latency: %v\n", numChecks, averageLatency)

		// Record the result
		results = append(results, plotter.XY{X: float64(numChecks), Y: float64(averageLatency.Milliseconds())})

		// Stop if latency exceeds 50 ms
		if averageLatency > 50*time.Millisecond {
			break
		}

		numChecks += step
	}

	// Plot the results
	p := plot.New()
	p.Title.Text = "Speed Check Load Test"
	p.X.Label.Text = "Number of Speed Checks"
	p.Y.Label.Text = "Average Latency (ms)"

	// Add system information to the plot
	systemInfo := fmt.Sprintf("Go version: %s\nCPU cores: %d\n", runtime.Version(), runtime.NumCPU())
	p.Title.Text += "\n" + systemInfo

	pts := make(plotter.XYs, len(results))
	for i, result := range results {
		pts[i].X = result.X
		pts[i].Y = result.Y
	}

	line, err := plotter.NewLine(pts)
	if err != nil {
		panic(err)
	}
	p.Add(line)

	if err := p.Save(8*vg.Inch, 6*vg.Inch, "speed_check_load_test.png"); err != nil {
		panic(err)
	}
}

// Mock connection to simulate net.Conn
type mockConn struct {
	net.Conn
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	return len(b), nil
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	return len(b), nil
}

func (m *mockConn) Close() error {
	return nil
}
