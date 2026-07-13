package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

type Target struct {
	Name            string `json:"name"`
	Url             string `json:"url"`
	IntervalSeconds int    `json:"interval_seconds"`
}

type Result struct {
	Target     string
	StatusCode int
	Latency    time.Duration
	Err        error
	When       time.Time
}

func worker(id int, jobs <-chan Target, results chan<- Result) {
	for j := range jobs {
		fmt.Println("Worker", id, "started job", j)
		started := time.Now()
		resp, err := http.Get(j.Url)
		result := Result{
			Target:  j.Name,
			Latency: time.Since(started),
			Err:     err,
			When:    started,
		}
		if err == nil {
			result.StatusCode = resp.StatusCode
			resp.Body.Close()
		}
		results <- result
	}
}

func main() {
	var targets []Target
	targetsRaw, err := os.ReadFile("targets.json")
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(targetsRaw, &targets); err != nil {
		panic(err)
	}

	jobs := make(chan Target)
	results := make(chan Result)

	var wg sync.WaitGroup

	for i := range 2 {
		wg.Go(func() {
			worker(i, jobs, results)
		})
	}

	wg.Go(func() {
		for range targets {
			fmt.Println(<-results)
		}
	})

	for i, t := range targets {
		jobs <- t
		fmt.Println("Pushed job", i)
		close(jobs)
	}
	wg.Wait()
	fmt.Println("Bye!")
}
