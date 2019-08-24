package main

import (
	"context"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type Master struct {
	workers  []*Worker
	store    RequestStore
	requests chan *StoredRequest
}

func newMaster(maxConcurrentRequests uint,
	serverURL *url.URL,
	store RequestStore,
	httpClient *http.Client) *Master {
	m := &Master{
		requests: make(chan *StoredRequest),
		store:    store,
	}
	m.workers = make([]*Worker, maxConcurrentRequests)
	for i := 0; i < len(m.workers); i++ {
		m.workers[i] = newWorker(i, m.requests, serverURL, httpClient, store)
	}
	return m
}

func (m *Master) run(ctx context.Context) {
	log.Printf("Starting master")
	var wg sync.WaitGroup
	for _, w := range m.workers {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()
			w.run(ctx)
		}(w)
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("Shutting down master. Waiting for all workers to finish")
			wg.Wait()
			return
		default:
			next, err := m.store.Next(10)
			if err != nil {
				log.Fatalf("Could not read from store: %v", err)
			}
			log.Printf("Read %d requests", len(next))
			enqueued := false
			now := time.Now().UnixNano()
			for _, r := range next {
				if r.DeliveryTime > now {
					break
				}
				if r.Scheduled {
					continue
				}
				log.Printf("Enqueueing message")
				r.Scheduled = true
				m.store.Put(r)
				m.requests <- r
				enqueued = true
			}
			if !enqueued {
				// Don't do this awful busy wait.
				time.Sleep(1 * time.Second)
			}
		}
	}
}
