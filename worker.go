package main

import (
	"context"
	"log"
	"net/http"
	"net/url"
	"time"
)

type Worker struct {
	id        int
	requests  chan *StoredRequest
	serverURL *url.URL
	client    *http.Client
	store     RequestStore
}

func newWorker(id int,
	requests chan *StoredRequest,
	serverURL *url.URL,
	client *http.Client,
	store RequestStore) *Worker {
	return &Worker{
		id:        id,
		requests:  requests,
		serverURL: serverURL,
		client:    client,
		store:     store,
	}
}

func (w *Worker) run(ctx context.Context) {
	log.Printf("Starting worker %d\n", w.id)
	for {
		select {
		case <-ctx.Done():
			log.Printf("Shutting down worker %d", w.id)
			return
		case r := <-w.requests:
			w.handleRequest(r)
		}
	}
}

func (w *Worker) handleRequest(s *StoredRequest) {
	log.Printf("Handling request %v", s)

	url, err := url.Parse(w.serverURL.String())
	if err != nil {
		log.Printf("Failed to parse UR: %v", err)
		w.reschedule(s)
	}
	url.Path = s.Url
	req, err := http.NewRequest(s.Method, url.String(), nil)
	if err != nil {
		log.Fatalf("Failed to create request: %v", s)
	}

	log.Printf("Sending request: %v", req)

	res, err := w.client.Do(req)

	if err != nil {
		log.Printf("Request failed: %v", err)
		w.reschedule(s)
		return
	}
	if res.StatusCode != 200 {
		log.Printf("Invalid response status: %d", res.StatusCode)
		w.reschedule(s)
		return
	}

	err = w.store.Delete(s)
	if err != nil {
		log.Fatalf("Could not delete stored request: %v", err)
	}

}

func (w *Worker) reschedule(s *StoredRequest) {
	rescheduled := &StoredRequest{
		UID: s.UID,
		// Use a parameterized exponential backoff
		DeliveryTime: s.DeliveryTime + int64(time.Second*10),
		Url:          s.Url,
		Method:       s.Method,
		Scheduled:    false,
	}

	if s.TTL > 0 {
		rescheduled.TTL = s.TTL - 1
	}

	if rescheduled.TTL == 0 {
		log.Printf("Expired TTL: %v", s)
		err := w.store.Delete(s)
		if err != nil {
			log.Fatalf("Could not delete request: %v", err)
		}
		return
	}

	err := w.store.Reschedule(s, rescheduled)
	if err != nil {
		log.Fatalf("Could not store rescheduled request: %v", err)
	}
}
