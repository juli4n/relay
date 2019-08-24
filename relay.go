package main

import (
	"context"
	"crypto/rand"
	"flag"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	serverHostname := flag.String("hostname", "http://google.com", "")
	maxConcurrentRequests := flag.Uint("max_concurrent_requests", 10, "")
	flag.Parse()

	server, err := initServer(*maxConcurrentRequests, *serverHostname)
	if err != nil {
		log.Fatalf("Could noit initialize server: %v", err)
	}

	s := &http.Server{
		Addr:           ":8080",
		Handler:        server,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	ctx, cancel := context.WithCancel(context.Background())
	//shutdown := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)
		signal.Notify(sigint, syscall.SIGTERM)
		<-sigint
		log.Print("Shutting down HTTP server\n")
		if err := s.Shutdown(ctx); err != nil {
			log.Fatal(err)
		}
		cancel()
		//close(shutdown)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		server.master.run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	wg.Wait()
	log.Print("Service shutdown completed\n")
}

type Server struct {
	store                 RequestStore
	master                *Master
	maxConcurrentRequests uint
	serverURL             *url.URL
}

func initServer(maxConcurrentRequests uint, serverHostname string) (*Server, error) {
	s, err := newBoltRequestStore("requests.db")
	if err != nil {
		return nil, err
	}
	serverURL, err := url.Parse(serverHostname)
	if err != nil {
		return nil, err
	}
	// Expose parameters as flags
	httpClient := http.DefaultClient
	return &Server{
		store:                 s,
		maxConcurrentRequests: maxConcurrentRequests,
		serverURL:             serverURL,
		master:                newMaster(maxConcurrentRequests, serverURL, s, httpClient),
	}, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	uid := make([]byte, 8)
	if _, err := rand.Read(uid); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	sr := &StoredRequest{
		UID:          uid,
		DeliveryTime: time.Now().UnixNano(),
		Url:          r.URL.String(),
		Method:       r.Method,
		TTL:          3, // TODO: Read from header
	}
	log.Printf("Storing request: %v", sr)
	if err := s.store.Put(sr); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
