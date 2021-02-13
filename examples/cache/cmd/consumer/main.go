package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"cache"

	"github.com/brandoshmando/backfill"
	"github.com/go-chi/chi"
	"github.com/google/uuid"
)

var (
	kafka_host  string = os.Getenv("KAFKA_HOST")
	kafka_topic string = os.Getenv("KAFKA_TOPIC")
)

func handleFunc(c *cache.Cache) backfill.HandlerFunc {
	return func(m backfill.Message) {
		var k uuid.UUID
		if err := json.Unmarshal(m.Key, &k); err != nil {
			log.Fatalf("failed to unmarshal key: %v", err)
		}

		var d cache.Doc
		if err := json.Unmarshal(m.Value, &d); err != nil {
			log.Fatalf("failed to unmarshal doc: %v", err)
		}

		c.Put(k, d)
	}
}

type Ready struct {
	r   bool
	mtx sync.RWMutex
}

func (r *Ready) Init() {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.r = true
}

func (r *Ready) Status() bool {
	return r.r
}

func main() {
	handler, err := backfill.NewHandler(backfill.Config{
		Host:      kafka_host,
		Topic:     kafka_topic,
		Namespace: "consumer",
	})
	if err != nil {
		log.Println("error creating handler: ", err)
		return
	}

	var store cache.Cache
	handler = handler.WithFiller(handleFunc(&store))

	var r Ready
	go func() {
		<-handler.Filled()
		r.Init()

		log.Printf("consumer backfilled successfully")
	}()

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ch
		cancel()
	}()

	log.Println("starting handler")
	handler.Start(ctx)

	router := chi.NewRouter()
	router.Get("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if r.Status() {
			w.WriteHeader(http.StatusOK)
			return
		}

		w.WriteHeader(http.StatusServiceUnavailable)
	})
	router.Get("/docs", func(w http.ResponseWriter, _ *http.Request) {
		docs := store.List()
		resp := struct {
			count int
			docs  []*cache.Doc
		}{
			count: len(docs),
			docs:  docs,
		}

		b, err := json.Marshal(resp)
		if err != nil {
			log.Printf("failed to marshal response: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(b)
	})
	router.Get("/docs/{uuid}", func(w http.ResponseWriter, req *http.Request) {
		docUUID, err := uuid.Parse(chi.URLParam(req, "uuid"))
		if err != nil {
			log.Printf("failed to unmarshal uuid from url path: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		d := store.Get(docUUID)

		b, err := json.Marshal(d)
		if err != nil {
			log.Printf("failed to marshal response: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(b)
	})

	log.Println("listening on port 8080")
	http.ListenAndServe(":8080", router)
}
