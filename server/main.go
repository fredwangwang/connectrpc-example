package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	samplev1 "example.com/sample/server/gen/sample/v1"
	"example.com/sample/server/gen/sample/v1/samplev1connect"
)

const (
	maxStreamLifetime = 5 * time.Minute
	notifyInterval    = 1 * time.Second
	heartbeatEvery    = 5 // every Nth tick is a heartbeat
)

type server struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

func (s *server) Echo(ctx context.Context, req *connect.Request[samplev1.EchoRequest]) (*connect.Response[samplev1.EchoResponse], error) {
	log.Printf("Echo: %q (%s)", req.Msg.Message, httpVersion(ctx))
	return connect.NewResponse(&samplev1.EchoResponse{
		Message:     req.Msg.Message,
		HttpVersion: httpVersion(ctx),
	}), nil
}

func (s *server) ReportTelemetry(ctx context.Context, req *connect.Request[samplev1.ReportTelemetryRequest]) (*connect.Response[samplev1.ReportTelemetryResponse], error) {
	id := req.Msg.BatchId
	s.mu.Lock()
	_, dup := s.seen[id]
	if id != "" && !dup {
		s.seen[id] = struct{}{}
	}
	s.mu.Unlock()

	n := int32(len(req.Msg.Events))
	log.Printf("ReportTelemetry: batch=%q events=%d duplicate=%v", id, n, dup)
	res := &samplev1.ReportTelemetryResponse{HttpVersion: httpVersion(ctx), Duplicate: dup}
	if !dup {
		res.Accepted = n
	}
	return connect.NewResponse(res), nil
}

func (s *server) Subscribe(ctx context.Context, req *connect.Request[samplev1.SubscribeRequest], stream *connect.ServerStream[samplev1.Notification]) error {
	ctx, cancel := context.WithTimeout(ctx, maxStreamLifetime)
	defer cancel()

	id := req.Msg.LastSeenId
	limit := int(req.Msg.MaxNotifications)
	sent := 0
	ver := httpVersion(ctx)
	log.Printf("Subscribe: resume after id=%d limit=%d", id, limit)

	tick := time.NewTicker(notifyInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("Subscribe: done (%v)", ctx.Err())
			return nil
		case <-tick.C:
			id++
			n := &samplev1.Notification{Id: id, HttpVersion: ver}
			if id%heartbeatEvery == 0 {
				n.Heartbeat = true
			} else {
				n.Text = fmt.Sprintf("notification #%d", id)
			}
			if err := stream.Send(n); err != nil {
				return err
			}
			sent++
			if limit > 0 && sent >= limit {
				return nil
			}
		}
	}
}

type ctxKey struct{}

// httpVersion returns the negotiated HTTP version stashed by the middleware.
func httpVersion(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKey{}).(string); ok {
		return v
	}
	return "unknown"
}

func withHTTPVersion(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s content-type=%s", r.Proto, r.URL.Path, r.Header.Get("Content-Type"))
		ctx := context.WithValue(r.Context(), ctxKey{}, r.Proto)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func main() {
	addr := flag.String("addr", "localhost:8080", "listen address")
	flag.Parse()

	mux := http.NewServeMux()
	path, handler := samplev1connect.NewSampleServiceHandler(&server{seen: map[string]struct{}{}})
	mux.Handle(path, handler)

	// h2c lets one cleartext listener serve both HTTP/1.1 and HTTP/2
	// (prior knowledge). Streaming over h1 needs no special setup.
	h := h2c.NewHandler(withHTTPVersion(mux), &http2.Server{})
	srv := &http.Server{Addr: *addr, Handler: h, ReadHeaderTimeout: 5 * time.Second}
	log.Printf("listening on %s (h1 + h2c)", *addr)
	log.Fatal(srv.ListenAndServe())
}
