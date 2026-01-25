package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"

	"github.com/acrlabs/prom2parquet/pkg/parquet"
)

// Prometheus metrics
var (
	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "prom2parquet_requests_total",
		Help: "Total number of requests received",
	}, []string{"endpoint", "status"})

	samplesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "prom2parquet_samples_received_total",
		Help: "Total number of samples received",
	})

	timeseriesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "prom2parquet_timeseries_received_total",
		Help: "Total number of timeseries received",
	})

	activeWriters = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "prom2parquet_active_writers",
		Help: "Number of active parquet writers",
	})

	requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "prom2parquet_request_duration_seconds",
		Help:    "Request duration in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"endpoint"})
)

const (
	prefixLabelKey = "prom2parquet_prefix"

	shutdownTime = 30 * time.Second
)

type promserver struct {
	httpserv *http.Server
	opts     *options
	channels map[string]chan prompb.TimeSeries

	m            sync.RWMutex
	flushChannel chan os.Signal
	killChannel  chan os.Signal

	// Health state
	isReady   atomic.Bool
	startTime time.Time
}

func newServer(opts *options) *promserver {
	fulladdr := fmt.Sprintf(":%d", opts.port)
	mux := http.NewServeMux()

	s := &promserver{
		httpserv:  &http.Server{Addr: fulladdr, Handler: mux, ReadHeaderTimeout: 10 * time.Second},
		opts:      opts,
		channels:  map[string]chan prompb.TimeSeries{},
		startTime: time.Now(),

		flushChannel: make(chan os.Signal, 1),
		killChannel:  make(chan os.Signal, 1),
	}

	// Data endpoints
	mux.HandleFunc("/receive", s.metricsReceive)
	mux.HandleFunc("/flush", s.flushData)

	// Health endpoints
	mux.HandleFunc("/health", s.healthCheck)
	mux.HandleFunc("/ready", s.readinessCheck)

	// Prometheus metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())

	return s
}

func (self *promserver) run() {
	signal.Notify(self.killChannel, syscall.SIGTERM)

	endChannel := make(chan struct{}, 1)

	go func() {
		if err := self.httpserv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("server failed: %v", err)
		}
	}()

	go func() {
		<-self.killChannel
		self.isReady.Store(false)
		self.handleShutdown()
		close(endChannel)
	}()

	// Mark server as ready
	self.isReady.Store(true)
	log.Infof("server listening on %s", self.httpserv.Addr)
	<-endChannel
}

func (self *promserver) handleShutdown() {
	log.Info("shutting down...")
	log.Infof("flushing all data files")
	for _, ch := range self.channels {
		close(ch)
	}

	ctxTimeout, cancel := context.WithTimeout(context.Background(), shutdownTime)
	defer cancel()
	if err := self.httpserv.Shutdown(ctxTimeout); err != nil {
		log.Errorf("failed shutting server down: %v", err)
	}

	timer := time.AfterFunc(shutdownTime, func() {
		os.Exit(0)
	})

	<-timer.C
}

func (self *promserver) metricsReceive(w http.ResponseWriter, req *http.Request) {
	timer := prometheus.NewTimer(requestDuration.WithLabelValues("receive"))
	defer timer.ObserveDuration()

	body, err := remote.DecodeWriteRequest(req.Body)
	if err != nil {
		requestsTotal.WithLabelValues("receive", "error").Inc()
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Count timeseries and samples
	timeseriesReceived.Add(float64(len(body.Timeseries)))
	for _, ts := range body.Timeseries {
		samplesReceived.Add(float64(len(ts.Samples)))
	}

	if err := self.sendTimeseries(req.Context(), body.Timeseries); err != nil {
		requestsTotal.WithLabelValues("receive", "error").Inc()
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	requestsTotal.WithLabelValues("receive", "success").Inc()
}

func (self *promserver) sendTimeseries(ctx context.Context, timeserieses []prompb.TimeSeries) (err error) {
	for _, ts := range timeserieses {
		// I'm not 100% sure which of these things would be recreated/shadowed below, so to be safe
		// I'm just declaring everything upfront
		var ch chan prompb.TimeSeries
		var ok bool

		nameLabel, _ := lo.Find(ts.Labels, func(i prompb.Label) bool { return i.Name == model.MetricNameLabel })
		prefixLabel, _ := lo.Find(ts.Labels, func(i prompb.Label) bool { return i.Name == prefixLabelKey })
		channelName := prefixLabel.Value + "/" + nameLabel.Value

		log.Debugf("received timeseries data for %s", channelName)

		self.m.RLock()
		ch, ok = self.channels[channelName]
		self.m.RUnlock()

		if !ok {
			ch, err = self.spawnWriter(ctx, channelName)
			if err != nil {
				return fmt.Errorf("could not spawn timeseries writer for %s: %w", channelName, err)
			}
		}

		ch <- ts
	}

	return nil
}

func (self *promserver) spawnWriter(ctx context.Context, channelName string) (chan prompb.TimeSeries, error) {
	self.m.Lock()
	defer self.m.Unlock()

	log.Infof("new metric name seen, creating writer %s", channelName)
	writer, err := parquet.NewProm2ParquetWriter(
		ctx,
		self.opts.backendRoot,
		channelName,
		self.opts.backend,
		self.opts.flushInterval,
	)
	if err != nil {
		return nil, fmt.Errorf("could not create writer for %s: %w", channelName, err)
	}
	ch := make(chan prompb.TimeSeries)
	self.channels[channelName] = ch

	activeWriters.Inc()
	go func() {
		writer.Listen(ch) //nolint:contextcheck // the req context and the backend creation context should be separate
		activeWriters.Dec()
	}()

	return ch, nil
}

func (self *promserver) flushData(w http.ResponseWriter, req *http.Request) {
	d := json.NewDecoder(req.Body)
	d.DisallowUnknownFields()

	flushReq := struct {
		Prefix *string `json:"prefix"`
	}{}

	if err := d.Decode(&flushReq); err != nil {
		requestsTotal.WithLabelValues("flush", "error").Inc()
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if flushReq.Prefix == nil {
		requestsTotal.WithLabelValues("flush", "error").Inc()
		http.Error(w, "missing field 'prefix' in JSON object", http.StatusBadRequest)
		return
	}

	log.Infof("flushing all data for %s", *flushReq.Prefix)
	for chName, ch := range self.channels {
		if strings.HasPrefix(chName, *flushReq.Prefix) {
			close(ch)
		}
	}
	requestsTotal.WithLabelValues("flush", "success").Inc()
}

// healthCheck handles liveness probe requests
func (self *promserver) healthCheck(w http.ResponseWriter, req *http.Request) {
	requestsTotal.WithLabelValues("health", "success").Inc()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"status":  "healthy",
		"uptime":  time.Since(self.startTime).String(),
		"version": "1.0.0",
	}
	json.NewEncoder(w).Encode(response)
}

// readinessCheck handles readiness probe requests
func (self *promserver) readinessCheck(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if !self.isReady.Load() {
		requestsTotal.WithLabelValues("ready", "not_ready").Inc()
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "not_ready",
			"reason": "server is shutting down or not yet initialized",
		})
		return
	}

	self.m.RLock()
	writerCount := len(self.channels)
	self.m.RUnlock()

	requestsTotal.WithLabelValues("ready", "success").Inc()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":         "ready",
		"active_writers": writerCount,
		"backend_root":   self.opts.backendRoot,
	})
}
