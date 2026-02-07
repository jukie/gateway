// Package main implements a standalone LRS aggregator and EDS server for testing
// the LRS_REPORTED_RATE locality basis feature. It receives LRS reports from Envoy,
// aggregates per-locality traffic fractions using EWMA, and injects those fractions
// back into EDS responses via locality metadata.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	loadstatsv3 "github.com/envoyproxy/go-control-plane/envoy/service/load_stats/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	cachetypes "github.com/envoyproxy/go-control-plane/pkg/cache/types"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	serverv3 "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// localityKey is a comparable key for envoy Locality protos.
type localityKey struct {
	Region  string
	Zone    string
	SubZone string
}

func toKey(l *corev3.Locality) localityKey {
	if l == nil {
		return localityKey{}
	}
	return localityKey{Region: l.GetRegion(), Zone: l.GetZone(), SubZone: l.GetSubZone()}
}

func toProto(k localityKey) *corev3.Locality {
	return &corev3.Locality{Region: k.Region, Zone: k.Zone, SubZone: k.SubZone}
}

// aggregator tracks per-locality request rates and computes EWMA-smoothed
// traffic fractions in basis points (0-10000).
type aggregator struct {
	mu    sync.Mutex
	alpha float64 // EWMA smoothing factor (0-1)
	rates map[localityKey]float64
}

func newAggregator(alpha float64) *aggregator {
	return &aggregator{
		alpha: alpha,
		rates: make(map[localityKey]float64),
	}
}

// ingest processes an LRS report and updates the EWMA rates.
func (a *aggregator) ingest(stats []*endpointv3.ClusterStats) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, cs := range stats {
		for _, ls := range cs.GetUpstreamLocalityStats() {
			key := toKey(ls.GetLocality())
			issued := float64(ls.GetTotalIssuedRequests())
			if prev, ok := a.rates[key]; ok {
				a.rates[key] = a.alpha*issued + (1-a.alpha)*prev
			} else {
				a.rates[key] = issued
			}
		}
	}
}

// fractions returns per-locality traffic fractions as basis points (0-10000).
func (a *aggregator) fractions() map[localityKey]uint32 {
	a.mu.Lock()
	defer a.mu.Unlock()

	total := 0.0
	for _, r := range a.rates {
		total += r
	}
	result := make(map[localityKey]uint32, len(a.rates))
	if total == 0 {
		return result
	}
	for k, r := range a.rates {
		bp := uint32(math.Round(10000 * r / total))
		result[k] = bp
	}
	return result
}

// lrsServer implements the LRS StreamLoadStats bidirectional stream.
type lrsServer struct {
	loadstatsv3.UnimplementedLoadReportingServiceServer
	agg             *aggregator
	clusters        []string
	reportInterval  time.Duration
}

func (s *lrsServer) StreamLoadStats(stream loadstatsv3.LoadReportingService_StreamLoadStatsServer) error {
	// First message from Envoy is the initial request with node info.
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	nodeID := "<unknown>"
	if req.GetNode() != nil {
		nodeID = req.GetNode().GetId()
	}
	log.Printf("[LRS] stream opened from node %s", nodeID)

	// Ingest any stats from the initial request.
	if len(req.GetClusterStats()) > 0 {
		s.agg.ingest(req.GetClusterStats())
		logStats(req.GetClusterStats())
	}

	// Send the initial response telling Envoy what clusters to report on.
	resp := &loadstatsv3.LoadStatsResponse{
		Clusters:              s.clusters,
		SendAllClusters:       len(s.clusters) == 0,
		LoadReportingInterval: durationpb.New(s.reportInterval),
	}
	if err := stream.Send(resp); err != nil {
		return err
	}
	log.Printf("[LRS] sent response: clusters=%v interval=%s send_all=%v",
		resp.GetClusters(), s.reportInterval, resp.GetSendAllClusters())

	// Receive loop: ingest reports and re-send response each cycle.
	for {
		req, err := stream.Recv()
		if err != nil {
			log.Printf("[LRS] stream from %s closed: %v", nodeID, err)
			return err
		}
		s.agg.ingest(req.GetClusterStats())
		logStats(req.GetClusterStats())

		// Log current fractions after each report.
		fracs := s.agg.fractions()
		log.Printf("[LRS] current fractions:")
		for k, bp := range fracs {
			log.Printf("  %s/%s/%s: %d bp (%.1f%%)", k.Region, k.Zone, k.SubZone, bp, float64(bp)/100)
		}

		// Re-send response to keep the stream alive.
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func logStats(clusterStats []*endpointv3.ClusterStats) {
	for _, cs := range clusterStats {
		log.Printf("[LRS] report cluster=%s", cs.GetClusterName())
		for _, ls := range cs.GetUpstreamLocalityStats() {
			loc := ls.GetLocality()
			log.Printf("  locality=%s/%s/%s issued=%d success=%d errors=%d",
				loc.GetRegion(), loc.GetZone(), loc.GetSubZone(),
				ls.GetTotalIssuedRequests(),
				ls.GetTotalSuccessfulRequests(),
				ls.GetTotalErrorRequests())
		}
	}
}

type edsEndpoint struct {
	Address string
	Port    uint32
}

// config holds the test server's static configuration.
type config struct {
	// Cluster names to track.
	localClusterName    string
	upstreamClusterName string

	// Static localities and their upstream endpoints.
	localities []struct {
		locality  localityKey
		endpoints []edsEndpoint
	}
}

// buildEDSSnapshot creates an xDS snapshot with the current fractions injected
// into the local cluster's EDS response via metadata.
func buildEDSSnapshot(version string, cfg *config, fracs map[localityKey]uint32) (*cachev3.Snapshot, error) {
	// Build the upstream cluster EDS (endpoints to route to).
	upstreamLocalities := make([]*endpointv3.LocalityLbEndpoints, 0, len(cfg.localities))
	for _, loc := range cfg.localities {
		endpoints := make([]*endpointv3.LbEndpoint, 0, len(loc.endpoints))
		for _, ep := range loc.endpoints {
			endpoints = append(endpoints, &endpointv3.LbEndpoint{
				HostIdentifier: &endpointv3.LbEndpoint_Endpoint{
					Endpoint: &endpointv3.Endpoint{
						Address: &corev3.Address{
							Address: &corev3.Address_SocketAddress{
								SocketAddress: &corev3.SocketAddress{
									Address: ep.Address,
									PortSpecifier: &corev3.SocketAddress_PortValue{
										PortValue: ep.Port,
									},
								},
							},
						},
					},
				},
			})
		}
		upstreamLocalities = append(upstreamLocalities, &endpointv3.LocalityLbEndpoints{
			Locality:    toProto(loc.locality),
			LbEndpoints: endpoints,
			LoadBalancingWeight: &wrapperspb.UInt32Value{Value: 1},
		})
	}

	upstreamCLA := &endpointv3.ClusterLoadAssignment{
		ClusterName: cfg.upstreamClusterName,
		Endpoints:   upstreamLocalities,
	}

	// Build the local cluster EDS with observed_traffic_fraction in metadata.
	// Since the proto field doesn't exist yet, we use locality metadata
	// under the key "envoy.lb.observed_traffic_fraction" (design doc Option 3).
	localLocalities := make([]*endpointv3.LocalityLbEndpoints, 0, len(cfg.localities))
	for _, loc := range cfg.localities {
		// Each locality in the local cluster gets a single endpoint representing
		// the local Envoy instance (or a placeholder).
		lbEp := &endpointv3.LbEndpoint{
			HostIdentifier: &endpointv3.LbEndpoint_Endpoint{
				Endpoint: &endpointv3.Endpoint{
					Address: &corev3.Address{
						Address: &corev3.Address_SocketAddress{
							SocketAddress: &corev3.SocketAddress{
								Address: "127.0.0.1",
								PortSpecifier: &corev3.SocketAddress_PortValue{
									PortValue: 0,
								},
							},
						},
					},
				},
			},
		}

		localityEp := &endpointv3.LocalityLbEndpoints{
			Locality:    toProto(loc.locality),
			LbEndpoints: []*endpointv3.LbEndpoint{lbEp},
			LoadBalancingWeight: &wrapperspb.UInt32Value{Value: 1},
		}

		// Inject observed_traffic_fraction via metadata if we have a fraction.
		if bp, ok := fracs[loc.locality]; ok && bp > 0 {
			localityEp.Metadata = &corev3.Metadata{
				FilterMetadata: map[string]*structpb.Struct{
					"envoy.lb": {
						Fields: map[string]*structpb.Value{
							"observed_traffic_fraction": structpb.NewNumberValue(float64(bp)),
						},
					},
				},
			}
		}

		localLocalities = append(localLocalities, localityEp)
	}

	localCLA := &endpointv3.ClusterLoadAssignment{
		ClusterName: cfg.localClusterName,
		Endpoints:   localLocalities,
	}

	return cachev3.NewSnapshot(version, map[resourcev3.Type][]cachetypes.Resource{
		resourcev3.EndpointType: {upstreamCLA, localCLA},
		resourcev3.ClusterType:  {buildCluster(cfg.upstreamClusterName)},
	})
}

// buildCluster creates a minimal CDS cluster that references the local cluster
// and uses EDS for endpoint discovery.
func buildCluster(upstreamName string) *clusterv3.Cluster {
	return &clusterv3.Cluster{
		Name: upstreamName,
		ClusterDiscoveryType: &clusterv3.Cluster_Type{
			Type: clusterv3.Cluster_EDS,
		},
		EdsClusterConfig: &clusterv3.Cluster_EdsClusterConfig{
			EdsConfig: &corev3.ConfigSource{
				ConfigSourceSpecifier: &corev3.ConfigSource_Ads{
					Ads: &corev3.AggregatedConfigSource{},
				},
			},
		},
		// Reference to the local cluster for zone-aware routing.
		CommonLbConfig: &clusterv3.Cluster_CommonLbConfig{
			LocalityConfigSpecifier: &clusterv3.Cluster_CommonLbConfig_ZoneAwareLbConfig_{
				ZoneAwareLbConfig: &clusterv3.Cluster_CommonLbConfig_ZoneAwareLbConfig{},
			},
		},
	}
}

// xdsCallbacks implements server.Callbacks for logging xDS stream events.
type xdsCallbacks struct{}

func (c *xdsCallbacks) OnStreamOpen(_ context.Context, id int64, typeURL string) error {
	log.Printf("[xDS] stream %d open for %s", id, typeURL)
	return nil
}

func (c *xdsCallbacks) OnStreamClosed(id int64, _ *corev3.Node) {
	log.Printf("[xDS] stream %d closed", id)
}

func (c *xdsCallbacks) OnStreamRequest(id int64, req *discoveryv3.DiscoveryRequest) error {
	log.Printf("[xDS] stream %d request: type=%s resources=%v", id, req.GetTypeUrl(), req.GetResourceNames())
	return nil
}

func (c *xdsCallbacks) OnStreamResponse(_ context.Context, id int64, _ *discoveryv3.DiscoveryRequest, resp *discoveryv3.DiscoveryResponse) {
	log.Printf("[xDS] stream %d response: type=%s version=%s nonce=%s resources=%d",
		id, resp.GetTypeUrl(), resp.GetVersionInfo(), resp.GetNonce(), len(resp.GetResources()))
}

func (c *xdsCallbacks) OnFetchRequest(_ context.Context, _ *discoveryv3.DiscoveryRequest) error {
	return nil
}

func (c *xdsCallbacks) OnFetchResponse(_ *discoveryv3.DiscoveryRequest, _ *discoveryv3.DiscoveryResponse) {}

func (c *xdsCallbacks) OnDeltaStreamOpen(_ context.Context, id int64, typeURL string) error {
	log.Printf("[xDS] delta stream %d open for %s", id, typeURL)
	return nil
}

func (c *xdsCallbacks) OnDeltaStreamClosed(id int64, _ *corev3.Node) {
	log.Printf("[xDS] delta stream %d closed", id)
}

func (c *xdsCallbacks) OnStreamDeltaRequest(id int64, _ *discoveryv3.DeltaDiscoveryRequest) error {
	return nil
}

func (c *xdsCallbacks) OnStreamDeltaResponse(id int64, _ *discoveryv3.DeltaDiscoveryRequest, _ *discoveryv3.DeltaDiscoveryResponse) {
}

func main() {
	var (
		xdsPort        = flag.Int("xds-port", 18000, "xDS/LRS gRPC server port")
		ewmaAlpha      = flag.Float64("ewma-alpha", 0.3, "EWMA smoothing factor (0-1)")
		reportInterval = flag.Duration("report-interval", 10*time.Second, "LRS reporting interval sent to Envoy")
		pushInterval   = flag.Duration("push-interval", 15*time.Second, "EDS fraction push interval")
		clusterName    = flag.String("cluster", "test-cluster", "upstream cluster name")
		localCluster   = flag.String("local-cluster", "local-cluster", "local (Envoy fleet) cluster name")
	)
	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Printf("LRS Test Server starting")
	log.Printf("  xDS port:        %d", *xdsPort)
	log.Printf("  EWMA alpha:      %.2f", *ewmaAlpha)
	log.Printf("  Report interval: %s", *reportInterval)
	log.Printf("  Push interval:   %s", *pushInterval)
	log.Printf("  Cluster:         %s", *clusterName)
	log.Printf("  Local cluster:   %s", *localCluster)

	// Static test topology: 3 zones with different endpoint counts.
	cfg := &config{
		localClusterName:    *localCluster,
		upstreamClusterName: *clusterName,
		localities: []struct {
			locality  localityKey
			endpoints []edsEndpoint
		}{
			{
				locality:  localityKey{Region: "us-east", Zone: "zone-a"},
				endpoints: []edsEndpoint{
					{Address: "10.0.1.1", Port: 8080},
					{Address: "10.0.1.2", Port: 8080},
					{Address: "10.0.1.3", Port: 8080},
				},
			},
			{
				locality:  localityKey{Region: "us-east", Zone: "zone-b"},
				endpoints: []edsEndpoint{
					{Address: "10.0.2.1", Port: 8080},
					{Address: "10.0.2.2", Port: 8080},
					{Address: "10.0.2.3", Port: 8080},
					{Address: "10.0.2.4", Port: 8080},
					{Address: "10.0.2.5", Port: 8080},
				},
			},
			{
				locality:  localityKey{Region: "us-east", Zone: "zone-c"},
				endpoints: []edsEndpoint{
					{Address: "10.0.3.1", Port: 8080},
					{Address: "10.0.3.2", Port: 8080},
				},
			},
		},
	}

	agg := newAggregator(*ewmaAlpha)

	// Set up xDS snapshot cache.
	cache := cachev3.NewSnapshotCache(true, cachev3.IDHash{}, nil)

	// Create initial snapshot with no fractions (host-count fallback).
	snap, err := buildEDSSnapshot("1", cfg, nil)
	if err != nil {
		log.Fatalf("failed to build initial snapshot: %v", err)
	}
	// Use a wildcard node ID so any connecting Envoy gets this snapshot.
	if err := cache.SetSnapshot(context.Background(), "test-node", snap); err != nil {
		log.Fatalf("failed to set initial snapshot: %v", err)
	}
	log.Printf("[xDS] initial snapshot set (version 1, no fractions)")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create xDS server.
	xdsSrv := serverv3.NewServer(ctx, cache, &xdsCallbacks{})

	// Create gRPC server with both xDS (ADS handles EDS) and LRS services.
	grpcServer := grpc.NewServer()
	discoveryv3.RegisterAggregatedDiscoveryServiceServer(grpcServer, xdsSrv)

	lrs := &lrsServer{
		agg:            agg,
		clusters:       []string{*clusterName},
		reportInterval: *reportInterval,
	}
	loadstatsv3.RegisterLoadReportingServiceServer(grpcServer, lrs)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *xdsPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Start gRPC server in background.
	go func() {
		log.Printf("[gRPC] listening on :%d", *xdsPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC serve error: %v", err)
		}
	}()

	// Periodic EDS push with updated fractions.
	snapshotVersion := 1
	go func() {
		ticker := time.NewTicker(*pushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fracs := agg.fractions()
				if len(fracs) == 0 {
					continue
				}
				snapshotVersion++
				ver := fmt.Sprintf("%d", snapshotVersion)
				snap, err := buildEDSSnapshot(ver, cfg, fracs)
				if err != nil {
					log.Printf("[xDS] ERROR building snapshot: %v", err)
					continue
				}
				if err := cache.SetSnapshot(ctx, "test-node", snap); err != nil {
					log.Printf("[xDS] ERROR setting snapshot: %v", err)
					continue
				}
				log.Printf("[xDS] pushed snapshot version %s with fractions:", ver)
				for k, bp := range fracs {
					log.Printf("  %s/%s: %d bp (%.1f%%)", k.Region, k.Zone, bp, float64(bp)/100)
				}
			}
		}
	}()

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("shutting down...")
	cancel()
	grpcServer.GracefulStop()
}

