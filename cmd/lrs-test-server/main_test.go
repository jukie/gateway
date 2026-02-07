package main

import (
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
)

func TestAggregatorBasicFractions(t *testing.T) {
	agg := newAggregator(1.0) // alpha=1 means no smoothing, just use latest value

	stats := []*endpointv3.ClusterStats{
		{
			ClusterName: "test-cluster",
			UpstreamLocalityStats: []*endpointv3.UpstreamLocalityStats{
				{
					Locality:            &corev3.Locality{Region: "us-east", Zone: "zone-a"},
					TotalIssuedRequests: 500,
				},
				{
					Locality:            &corev3.Locality{Region: "us-east", Zone: "zone-b"},
					TotalIssuedRequests: 350,
				},
				{
					Locality:            &corev3.Locality{Region: "us-east", Zone: "zone-c"},
					TotalIssuedRequests: 150,
				},
			},
		},
	}

	agg.ingest(stats)
	fracs := agg.fractions()

	// 500/(500+350+150) = 50% = 5000bp
	assertFraction(t, fracs, localityKey{Region: "us-east", Zone: "zone-a"}, 5000)
	// 350/1000 = 35% = 3500bp
	assertFraction(t, fracs, localityKey{Region: "us-east", Zone: "zone-b"}, 3500)
	// 150/1000 = 15% = 1500bp
	assertFraction(t, fracs, localityKey{Region: "us-east", Zone: "zone-c"}, 1500)
}

func TestAggregatorEWMASmoothing(t *testing.T) {
	agg := newAggregator(0.5) // alpha=0.5

	// First report: zone-a=1000, zone-b=1000
	agg.ingest([]*endpointv3.ClusterStats{
		{
			ClusterName: "test",
			UpstreamLocalityStats: []*endpointv3.UpstreamLocalityStats{
				{
					Locality:            &corev3.Locality{Region: "r", Zone: "a"},
					TotalIssuedRequests: 1000,
				},
				{
					Locality:            &corev3.Locality{Region: "r", Zone: "b"},
					TotalIssuedRequests: 1000,
				},
			},
		},
	})

	// Both should be 50/50.
	fracs := agg.fractions()
	assertFraction(t, fracs, localityKey{Region: "r", Zone: "a"}, 5000)
	assertFraction(t, fracs, localityKey{Region: "r", Zone: "b"}, 5000)

	// Second report: zone-a spikes to 3000, zone-b stays 1000
	// EWMA: zone-a = 0.5*3000 + 0.5*1000 = 2000
	//        zone-b = 0.5*1000 + 0.5*1000 = 1000
	// Fractions: a=2000/3000=6667bp, b=1000/3000=3333bp
	agg.ingest([]*endpointv3.ClusterStats{
		{
			ClusterName: "test",
			UpstreamLocalityStats: []*endpointv3.UpstreamLocalityStats{
				{
					Locality:            &corev3.Locality{Region: "r", Zone: "a"},
					TotalIssuedRequests: 3000,
				},
				{
					Locality:            &corev3.Locality{Region: "r", Zone: "b"},
					TotalIssuedRequests: 1000,
				},
			},
		},
	})

	fracs = agg.fractions()
	assertFractionRange(t, fracs, localityKey{Region: "r", Zone: "a"}, 6666, 6668)
	assertFractionRange(t, fracs, localityKey{Region: "r", Zone: "b"}, 3332, 3334)
}

func TestAggregatorEmptyFractions(t *testing.T) {
	agg := newAggregator(0.3)
	fracs := agg.fractions()
	if len(fracs) != 0 {
		t.Errorf("expected empty fractions, got %v", fracs)
	}
}

func TestBuildEDSSnapshotNoFractions(t *testing.T) {
	cfg := &config{
		localClusterName:    "local",
		upstreamClusterName: "upstream",
		localities: []struct {
			locality  localityKey
			endpoints []edsEndpoint
		}{
			{
				locality:  localityKey{Region: "r", Zone: "a"},
				endpoints: []edsEndpoint{{Address: "10.0.0.1", Port: 80}},
			},
		},
	}

	snap, err := buildEDSSnapshot("1", cfg, nil)
	if err != nil {
		t.Fatalf("buildEDSSnapshot failed: %v", err)
	}
	if snap == nil {
		t.Fatal("snapshot is nil")
	}
}

func TestBuildEDSSnapshotWithFractions(t *testing.T) {
	cfg := &config{
		localClusterName:    "local",
		upstreamClusterName: "upstream",
		localities: []struct {
			locality  localityKey
			endpoints []edsEndpoint
		}{
			{
				locality:  localityKey{Region: "r", Zone: "a"},
				endpoints: []edsEndpoint{{Address: "10.0.0.1", Port: 80}},
			},
			{
				locality:  localityKey{Region: "r", Zone: "b"},
				endpoints: []edsEndpoint{{Address: "10.0.0.2", Port: 80}},
			},
		},
	}

	fracs := map[localityKey]uint32{
		{Region: "r", Zone: "a"}: 6000,
		{Region: "r", Zone: "b"}: 4000,
	}

	snap, err := buildEDSSnapshot("2", cfg, fracs)
	if err != nil {
		t.Fatalf("buildEDSSnapshot with fractions failed: %v", err)
	}
	if snap == nil {
		t.Fatal("snapshot is nil")
	}
}

func TestLocalityKeyRoundTrip(t *testing.T) {
	key := localityKey{Region: "us-east", Zone: "zone-a", SubZone: "sub-1"}
	proto := toProto(key)
	back := toKey(proto)
	if back != key {
		t.Errorf("roundtrip failed: got %v, want %v", back, key)
	}
}

func TestToKeyNil(t *testing.T) {
	key := toKey(nil)
	if key != (localityKey{}) {
		t.Errorf("toKey(nil) should be zero value, got %v", key)
	}
}

func assertFraction(t *testing.T, fracs map[localityKey]uint32, key localityKey, expected uint32) {
	t.Helper()
	got, ok := fracs[key]
	if !ok {
		t.Errorf("missing fraction for %v", key)
		return
	}
	if got != expected {
		t.Errorf("fraction for %v: got %d, want %d", key, got, expected)
	}
}

func assertFractionRange(t *testing.T, fracs map[localityKey]uint32, key localityKey, min, max uint32) {
	t.Helper()
	got, ok := fracs[key]
	if !ok {
		t.Errorf("missing fraction for %v", key)
		return
	}
	if got < min || got > max {
		t.Errorf("fraction for %v: got %d, want [%d, %d]", key, got, min, max)
	}
}
