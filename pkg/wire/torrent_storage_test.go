package wire

import (
	"testing"
	"time"
)

func TestGetStalledTorrentsUsesLastActivity(t *testing.T) {
	now := time.Now()
	ts := &TorrentStorage{
		torrents: Torrents{
			"stalled": {
				Hash:         "stalled",
				DebridID:     "db-1",
				State:        "downloading",
				AmountLeft:   100,
				Dlspeed:      0,
				LastActivity: now.Add(-15 * time.Minute).Unix(),
				AddedOn:      now.Add(-30 * time.Minute).Unix(),
				Progress:     0.25,
			},
			"active": {
				Hash:         "active",
				DebridID:     "db-2",
				State:        "downloading",
				AmountLeft:   100,
				Dlspeed:      1024,
				LastActivity: now.Add(-15 * time.Second).Unix(),
				AddedOn:      now.Add(-30 * time.Minute).Unix(),
				Progress:     0.25,
			},
			"completed": {
				Hash:         "completed",
				DebridID:     "db-3",
				State:        "pausedUP",
				AmountLeft:   0,
				Dlspeed:      0,
				LastActivity: now.Add(-15 * time.Minute).Unix(),
				AddedOn:      now.Add(-30 * time.Minute).Unix(),
				Progress:     1,
			},
		},
	}

	stalled := ts.GetStalledTorrents(10 * time.Minute)
	if len(stalled) != 1 {
		t.Fatalf("expected 1 stalled torrent, got %d", len(stalled))
	}
	if stalled[0].Hash != "stalled" {
		t.Fatalf("expected stalled torrent to be selected, got %q", stalled[0].Hash)
	}
}

func TestGetStalledTorrentsFallsBackToAddedOn(t *testing.T) {
	now := time.Now()
	ts := &TorrentStorage{
		torrents: Torrents{
			"legacy": {
				Hash:       "legacy",
				DebridID:   "db-1",
				State:      "downloading",
				AmountLeft: 100,
				Dlspeed:    0,
				AddedOn:    now.Add(-20 * time.Minute).Unix(),
				Progress:   0.5,
			},
		},
	}

	stalled := ts.GetStalledTorrents(10 * time.Minute)
	if len(stalled) != 1 {
		t.Fatalf("expected legacy torrent to be considered stalled, got %d matches", len(stalled))
	}
}
