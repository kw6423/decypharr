package wire

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/sirrobot01/decypharr/internal/logger"
)

func keyPair(hash, category string) string {
	return fmt.Sprintf("%s|%s", hash, category)
}

type Torrents = map[string]*Torrent

type TorrentStorage struct {
	torrents Torrents
	mu       sync.RWMutex
	filename string // Added to store the filename for persistence
}

func loadTorrentsFromJSON(filename string) (Torrents, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	torrents := make(Torrents)
	if err := json.Unmarshal(data, &torrents); err != nil {
		return nil, err
	}
	return torrents, nil
}

func newTorrentStorage(filename string) *TorrentStorage {
	// Open the JSON file and read the data
	torrents, err := loadTorrentsFromJSON(filename)
	if err != nil {
		torrents = make(Torrents)
	}
	// Create a new Storage
	return &TorrentStorage{
		torrents: torrents,
		filename: filename,
	}
}

func (ts *TorrentStorage) Add(torrent *Torrent) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.torrents[keyPair(torrent.Hash, torrent.Category)] = torrent
	go func() {
		err := ts.saveToFile()
		if err != nil {
			fmt.Println(err)
		}
	}()
}

func (ts *TorrentStorage) AddOrUpdate(torrent *Torrent) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.torrents[keyPair(torrent.Hash, torrent.Category)] = torrent
	go func() {
		err := ts.saveToFile()
		if err != nil {
			fmt.Println(err)
		}
	}()
}

func (ts *TorrentStorage) Get(hash, category string) *Torrent {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	torrent, exists := ts.torrents[keyPair(hash, category)]
	if !exists && category == "" {
		// Try to find the torrent without knowing the category
		for _, t := range ts.torrents {
			if t.Hash == hash {
				return t
			}
		}
	}
	return torrent
}

func (ts *TorrentStorage) GetAll(category string, filter string, hashes []string) []*Torrent {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	torrents := make([]*Torrent, 0)
	for _, torrent := range ts.torrents {
		if category != "" && torrent.Category != category {
			continue
		}
		if filter != "" && torrent.State != filter {
			continue
		}
		torrents = append(torrents, torrent)
	}

	if len(hashes) > 0 {
		filtered := make([]*Torrent, 0)
		for _, hash := range hashes {
			for _, torrent := range torrents {
				if torrent.Hash == hash {
					filtered = append(filtered, torrent)
				}
			}
		}
		torrents = filtered
	}
	return torrents
}

func (ts *TorrentStorage) GetAllSorted(category string, filter string, hashes []string, sortBy string, ascending bool) []*Torrent {
	torrents := ts.GetAll(category, filter, hashes)
	if sortBy != "" {
		sort.Slice(torrents, func(i, j int) bool {
			// If ascending is false, swap i and j to get descending order
			if !ascending {
				i, j = j, i
			}

			switch sortBy {
			case "name":
				return torrents[i].Name < torrents[j].Name
			case "size":
				return torrents[i].Size < torrents[j].Size
			case "added_on":
				return torrents[i].AddedOn < torrents[j].AddedOn
			case "completed":
				return torrents[i].Completed < torrents[j].Completed
			case "progress":
				return torrents[i].Progress < torrents[j].Progress
			case "state":
				return torrents[i].State < torrents[j].State
			case "category":
				return torrents[i].Category < torrents[j].Category
			case "dlspeed":
				return torrents[i].Dlspeed < torrents[j].Dlspeed
			case "upspeed":
				return torrents[i].Upspeed < torrents[j].Upspeed
			case "ratio":
				return torrents[i].Ratio < torrents[j].Ratio
			default:
				// Default sort by added_on
				return torrents[i].AddedOn < torrents[j].AddedOn
			}
		})
	}
	return torrents
}

func (ts *TorrentStorage) Update(torrent *Torrent) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.torrents[keyPair(torrent.Hash, torrent.Category)] = torrent
	go func() {
		err := ts.saveToFile()
		if err != nil {
			fmt.Println(err)
		}
	}()
}

func (ts *TorrentStorage) Delete(hash, category string, removeFromDebrid bool) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	wireStore := Get()
	for key, torrent := range ts.torrents {
		if torrent == nil {
			continue
		}
		if torrent.Hash == hash && (category == "" || torrent.Category == category) {
			if torrent.State == "queued" && torrent.ID != "" {
				// Remove the torrent from the import queue if it exists
				wireStore.importsQueue.Delete(torrent.ID)
			}
			if removeFromDebrid && torrent.DebridID != "" && torrent.Debrid != "" {
				dbClient := wireStore.debrid.Client(torrent.Debrid)
				if dbClient != nil {
					_ = dbClient.DeleteTorrent(torrent.DebridID)
				}
			}
			delete(ts.torrents, key)

			// Delete the torrent folder
			if torrent.ContentPath != "" {
				err := os.RemoveAll(torrent.ContentPath)
				if err != nil {
					return
				}
			}
			break
		}
	}
	go func() {
		err := ts.saveToFile()
		if err != nil {
			fmt.Println(err)
		}
	}()
}

func (ts *TorrentStorage) DeleteMultiple(hashes []string, removeFromDebrid bool) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	toDelete := make(map[string]string)

	st := Get()

	for _, hash := range hashes {
		for key, torrent := range ts.torrents {
			if torrent == nil {
				continue
			}
			if torrent.Hash == hash {
				if torrent.State == "queued" && torrent.ID != "" {
					// Remove the torrent from the import queue if it exists
					st.importsQueue.Delete(torrent.ID)
				}
				if removeFromDebrid && torrent.DebridID != "" && torrent.Debrid != "" {
					toDelete[torrent.DebridID] = torrent.Debrid
				}
				delete(ts.torrents, key)
				if torrent.ContentPath != "" {
					err := os.RemoveAll(torrent.ContentPath)
					if err != nil {
						return
					}
				}
				break
			}
		}
	}
	go func() {
		err := ts.saveToFile()
		if err != nil {
			fmt.Println(err)
		}
	}()

	clients := st.debrid.Clients()

	go func() {
		for id, debrid := range toDelete {
			dbClient, ok := clients[debrid]
			if !ok {
				continue
			}
			err := dbClient.DeleteTorrent(id)
			if err != nil {
				fmt.Println(err)
			}
		}
	}()
}

func (ts *TorrentStorage) Save() error {
	return ts.saveToFile()
}

// saveToFile is a helper function to write the current state to the JSON file
func (ts *TorrentStorage) saveToFile() error {
	ts.mu.RLock()
	data, err := json.MarshalIndent(ts.torrents, "", "  ")
	ts.mu.RUnlock()
	if err != nil {
		return err
	}
	return os.WriteFile(ts.filename, data, 0644)
}

func (ts *TorrentStorage) Reset() {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.torrents = make(Torrents)
}

// GetStalledTorrents returns a list of torrents that are stalled.
// A torrent is considered stalled if it is still downloading, has bytes left,
// has no download speed, and has had no activity for longer than removeAfter.
func (ts *TorrentStorage) GetStalledTorrents(removeAfter time.Duration) []*Torrent {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	stalled := make([]*Torrent, 0)
	currentTime := time.Now()
	log := logger.Default()
	for _, torrent := range ts.torrents {
		event := log.Trace().
			Str("torrent", torrent.Name).
			Str("hash", torrent.Hash).
			Str("state", torrent.State).
			Str("debrid_id", torrent.DebridID).
			Int64("amount_left", torrent.AmountLeft).
			Int64("dlspeed", torrent.Dlspeed).
			Float64("progress", torrent.Progress).
			Int64("last_activity", torrent.LastActivity).
			Int64("added_on", torrent.AddedOn)

		if torrent.DebridID == "" {
			event.Msg("Skipping stalled check: torrent has no debrid id")
			continue
		}
		if torrent.State != "downloading" {
			event.Msg("Skipping stalled check: torrent is not downloading")
			continue
		}
		if torrent.AmountLeft <= 0 {
			event.Msg("Skipping stalled check: torrent has no remaining bytes")
			continue
		}
		if torrent.Dlspeed > 0 {
			event.Msg("Skipping stalled check: torrent is still downloading")
			continue
		}

		lastActivity := torrent.LastActivity
		if lastActivity == 0 {
			lastActivity = torrent.AddedOn
			event = event.Int64("effective_last_activity", lastActivity)
			if lastActivity == 0 {
				event.Msg("Skipping stalled check: torrent has no activity timestamp")
				continue
			}
			event.Msg("Using added_on as fallback last activity for stalled check")
		}
		if lastActivity == 0 {
			event.Msg("Skipping stalled check: torrent has no activity timestamp")
			continue
		}

		idleFor := currentTime.Sub(time.Unix(lastActivity, 0))
		if idleFor > removeAfter {
			log.Debug().
				Str("torrent", torrent.Name).
				Str("hash", torrent.Hash).
				Dur("idle_for", idleFor).
				Dur("remove_after", removeAfter).
				Msg("Torrent marked as stalled")
			stalled = append(stalled, torrent)
			continue
		}
		log.Trace().
			Str("torrent", torrent.Name).
			Str("hash", torrent.Hash).
			Dur("idle_for", idleFor).
			Dur("remove_after", removeAfter).
			Msg("Skipping stalled check: torrent has not exceeded inactivity threshold")
	}
	return stalled
}
