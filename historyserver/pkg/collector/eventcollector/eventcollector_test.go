package eventcollector

import (
	"compress/gzip"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type memoryStorageWriter struct {
	mu    sync.Mutex
	files map[string][]byte
}

func (w *memoryStorageWriter) CreateDirectory(string) error {
	return nil
}

func (w *memoryStorageWriter) WriteFile(name string, reader io.ReadSeeker) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.files[name] = data
	return nil
}

func (w *memoryStorageWriter) fileNames() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	names := make([]string, 0, len(w.files))
	for name := range w.files {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (w *memoryStorageWriter) uncompressedContents(t *testing.T) []string {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	contents := make([]string, 0, len(w.files))
	for _, data := range w.files {
		reader, err := gzip.NewReader(strings.NewReader(string(data)))
		require.NoError(t, err)
		content, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		contents = append(contents, string(content))
	}
	sort.Strings(contents)
	return contents
}

func TestSameHourFlushesRetainBothObjects(t *testing.T) {
	const hour = "2024-01-02-03"

	tests := []struct {
		name       string
		directory  string
		flushBatch func(*EventCollector, []Event) error
	}{
		{
			name:      "node events",
			directory: "node_events",
			flushBatch: func(collector *EventCollector, events []Event) error {
				return collector.flushNodeEventsForHour(hour, events)
			},
		},
		{
			name:      "job events",
			directory: path.Join("job_events", "job-1"),
			flushBatch: func(collector *EventCollector, events []Event) error {
				return collector.flushJobEventsForHour("job-1", hour, events)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer := &memoryStorageWriter{files: make(map[string][]byte)}
			collector := NewEventCollector(writer, "root", "", "node-1", "cluster", "namespace", "session")
			first := []Event{{Data: map[string]interface{}{"eventId": "first"}}}
			second := []Event{{Data: map[string]interface{}{"eventId": "second"}}}

			require.NoError(t, tt.flushBatch(collector, first))
			require.NoError(t, tt.flushBatch(collector, second))

			names := writer.fileNames()
			require.Len(t, names, 2)
			for _, name := range names {
				require.True(t, strings.HasPrefix(name, path.Join("root", "cluster_namespace", "session", tt.directory, "node-1-"+hour+"-")))
				require.True(t, strings.HasSuffix(name, ".gz"))
			}
			require.Equal(t,
				[]string{`[{"eventId":"first"}]`, `[{"eventId":"second"}]`},
				writer.uncompressedContents(t),
			)
		})
	}
}
