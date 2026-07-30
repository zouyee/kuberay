package eventcollector

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/emicklei/go-restful/v3"
	"github.com/stretchr/testify/require"
)

type recordingWriter struct {
	mutex sync.Mutex
	files map[string][]byte
}

func newRecordingWriter() *recordingWriter {
	return &recordingWriter{files: make(map[string][]byte)}
}

func (w *recordingWriter) CreateDirectory(string) error {
	return nil
}

func (w *recordingWriter) WriteFile(file string, reader io.ReadSeeker) error {
	content, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.files[file] = content
	return nil
}

func (w *recordingWriter) eventsForSession(t *testing.T, sessionName string) []map[string]interface{} {
	t.Helper()

	w.mutex.Lock()
	defer w.mutex.Unlock()

	var events []map[string]interface{}
	for file, content := range w.files {
		if !strings.Contains(file, "/"+sessionName+"/") {
			continue
		}

		reader, err := gzip.NewReader(bytes.NewReader(content))
		require.NoError(t, err)
		decoded, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.NoError(t, reader.Close())

		var fileEvents []map[string]interface{}
		require.NoError(t, json.Unmarshal(decoded, &fileEvents))
		events = append(events, fileEvents...)
	}
	return events
}

func persistEventBatch(t *testing.T, collector *EventCollector, events []map[string]interface{}) {
	t.Helper()

	body, err := json.Marshal(events)
	require.NoError(t, err)

	httpRequest := httptest.NewRequest("POST", "/v1/events", bytes.NewReader(body))
	recorder := httptest.NewRecorder()
	collector.PersistEvents(restful.NewRequest(httpRequest), restful.NewResponse(recorder))
	require.Equal(t, 200, recorder.Code)
}

func nodeEvent(eventID, sessionName string) map[string]interface{} {
	return map[string]interface{}{
		"eventId":     eventID,
		"eventType":   "NODE_LIFECYCLE_EVENT",
		"sessionName": sessionName,
		"timestamp":   "2025-01-02T03:04:05Z",
	}
}

func eventIDs(events []map[string]interface{}) []string {
	ids := make([]string, 0, len(events))
	for _, event := range events {
		ids = append(ids, event["eventId"].(string))
	}
	return ids
}

func TestPersistEventsStoresTransitionEventsInTheirOwnSessions(t *testing.T) {
	writer := newRecordingWriter()
	collector := NewEventCollector(writer, "/events", "", "node-1", "cluster", "namespace", "session-old")

	persistEventBatch(t, collector, []map[string]interface{}{
		nodeEvent("old-event", "session-old"),
		nodeEvent("new-event", "session-new"),
	})
	collector.flushEvents()

	require.Equal(t, []string{"old-event"}, eventIDs(writer.eventsForSession(t, "session-old")))
	require.Equal(t, []string{"new-event"}, eventIDs(writer.eventsForSession(t, "session-new")))
}

func TestPersistEventsProcessesAllEventsAfterSessionTransition(t *testing.T) {
	writer := newRecordingWriter()
	collector := NewEventCollector(writer, "/events", "", "node-1", "cluster", "namespace", "session-old")

	persistEventBatch(t, collector, []map[string]interface{}{
		nodeEvent("old-event", "session-old"),
		nodeEvent("first-new-event", "session-new"),
		nodeEvent("second-new-event", "session-new"),
		nodeEvent("third-new-event", "session-new"),
	})
	collector.flushEvents()

	require.ElementsMatch(t,
		[]string{"first-new-event", "second-new-event", "third-new-event"},
		eventIDs(writer.eventsForSession(t, "session-new")),
	)
}
