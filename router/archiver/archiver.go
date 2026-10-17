package archiver

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/pterodactyl/wings/internal/ufs"
	"github.com/pterodactyl/wings/server"
	"github.com/pterodactyl/wings/server/filesystem"
)

// TaskType represents the type of archive operation
type TaskType string

const (
	TaskCompress   TaskType = "compress"
	TaskDecompress TaskType = "decompress"
)

// TaskStatus represents the current status of an archive task
type TaskStatus string

const (
	StatusPending    TaskStatus = "pending"
	StatusProcessing TaskStatus = "processing"
	StatusCompleted  TaskStatus = "completed"
	StatusFailed     TaskStatus = "failed"
)

// maxActiveTasksPerServer is the number of archive tasks a single server may
// have pending or processing at the same time.
const maxActiveTasksPerServer = 2

// ErrTooManyTasks is returned when a server already has the maximum number of
// archive tasks running.
var ErrTooManyTasks = errors.New("archiver: too many archive tasks are already running for this server")

// Task represents an archive operation (compress or decompress)
type Task struct {
	Identifier string     `json:"identifier"`
	ServerID   string     `json:"server_id"`
	Type       TaskType   `json:"type"`
	Status     TaskStatus `json:"status"`
	RootPath   string     `json:"root_path"`
	Files      []string   `json:"files,omitempty"`       // For compress
	File       string     `json:"file,omitempty"`        // For decompress
	ResultFile string     `json:"result_file,omitempty"` // Resulting archive path for compress
	Error      string     `json:"error,omitempty"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt time.Time  `json:"finished_at,omitempty"`

	server *server.Server
	mu     sync.RWMutex
}

// Manager handles all archive tasks
type Manager struct {
	mu    sync.RWMutex
	tasks map[string]*Task
	// Map server ID to task IDs for quick lookup
	serverTasks map[string][]string
}

var instance = &Manager{
	tasks:       make(map[string]*Task),
	serverTasks: make(map[string][]string),
}

// NewCompressTask creates a new compression task
func NewCompressTask(s *server.Server, rootPath string, files []string) (*Task, error) {
	task := &Task{
		Identifier: uuid.New().String(),
		ServerID:   s.ID(),
		Type:       TaskCompress,
		Status:     StatusPending,
		RootPath:   rootPath,
		Files:      files,
		StartedAt:  time.Now(),
		server:     s,
	}

	if err := instance.add(task); err != nil {
		return nil, err
	}
	return task, nil
}

// NewDecompressTask creates a new decompression task
func NewDecompressTask(s *server.Server, rootPath, file string) (*Task, error) {
	task := &Task{
		Identifier: uuid.New().String(),
		ServerID:   s.ID(),
		Type:       TaskDecompress,
		Status:     StatusPending,
		RootPath:   rootPath,
		File:       file,
		StartedAt:  time.Now(),
		server:     s,
	}

	if err := instance.add(task); err != nil {
		return nil, err
	}
	return task, nil
}

// add registers a task, unless its server already has the maximum number of
// tasks pending or processing.
func (m *Manager) add(task *Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var active int
	for _, id := range m.serverTasks[task.ServerID] {
		if t, ok := m.tasks[id]; ok {
			if st := t.GetStatus(); st == StatusPending || st == StatusProcessing {
				active++
			}
		}
	}
	if active >= maxActiveTasksPerServer {
		return ErrTooManyTasks
	}

	m.tasks[task.Identifier] = task
	m.serverTasks[task.ServerID] = append(m.serverTasks[task.ServerID], task.Identifier)
	return nil
}

// Execute runs the archive task
func (t *Task) Execute() {
	t.mu.Lock()
	t.Status = StatusProcessing
	t.mu.Unlock()

	// Publish start event
	t.server.Events().Publish(server.ArchiveStartedEvent, map[string]interface{}{
		"identifier": t.Identifier,
		"type":       t.Type,
		"root_path":  t.RootPath,
	})

	var err error
	var result *filesystem.Stat

	if t.Type == TaskCompress {
		var fileInfo ufs.FileInfo
		fileInfo, err = t.server.Filesystem().CompressFiles(t.RootPath, t.Files)
		if err == nil && fileInfo != nil {
			t.mu.Lock()
			t.ResultFile = fileInfo.Name()
			t.mu.Unlock()
			result = &filesystem.Stat{
				FileInfo: fileInfo,
				Mimetype: "application/tar+gzip",
			}
		}
	} else {
		// Use the server context so that decompression stops if the server is deleted.
		err = t.server.Filesystem().DecompressFile(t.server.Context(), t.RootPath, t.File)
	}

	t.mu.Lock()
	t.FinishedAt = time.Now()

	if err != nil {
		t.Status = StatusFailed
		t.Error = err.Error()
		t.mu.Unlock()

		// Publish failed event
		t.server.Events().Publish(server.ArchiveFailedEvent, map[string]interface{}{
			"identifier": t.Identifier,
			"type":       t.Type,
			"error":      err.Error(),
		})
	} else {
		t.Status = StatusCompleted
		t.mu.Unlock()

		// Publish completed event
		eventData := map[string]interface{}{
			"identifier": t.Identifier,
			"type":       t.Type,
		}
		if result != nil {
			eventData["result"] = result
		}
		t.server.Events().Publish(server.ArchiveCompletedEvent, eventData)
	}

	// Clean up task after some time (keep for 5 minutes for status queries)
	go func() {
		time.Sleep(5 * time.Minute)
		instance.mu.Lock()
		delete(instance.tasks, t.Identifier)
		// Remove from server tasks list
		serverTasks := instance.serverTasks[t.ServerID]
		for i, id := range serverTasks {
			if id == t.Identifier {
				instance.serverTasks[t.ServerID] = append(serverTasks[:i], serverTasks[i+1:]...)
				break
			}
		}
		instance.mu.Unlock()
	}()
}

// GetTask returns a task by its identifier
func GetTask(id string) *Task {
	instance.mu.RLock()
	defer instance.mu.RUnlock()
	return instance.tasks[id]
}

// GetServerTasks returns all tasks for a server
func GetServerTasks(serverID string) []*Task {
	instance.mu.RLock()
	defer instance.mu.RUnlock()

	taskIDs := instance.serverTasks[serverID]
	tasks := make([]*Task, 0, len(taskIDs))
	for _, id := range taskIDs {
		if task, ok := instance.tasks[id]; ok {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

// BelongsTo checks if the task belongs to the given server
func (t *Task) BelongsTo(s *server.Server) bool {
	return t.ServerID == s.ID()
}

// GetStatus returns the current status of the task
func (t *Task) GetStatus() TaskStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Status
}
