package router

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"emperror.dev/errors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/router/middleware"
)

const (
	uploadBufferSize = 1024 * 1024

	// minUploadChunkSize and maxUploadChunkSize bound the chunk size a client may
	// request, which keeps the number of chunks tracked per session reasonable.
	minUploadChunkSize = 64 * 1024
	maxUploadChunkSize = 512 * 1024 * 1024
)

// UploadSession tracks the state of a chunked file upload.
type UploadSession struct {
	ID             string
	ServerID       string
	FilePath       string
	FileSize       int64
	ChunkSize      int64
	TotalChunks    int
	ReceivedChunks map[int]bool
	CreatedAt      time.Time
	finalizing     bool
	mu             sync.RWMutex
}

// ChunkManager manages concurrent upload sessions across multiple servers.
type ChunkManager struct {
	sessions map[string]*UploadSession
	mu       sync.RWMutex
}

var globalChunkManager = &ChunkManager{
	sessions: make(map[string]*UploadSession),
}

func (cm *ChunkManager) CreateSession(serverID, filePath string, fileSize, chunkSize int64) (*UploadSession, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	activeSessions := 0
	for _, session := range cm.sessions {
		if session.ServerID == serverID {
			activeSessions++
		}
	}

	maxConcurrent := config.Get().ChunkedUpload.MaxConcurrentUploads
	if activeSessions >= maxConcurrent {
		return nil, errors.New(fmt.Sprintf("maximum concurrent uploads (%d) reached for this server", maxConcurrent))
	}

	totalChunks := int((fileSize + chunkSize - 1) / chunkSize)

	session := &UploadSession{
		ID:             uuid.New().String(),
		ServerID:       serverID,
		FilePath:       filePath,
		FileSize:       fileSize,
		ChunkSize:      chunkSize,
		TotalChunks:    totalChunks,
		ReceivedChunks: make(map[int]bool),
		CreatedAt:      time.Now(),
	}

	cm.sessions[session.ID] = session
	return session, nil
}

func (cm *ChunkManager) GetSession(uploadID string) (*UploadSession, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	session, exists := cm.sessions[uploadID]
	return session, exists
}

func (cm *ChunkManager) DeleteSession(uploadID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	delete(cm.sessions, uploadID)
}

// GetReservedSpace calculates the total space reserved by all active upload sessions.
// This includes both received chunks and space reserved for pending chunks.
func (cm *ChunkManager) GetReservedSpace() int64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var totalReserved int64
	for _, session := range cm.sessions {
		totalReserved += session.FileSize
	}
	return totalReserved
}

// HasTmpSpaceFor checks if there's enough space in the temp directory to store
// chunks for a new upload of the given size. It considers both the physical
// available space and space already reserved by active upload sessions.
func (cm *ChunkManager) HasTmpSpaceFor(fileSize int64) error {
	tmpDir := os.TempDir()

	var stat syscall.Statfs_t
	if err := syscall.Statfs(tmpDir, &stat); err != nil {
		return errors.Wrap(err, "failed to check temp directory space")
	}

	availableSpace := int64(stat.Bavail) * int64(stat.Bsize)

	reservedSpace := cm.GetReservedSpace()

	effectiveAvailable := availableSpace - reservedSpace

	// Add a 10% safety margin to avoid filling the disk completely
	requiredSpace := int64(float64(fileSize) * 1.1)

	if effectiveAvailable < requiredSpace {
		availableMB := effectiveAvailable / (1024 * 1024)
		requiredMB := requiredSpace / (1024 * 1024)
		return errors.New(fmt.Sprintf(
			"insufficient temp directory space: %d MB available, %d MB required (including safety margin)",
			availableMB, requiredMB,
		))
	}

	return nil
}

func (cm *ChunkManager) CleanupExpiredSessions() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	expirationHours := time.Duration(config.Get().ChunkedUpload.ChunkExpirationHours) * time.Hour
	now := time.Now()

	for id, session := range cm.sessions {
		if now.Sub(session.CreatedAt) > expirationHours {
			tmpDir := filepath.Join(os.TempDir(), "wings-uploads", id)
			os.RemoveAll(tmpDir)
			delete(cm.sessions, id)
		}
	}
}

func (s *UploadSession) MarkChunkReceived(chunkIndex int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ReceivedChunks[chunkIndex] = true
}

func (s *UploadSession) IsChunkReceived(chunkIndex int) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ReceivedChunks[chunkIndex]
}

func (s *UploadSession) AllChunksReceived() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.ReceivedChunks) == s.TotalChunks
}

// ExpectedChunkSize returns the exact number of bytes the chunk at the given
// index must contain. Every chunk is ChunkSize bytes except the last one, which
// holds whatever remains of the file.
func (s *UploadSession) ExpectedChunkSize(chunkIndex int) int64 {
	if chunkIndex == s.TotalChunks-1 {
		return s.FileSize - s.ChunkSize*int64(s.TotalChunks-1)
	}
	return s.ChunkSize
}

// BeginFinalize marks the session as being assembled, returning false if it
// already is so that the same upload cannot be finalized twice at once.
func (s *UploadSession) BeginFinalize() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finalizing {
		return false
	}
	s.finalizing = true
	return true
}

func (s *UploadSession) GetProgress() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.TotalChunks == 0 {
		return 0
	}
	return float64(len(s.ReceivedChunks)) / float64(s.TotalChunks) * 100
}

// postInitChunkedUpload initializes a new chunked upload session for large files.
// The client specifies the file path, total size, and preferred chunk size.
// Returns a unique upload ID and calculated total chunks needed.
func postInitChunkedUpload(c *gin.Context) {
	s := middleware.ExtractServer(c)

	var data struct {
		Path      string `json:"path" binding:"required"`
		FileSize  int64  `json:"file_size" binding:"required"`
		ChunkSize int64  `json:"chunk_size" binding:"required"`
	}

	if err := c.BindJSON(&data); err != nil {
		return
	}

	// Resolve the path against the server root so that it can never point above it.
	cleanPath := strings.TrimLeft(filepath.Clean("/"+data.Path), "/")
	if cleanPath == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "A file path is required",
		})
		return
	}
	if err := s.Filesystem().IsIgnored(cleanPath); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	if data.FileSize <= 0 {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "File size must be greater than zero",
		})
		return
	}

	if data.ChunkSize < min(minUploadChunkSize, data.FileSize) || data.ChunkSize > maxUploadChunkSize {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("Chunk size must be between %d and %d bytes", minUploadChunkSize, maxUploadChunkSize),
		})
		return
	}

	maxFileSize := config.Get().ChunkedUpload.MaxFileSize * 1024 * 1024
	if data.FileSize > maxFileSize {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("File size exceeds maximum allowed size of %d MB", config.Get().ChunkedUpload.MaxFileSize),
		})
		return
	}

	if err := s.Filesystem().HasSpaceFor(data.FileSize); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	// Check if temp directory has enough space for the upload chunks
	if err := globalChunkManager.HasTmpSpaceFor(data.FileSize); err != nil {
		c.AbortWithStatusJSON(http.StatusInsufficientStorage, gin.H{
			"error": err.Error(),
		})
		return
	}

	session, err := globalChunkManager.CreateSession(s.ID(), cleanPath, data.FileSize, data.ChunkSize)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{
			"error": err.Error(),
		})
		return
	}

	tmpDir := filepath.Join(os.TempDir(), "wings-uploads", session.ID)
	if err := os.MkdirAll(tmpDir, 0o700); err != nil {
		globalChunkManager.DeleteSession(session.ID)
		middleware.CaptureAndAbort(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"upload_id":    session.ID,
		"total_chunks": session.TotalChunks,
		"chunk_size":   session.ChunkSize,
	})
}

// postUploadChunk receives a single chunk of data for an active upload session.
// Chunks can be uploaded in any order and duplicates are safely ignored.
// Uses buffered I/O for optimal performance on high-bandwidth networks.
func postUploadChunk(c *gin.Context) {
	s := middleware.ExtractServer(c)

	uploadID := c.GetHeader("X-Upload-Id")
	chunkIndexStr := c.GetHeader("X-Chunk-Index")
	totalChunksStr := c.GetHeader("X-Total-Chunks")

	if uploadID == "" || chunkIndexStr == "" || totalChunksStr == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "Missing required headers: X-Upload-Id, X-Chunk-Index, X-Total-Chunks",
		})
		return
	}

	chunkIndex, err := strconv.Atoi(chunkIndexStr)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "Invalid chunk index",
		})
		return
	}

	totalChunks, err := strconv.Atoi(totalChunksStr)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "Invalid total chunks",
		})
		return
	}

	session, exists := globalChunkManager.GetSession(uploadID)
	if !exists {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
			"error": "Upload session not found",
		})
		return
	}

	if session.ServerID != s.ID() {
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
			"error": "Upload session does not belong to this server",
		})
		return
	}

	if totalChunks != session.TotalChunks {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "Total chunks mismatch",
		})
		return
	}

	if chunkIndex < 0 || chunkIndex >= session.TotalChunks {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "Invalid chunk index",
		})
		return
	}

	if session.IsChunkReceived(chunkIndex) {
		c.JSON(http.StatusOK, gin.H{
			"chunk_index":    chunkIndex,
			"status":         "already_received",
			"bytes_received": session.ChunkSize,
			"total_bytes":    session.FileSize,
			"progress":       session.GetProgress(),
		})
		return
	}

	tmpDir := filepath.Join(os.TempDir(), "wings-uploads", session.ID)
	chunkPath := filepath.Join(tmpDir, fmt.Sprintf("chunk_%d", chunkIndex))

	// Write the chunk to a temporary file and only move it into place once it is
	// complete, so a chunk that is being uploaded twice at the same time is never
	// read half written.
	file, err := os.CreateTemp(tmpDir, fmt.Sprintf("chunk_%d.*.part", chunkIndex))
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}
	partPath := file.Name()
	defer os.Remove(partPath)
	defer file.Close()

	// Never read more than the chunk is expected to contain, so that a client cannot
	// use a single chunk to fill the temporary directory.
	expected := session.ExpectedChunkSize(chunkIndex)
	bufferedWriter := bufio.NewWriterSize(file, uploadBufferSize)
	written, err := io.Copy(bufferedWriter, io.LimitReader(c.Request.Body, expected+1))
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	if written != expected {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("Chunk %d must contain exactly %d bytes", chunkIndex, expected),
		})
		return
	}

	if err := bufferedWriter.Flush(); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	if err := file.Close(); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	if err := os.Rename(partPath, chunkPath); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	session.MarkChunkReceived(chunkIndex)

	c.JSON(http.StatusOK, gin.H{
		"chunk_index":    chunkIndex,
		"status":         "received",
		"bytes_received": written,
		"total_bytes":    session.FileSize,
		"progress":       session.GetProgress(),
	})
}

// postFinalizeUpload assembles all received chunks into the final file.
// Verifies all chunks are present, reassembles them sequentially, computes
// a SHA256 checksum, and sets proper file ownership before cleanup.
func postFinalizeUpload(c *gin.Context) {
	s := middleware.ExtractServer(c)

	var data struct {
		UploadID string `json:"upload_id" binding:"required"`
	}

	if err := c.BindJSON(&data); err != nil {
		return
	}

	session, exists := globalChunkManager.GetSession(data.UploadID)
	if !exists {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
			"error": "Upload session not found",
		})
		return
	}

	if session.ServerID != s.ID() {
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
			"error": "Upload session does not belong to this server",
		})
		return
	}

	if !session.AllChunksReceived() {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error":           "Not all chunks have been received",
			"received_chunks": len(session.ReceivedChunks),
			"total_chunks":    session.TotalChunks,
		})
		return
	}

	if !session.BeginFinalize() {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{
			"error": "Upload session is already being finalized",
		})
		return
	}

	tmpDir := filepath.Join(os.TempDir(), "wings-uploads", session.ID)
	defer func() {
		os.RemoveAll(tmpDir)
		globalChunkManager.DeleteSession(session.ID)
	}()

	// Assemble the chunks through the server filesystem, which keeps the file inside
	// the server root, reserves the space against the disk limit and chowns any
	// directories it has to create.
	hash := sha256.New()
	chunks := &chunkReader{dir: tmpDir, total: session.TotalChunks}
	defer chunks.Close()
	counter := &countingWriter{w: hash}
	if err := s.Filesystem().Write(session.FilePath, io.TeeReader(chunks, counter), session.FileSize, 0o644); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	checksum := hex.EncodeToString(hash.Sum(nil))

	c.JSON(http.StatusOK, gin.H{
		"status":    "completed",
		"file_path": session.FilePath,
		"file_size": counter.n,
		"checksum":  "sha256:" + checksum,
	})
}

// chunkReader reads the chunks of an upload one after the other, opening each
// chunk file only once the previous one has been read.
type chunkReader struct {
	dir     string
	total   int
	current int
	file    *os.File
}

func (r *chunkReader) Read(p []byte) (int, error) {
	for {
		if r.file == nil {
			if r.current >= r.total {
				return 0, io.EOF
			}
			f, err := os.Open(filepath.Join(r.dir, fmt.Sprintf("chunk_%d", r.current)))
			if err != nil {
				return 0, err
			}
			r.file = f
		}
		n, err := r.file.Read(p)
		if err == io.EOF {
			_ = r.file.Close()
			r.file = nil
			r.current++
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (r *chunkReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// countingWriter passes writes through to w while counting the bytes written.
type countingWriter struct {
	w io.Writer
	n int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}

// deleteChunkedUpload cancels an active upload session and removes all stored chunks.
func deleteChunkedUpload(c *gin.Context) {
	s := middleware.ExtractServer(c)
	uploadID := c.Param("upload_id")

	session, exists := globalChunkManager.GetSession(uploadID)
	if !exists {
		c.Status(http.StatusNoContent)
		return
	}

	if session.ServerID != s.ID() {
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
			"error": "Upload session does not belong to this server",
		})
		return
	}

	tmpDir := filepath.Join(os.TempDir(), "wings-uploads", session.ID)
	os.RemoveAll(tmpDir)
	globalChunkManager.DeleteSession(session.ID)

	c.Status(http.StatusNoContent)
}

func init() {
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			globalChunkManager.CleanupExpiredSessions()
		}
	}()
}
