package router

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/pterodactyl/wings/router/middleware"
	"github.com/pterodactyl/wings/router/tokens"
	"github.com/pterodactyl/wings/server/backup"
)

// Structure pour gérer les Range requests
type rangeSpec struct {
	start int64
	end   int64
	size  int64
}

// Parse le header Range selon RFC 7233
func parseRangeHeader(rangeHeader string, fileSize int64) (*rangeSpec, error) {
	if rangeHeader == "" {
		return nil, nil // Pas de range = fichier complet
	}

	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return nil, errors.New("format range invalide")
	}

	// Parse "bytes=start-end" ou "bytes=start-" ou "bytes=-suffix"
	ranges := strings.TrimPrefix(rangeHeader, "bytes=")

	// On ne supporte qu'un seul range pour simplifier
	if strings.Contains(ranges, ",") {
		parts := strings.Split(ranges, ",")
		ranges = strings.TrimSpace(parts[0]) // Prend juste le premier range
	}

	var start, end int64
	var err error

	if strings.HasPrefix(ranges, "-") {
		// Suffix range: "-500" = derniers 500 bytes
		suffixLength, err := strconv.ParseInt(ranges[1:], 10, 64)
		if err != nil {
			return nil, errors.New("suffix range invalide")
		}
		if suffixLength >= fileSize {
			start = 0
		} else {
			start = fileSize - suffixLength
		}
		end = fileSize - 1
	} else if strings.HasSuffix(ranges, "-") {
		// Start range: "500-" = du byte 500 à la fin
		start, err = strconv.ParseInt(ranges[:len(ranges)-1], 10, 64)
		if err != nil {
			return nil, errors.New("start range invalide")
		}
		end = fileSize - 1
	} else {
		// Full range: "500-999"
		parts := strings.Split(ranges, "-")
		if len(parts) != 2 {
			return nil, errors.New("format range invalide")
		}

		start, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return nil, errors.New("start range invalide")
		}

		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return nil, errors.New("end range invalide")
		}
	}

	// Validation des limites
	if start < 0 || start >= fileSize || end < start || end >= fileSize {
		return nil, errors.New("range hors limites")
	}

	return &rangeSpec{
		start: start,
		end:   end,
		size:  end - start + 1,
	}, nil
}

// Handle HEAD request for server backup
func getDownloadBackupHead(c *gin.Context) {
	client := middleware.ExtractApiClient(c)
	manager := middleware.ExtractManager(c)

	// Get the payload from the token
	token := tokens.BackupPayload{}
	if err := tokens.ParseToken([]byte(c.Query("token")), &token); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	// Get the server using the UUID from the token.
	// Note: HEAD endpoint is a probe, so we don't consume the one-time token here.
	if _, ok := manager.Get(token.ServerUuid); !ok || token.Denylisted() || !token.HasScope(tokens.BackupDownload) {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
			"error": "The requested resource was not found on this server.",
		})
		return
	}
	middleware.ClearDeadlines(c)

	// Validate UUID
	if _, err := uuid.Parse(token.BackupUuid); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	// Locate the backup on the local disk
	b, st, err := backup.LocateLocal(client, token.BackupUuid, token.ServerUuid)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
				"error": "The requested backup was not found on this server.",
			})
			return
		}
		middleware.CaptureAndAbort(c, err)
		return
	}

	// The use of `os` here is safe as backups are not stored within server accessible directories
	f, err := os.Open(b.Path())
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}
	defer f.Close()

	// Headers standards + Range support
	c.Header("Content-Length", strconv.FormatInt(st.Size(), 10))
	c.Header("Content-Disposition", "attachment; filename="+strconv.Quote(st.Name()))
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Accept-Ranges", "bytes") //Support Range
	c.Header("Last-Modified", st.ModTime().UTC().Format(http.TimeFormat))

	c.Status(http.StatusOK)
}

// Handle a download request for a server backup.
func getDownloadBackup(c *gin.Context) {
	client := middleware.ExtractApiClient(c)
	manager := middleware.ExtractManager(c)

	// Get the payload from the token
	token := tokens.BackupPayload{}
	if err := tokens.ParseToken([]byte(c.Query("token")), &token); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	// Get the server using the UUID from the token
	if _, ok := manager.Get(token.ServerUuid); !ok || token.Denylisted() || !token.IsUniqueRequest() || !token.HasScope(tokens.BackupDownload) {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
			"error": "The requested resource was not found on this server.",
		})
		return
	}
	middleware.ClearDeadlines(c)

	// Validate UUID
	if _, err := uuid.Parse(token.BackupUuid); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	// Locate the backup on the local disk
	b, st, err := backup.LocateLocal(client, token.BackupUuid, token.ServerUuid)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
				"error": "The requested backup was not found on this server.",
			})
			return
		}
		middleware.CaptureAndAbort(c, err)
		return
	}

	// The use of `os` here is safe as backups are not stored within server accessible directories
	f, err := os.Open(b.Path())
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}
	defer f.Close()

	fileSize := st.Size()

	// Parse Range header si présent
	rangeHeader := c.GetHeader("Range")
	rangeSpec, err := parseRangeHeader(rangeHeader, fileSize)

	if err != nil {
		// Range invalide
		c.Header("Content-Range", fmt.Sprintf("bytes */%d", fileSize))
		c.AbortWithStatus(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	// Headers de base
	c.Header("Content-Disposition", "attachment; filename="+strconv.Quote(st.Name()))
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Accept-Ranges", "bytes") // SEULE NOUVEAUTÉ

	if rangeSpec != nil {
		// Partial Content (206) NOUVELLE FONCTIONNALITÉ
		c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rangeSpec.start, rangeSpec.end, fileSize))
		c.Header("Content-Length", strconv.FormatInt(rangeSpec.size, 10))
		c.Status(http.StatusPartialContent)

		// Seek au début du range
		if _, err := f.Seek(rangeSpec.start, io.SeekStart); err != nil {
			middleware.CaptureAndAbort(c, err)
			return
		}

		// Copy seulement la partie demandée
		_, err := io.CopyN(c.Writer, f, rangeSpec.size)
		if err != nil && err != io.EOF {
			// Log l'erreur mais continue (connexion client fermée normale)
			middleware.ExtractLogger(c).WithError(err).Debug("erreur streaming backup partiel (connexion probablement fermée)")
		}
	} else {
		// Fichier complet (200) buffer optimisé
		c.Header("Content-Length", strconv.FormatInt(fileSize, 10))
		c.Status(http.StatusOK)

		// Utilise un buffer optimisé au lieu de bufio.NewReader(f).WriteTo(c.Writer)
		bufferSize := 64 * 1024        // 64KB par défaut
		if fileSize > 1024*1024*1024 { // > 1GB
			bufferSize = 2 * 1024 * 1024 // 2MB pour gros backups
		}

		bufferedReader := bufio.NewReaderSize(f, bufferSize)
		_, err := bufferedReader.WriteTo(c.Writer)
		if err != nil && err != io.EOF {
			// Log sans abort
			middleware.ExtractLogger(c).WithError(err).Debug("erreur streaming backup (connexion probablement fermée)")
		}
	}
}

// Handle HEAD request for server file
func getDownloadFileHead(c *gin.Context) {
	manager := middleware.ExtractManager(c)

	// Parse token
	token := tokens.FilePayload{}
	if err := tokens.ParseToken([]byte(c.Query("token")), &token); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	// Get server
	// Note: HEAD endpoint is a probe, so we don't consume the one-time token here.
	s, ok := manager.Get(token.ServerUuid)
	if !ok || !token.HasScope(tokens.FileDownload) {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
			"error": "The requested resource was not found on this server.",
		})
		return
	}

	// Get file
	f, st, err := s.Filesystem().File(token.FilePath)
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}
	defer f.Close()

	// Check not directory
	if st.IsDir() {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
			"error": "The requested resource was not found on this server.",
		})
		return
	}

	// Headers standards + Range support
	c.Header("Content-Length", strconv.FormatInt(st.Size(), 10))
	c.Header("Content-Disposition", "attachment; filename="+strconv.Quote(st.Name()))
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Accept-Ranges", "bytes") // ← SEULE NOUVEAUTÉ : Support Range
	c.Header("Last-Modified", st.ModTime().UTC().Format(http.TimeFormat))

	c.Status(http.StatusOK)
}

// Handles downloading a specific file for a server.
func getDownloadFile(c *gin.Context) {
	manager := middleware.ExtractManager(c)

	// Parse token
	token := tokens.FilePayload{}
	if err := tokens.ParseToken([]byte(c.Query("token")), &token); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	// Get server
	s, ok := manager.Get(token.ServerUuid)
	if !ok || token.Denylisted() || !token.IsUniqueRequest() || !token.HasScope(tokens.FileDownload) {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
			"error": "The requested resource was not found on this server.",
		})
		return
	}
	middleware.ClearDeadlines(c)

	// Get file
	f, st, err := s.Filesystem().File(token.FilePath)
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}
	defer f.Close()

	// Check not directory
	if st.IsDir() {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
			"error": "The requested resource was not found on this server.",
		})
		return
	}

	fileSize := st.Size()

	// Parse Range header si présent
	rangeHeader := c.GetHeader("Range")
	rangeSpec, err := parseRangeHeader(rangeHeader, fileSize)

	if err != nil {
		// Range invalide
		c.Header("Content-Range", fmt.Sprintf("bytes */%d", fileSize))
		c.AbortWithStatus(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	// Headers de base
	c.Header("Content-Disposition", "attachment; filename="+strconv.Quote(st.Name()))
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Accept-Ranges", "bytes") // SEULE NOUVEAUTÉ

	if rangeSpec != nil {
		// Partial Content (206) NOUVELLE FONCTIONNALITÉ
		c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rangeSpec.start, rangeSpec.end, fileSize))
		c.Header("Content-Length", strconv.FormatInt(rangeSpec.size, 10))
		c.Status(http.StatusPartialContent)

		// Seek au début du range
		if _, err := f.Seek(rangeSpec.start, io.SeekStart); err != nil {
			middleware.CaptureAndAbort(c, err)
			return
		}

		// Copy seulement la partie demandée
		_, err := io.CopyN(c.Writer, f, rangeSpec.size)
		if err != nil && err != io.EOF {
			// Log l'erreur mais continue (connexion client fermée normale)
			middleware.ExtractLogger(c).WithError(err).Debug("erreur streaming file partiel (connexion probablement fermée)")
		}
	} else {
		// Fichier complet (200) buffer optimisé
		c.Header("Content-Length", strconv.FormatInt(fileSize, 10))
		c.Status(http.StatusOK)

		// Utilise un buffer optimisé au lieu de bufio.NewReader(f).WriteTo(c.Writer)
		bufferSize := 64 * 1024       // 64KB par défaut
		if fileSize > 100*1024*1024 { // > 100MB
			bufferSize = 1024 * 1024 // 1MB pour gros fichiers
		}

		bufferedReader := bufio.NewReaderSize(f, bufferSize)
		_, err := bufferedReader.WriteTo(c.Writer)
		if err != nil && err != io.EOF {
			// Log sans abort
			middleware.ExtractLogger(c).WithError(err).Debug("erreur streaming file (connexion probablement fermée)")
		}
	}
}
