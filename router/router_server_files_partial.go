package router

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"emperror.dev/errors"
	"github.com/gin-gonic/gin"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/internal/ufs"
	"github.com/pterodactyl/wings/router/middleware"
	"github.com/pterodactyl/wings/server"
	"github.com/pterodactyl/wings/server/filesystem"
)

// getFileLines returns specific lines from a file without loading the entire file.
// Useful for viewing portions of large log files.
func getFileLines(c *gin.Context) {
	s := middleware.ExtractServer(c)
	filePath := c.Query("file")
	startLine, _ := strconv.Atoi(c.Query("start"))
	endLine, _ := strconv.Atoi(c.Query("end"))

	if filePath == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "File path is required",
		})
		return
	}

	if startLine < 1 {
		startLine = 1
	}

	maxLines := config.Get().PartialEdit.MaxLinesPerRequest
	if endLine-startLine+1 > maxLines {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "Too many lines requested. Maximum is " + strconv.Itoa(maxLines),
		})
		return
	}

	file, _, ok := openPartialEditFile(c, s, filePath, false)
	if !ok {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	maxLineSize := config.Get().PartialEdit.MaxLineSize
	buf := make([]byte, maxLineSize)
	scanner.Buffer(buf, int(maxLineSize))

	lines := []gin.H{}
	currentLine := 1
	totalLines := 0

	for scanner.Scan() {
		totalLines++
		if currentLine >= startLine && currentLine <= endLine {
			lines = append(lines, gin.H{
				"number":  currentLine,
				"content": scanner.Text(),
			})
		}
		if currentLine > endLine {
			break
		}
		currentLine++
	}

	if currentLine < endLine {
		for currentLine <= endLine {
			totalLines = currentLine
			currentLine++
		}
	}

	if err := scanner.Err(); err != nil {
		if err == bufio.ErrTooLong {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"error": "File contains lines that exceed the maximum allowed size. This file cannot be read line by line.",
			})
			return
		}
		middleware.CaptureAndAbort(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"file":        filePath,
		"start_line":  startLine,
		"end_line":    endLine,
		"total_lines": totalLines,
		"lines":       lines,
	})
}

// postReplaceLine replaces a specific line in a file with new content.
// Reads the entire file, modifies the target line, and writes it back.
func postReplaceLine(c *gin.Context) {
	s := middleware.ExtractServer(c)

	var data struct {
		File       string `json:"file" binding:"required"`
		LineNumber int    `json:"line_number" binding:"required"`
		Content    string `json:"content"`
	}

	if err := c.BindJSON(&data); err != nil {
		return
	}

	cleanPath := strings.TrimLeft(data.File, "/")
	file, _, ok := openPartialEditFile(c, s, cleanPath, true)
	if !ok {
		return
	}

	var lines []string
	scanner := bufio.NewScanner(file)
	maxLineSize := config.Get().PartialEdit.MaxLineSize
	buf := make([]byte, maxLineSize)
	scanner.Buffer(buf, int(maxLineSize))

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	file.Close()

	if err := scanner.Err(); err != nil {
		if err == bufio.ErrTooLong {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"error": "File contains lines that exceed the maximum allowed size.",
			})
			return
		}
		middleware.CaptureAndAbort(c, err)
		return
	}

	if data.LineNumber < 1 || data.LineNumber > len(lines) {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "Line number out of range",
		})
		return
	}

	lines[data.LineNumber-1] = data.Content

	content := strings.Join(lines, "\n")
	if len(lines) > 0 {
		content += "\n"
	}

	if err := s.Filesystem().Write(cleanPath, strings.NewReader(content), int64(len(content)), 0o644); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	c.Status(http.StatusNoContent)
}

// postInsertLines inserts one or more lines at a specific position in a file.
// Existing lines at and after the insertion point are shifted down.
func postInsertLines(c *gin.Context) {
	s := middleware.ExtractServer(c)

	var data struct {
		File       string   `json:"file" binding:"required"`
		LineNumber int      `json:"line_number" binding:"required"`
		Lines      []string `json:"lines" binding:"required"`
	}

	if err := c.BindJSON(&data); err != nil {
		return
	}

	cleanPath := strings.TrimLeft(data.File, "/")
	file, _, ok := openPartialEditFile(c, s, cleanPath, true)
	if !ok {
		return
	}

	var lines []string
	scanner := bufio.NewScanner(file)
	maxLineSize := config.Get().PartialEdit.MaxLineSize
	buf := make([]byte, maxLineSize)
	scanner.Buffer(buf, int(maxLineSize))

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	file.Close()

	if err := scanner.Err(); err != nil {
		if err == bufio.ErrTooLong {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"error": "File contains lines that exceed the maximum allowed size.",
			})
			return
		}
		middleware.CaptureAndAbort(c, err)
		return
	}

	if data.LineNumber < 1 || data.LineNumber > len(lines)+1 {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "Line number out of range",
		})
		return
	}

	newLines := make([]string, 0, len(lines)+len(data.Lines))
	newLines = append(newLines, lines[:data.LineNumber-1]...)
	newLines = append(newLines, data.Lines...)
	newLines = append(newLines, lines[data.LineNumber-1:]...)

	content := strings.Join(newLines, "\n")
	if len(newLines) > 0 {
		content += "\n"
	}

	if err := s.Filesystem().Write(cleanPath, strings.NewReader(content), int64(len(content)), 0o644); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	c.Status(http.StatusNoContent)
}

// postDeleteLines removes a range of lines from a file.
// Supports deleting single lines or multiple consecutive lines.
func postDeleteLines(c *gin.Context) {
	s := middleware.ExtractServer(c)

	var data struct {
		File      string `json:"file" binding:"required"`
		StartLine int    `json:"start_line" binding:"required"`
		EndLine   int    `json:"end_line" binding:"required"`
	}

	if err := c.BindJSON(&data); err != nil {
		return
	}

	cleanPath := strings.TrimLeft(data.File, "/")
	file, _, ok := openPartialEditFile(c, s, cleanPath, true)
	if !ok {
		return
	}

	var lines []string
	scanner := bufio.NewScanner(file)
	maxLineSize := config.Get().PartialEdit.MaxLineSize
	buf := make([]byte, maxLineSize)
	scanner.Buffer(buf, int(maxLineSize))

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	file.Close()

	if err := scanner.Err(); err != nil {
		if err == bufio.ErrTooLong {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"error": "File contains lines that exceed the maximum allowed size.",
			})
			return
		}
		middleware.CaptureAndAbort(c, err)
		return
	}

	if data.StartLine < 1 || data.EndLine > len(lines) || data.StartLine > data.EndLine {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "Invalid line range",
		})
		return
	}

	newLines := make([]string, 0, len(lines)-(data.EndLine-data.StartLine+1))
	newLines = append(newLines, lines[:data.StartLine-1]...)
	newLines = append(newLines, lines[data.EndLine:]...)

	content := strings.Join(newLines, "\n")
	if len(newLines) > 0 {
		content += "\n"
	}

	if err := s.Filesystem().Write(cleanPath, strings.NewReader(content), int64(len(content)), 0o644); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	c.Status(http.StatusNoContent)
}

// getFileMetadata returns detailed information about a file including size,
// total lines, encoding type, and line ending format.
func getFileMetadata(c *gin.Context) {
	s := middleware.ExtractServer(c)
	filePath := c.Query("file")

	if filePath == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "File path is required",
		})
		return
	}

	file, fileInfo, ok := openPartialEditFile(c, s, filePath, false)
	if !ok {
		return
	}
	defer file.Close()

	encoding := detectEncoding(file)
	file.Seek(0, 0)

	lineEnding := detectLineEnding(file)
	file.Seek(0, 0)

	totalLines := countLines(file)

	c.JSON(http.StatusOK, gin.H{
		"file":        filePath,
		"size":        fileInfo.Size(),
		"total_lines": totalLines,
		"encoding":    encoding,
		"line_ending": lineEnding,
		"created":     fileInfo.ModTime(),
		"modified":    fileInfo.ModTime(),
	})
}

// openPartialEditFile opens a regular file inside the server's root directory
// through the server filesystem, so paths and symlinks cannot resolve outside
// of it. Named pipes, devices and directories are refused. When readAll is
// true the file is about to be read into memory in full, so its size is also
// checked against the configured limit. If false is returned the request has
// already been aborted.
func openPartialEditFile(c *gin.Context, s *server.Server, p string, readAll bool) (ufs.File, filesystem.Stat, bool) {
	cleanPath := strings.TrimLeft(p, "/")
	if err := s.Filesystem().IsIgnored(cleanPath); err != nil {
		middleware.CaptureAndAbort(c, err)
		return nil, filesystem.Stat{}, false
	}

	file, st, err := s.Filesystem().File(cleanPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
				"error":      "File not found",
				"request_id": c.Writer.Header().Get("X-Request-Id"),
			})
			return nil, filesystem.Stat{}, false
		}
		middleware.CaptureAndAbort(c, err)
		return nil, filesystem.Stat{}, false
	}

	if !st.Mode().IsRegular() {
		_ = file.Close()
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "Cannot open files of this type.",
		})
		return nil, filesystem.Stat{}, false
	}

	if maxSize := config.Get().PartialEdit.MaxFileSize; readAll && maxSize > 0 && st.Size() > maxSize {
		_ = file.Close()
		c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{
			"error": "File is too large to be edited line by line. Maximum is " + strconv.FormatInt(maxSize, 10) + " bytes.",
		})
		return nil, filesystem.Stat{}, false
	}

	return file, st, true
}

// detectEncoding analyzes the file's byte order mark and content to determine
// its character encoding (UTF-8, UTF-8-BOM, or ASCII).
func detectEncoding(file io.Reader) string {
	buf := make([]byte, 1024)
	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		return "unknown"
	}
	buf = buf[:n]

	if bytes.HasPrefix(buf, []byte{0xEF, 0xBB, 0xBF}) {
		return "utf-8-bom"
	}

	if _, _, err := transform.Bytes(unicode.UTF8.NewDecoder(), buf); err == nil {
		return "utf-8"
	}

	return "ascii"
}

// detectLineEnding determines the line ending style used in the file
// (CRLF for Windows, LF for Unix/Linux, or CR for classic Mac).
func detectLineEnding(file io.Reader) string {
	buf := make([]byte, 1024)
	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		return "unknown"
	}
	buf = buf[:n]

	hasCRLF := bytes.Contains(buf, []byte("\r\n"))
	hasLF := bytes.Contains(buf, []byte("\n"))
	hasCR := bytes.Contains(buf, []byte("\r"))

	if hasCRLF {
		return "CRLF"
	} else if hasLF {
		return "LF"
	} else if hasCR {
		return "CR"
	}

	return "unknown"
}

// countLines efficiently counts the total number of lines in a file
// using buffered scanning without loading the entire file into memory.
func countLines(file io.Reader) int {
	scanner := bufio.NewScanner(file)
	maxLineSize := config.Get().PartialEdit.MaxLineSize
	buf := make([]byte, maxLineSize)
	scanner.Buffer(buf, int(maxLineSize))
	count := 0
	for scanner.Scan() {
		count++
	}
	return count
}
