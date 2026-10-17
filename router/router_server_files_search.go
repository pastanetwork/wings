package router

import (
	"net/http"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/internal/ufs"
	"github.com/pterodactyl/wings/router/middleware"
	"github.com/pterodactyl/wings/server"
	"github.com/pterodactyl/wings/server/filesystem"
)

type customFileInfo struct {
	ufs.FileInfo
	newName string
}

func (cfi customFileInfo) Name() string {
	return cfi.newName
}

// appendMatchedEntry adds a matched file to the search results with its full path.
func appendMatchedEntry(matchedEntries *[]filesystem.Stat, fileInfo ufs.FileInfo, fullPath string, fileType string) {
	*matchedEntries = append(*matchedEntries, filesystem.Stat{
		FileInfo: customFileInfo{
			FileInfo: fileInfo,
			newName:  fullPath,
		},
		Mimetype: fileType,
	})
}

// isBlacklisted checks if a directory name is in the blacklist configuration.
func isBlacklisted(dirName string) bool {
	blacklist := config.Get().SearchRecursion.BlacklistedDirs
	for _, blacklisted := range blacklist {
		if strings.EqualFold(dirName, strings.ToLower(blacklisted)) {
			return true
		}
	}
	return false
}

// searchDirectory recursively searches for files matching the pattern in the given directory.
// An error is only returned if dir itself cannot be listed, subdirectories that cannot be
// listed are skipped.
func searchDirectory(s *server.Server, dir string, patternLower string, depth int, matchedEntries *[]filesystem.Stat, matchedDirectories *[]string) error {
	if depth > config.Get().SearchRecursion.MaxRecursionDepth {
		return nil
	}

	stats, err := s.Filesystem().ListDirectory(dir)
	if err != nil {
		return err
	}

	for _, fileInfo := range stats {
		fileName := fileInfo.Name()
		fileType := fileInfo.Mimetype
		fileNameLower := strings.ToLower(fileName)
		fullPath := filepath.Join(dir, fileName)

		if fileType == "inode/directory" {
			if isBlacklisted(fileNameLower) {
				continue
			}
			*matchedDirectories = append(*matchedDirectories, fullPath)

			_ = searchDirectory(s, fullPath, patternLower, depth+1, matchedEntries, matchedDirectories)
		}

		if strings.ContainsAny(patternLower, "*?") {
			if match, _ := filepath.Match(patternLower, fileNameLower); match {
				appendMatchedEntry(matchedEntries, fileInfo, fullPath, fileType)
			}
		} else {
			if strings.Contains(fileNameLower, patternLower) {
				appendMatchedEntry(matchedEntries, fileInfo, fullPath, fileType)
			} else {
				ext := filepath.Ext(fileNameLower)
				if strings.HasPrefix(patternLower, ".") || !strings.Contains(patternLower, ".") {
					if strings.TrimPrefix(ext, ".") == strings.TrimPrefix(patternLower, ".") {
						appendMatchedEntry(matchedEntries, fileInfo, fullPath, fileType)
					}
				} else if fileNameLower == patternLower {
					appendMatchedEntry(matchedEntries, fileInfo, fullPath, fileType)
				}
			}
		}
	}
	return nil
}

// getFilesBySearch performs a recursive file search in the server's filesystem.
// Supports wildcards (*?), substring matching, and extension-based searches.
func getFilesBySearch(c *gin.Context) {
	s := middleware.ExtractServer(c)
	dir := strings.TrimSuffix(c.Query("directory"), "/")
	pattern := c.Query("pattern")

	patternLower := strings.ToLower(pattern)

	if len(pattern) < 3 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Pattern must be at least 3 characters long"})
		return
	}

	matchedEntries := []filesystem.Stat{}
	matchedDirectories := []string{}

	if err := searchDirectory(s, dir, patternLower, 0, &matchedEntries, &matchedDirectories); err != nil {
		c.JSON(http.StatusOK, gin.H{"message": "Directory not found"})
		return
	}

	c.JSON(http.StatusOK, matchedEntries)
}
