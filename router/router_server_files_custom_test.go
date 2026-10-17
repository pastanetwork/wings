package router

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// request sends an authenticated request to one of the server endpoints and
// returns the response status and body.
func (u *uploadTest) request(t *testing.T, method, path string, headers map[string]string, body io.Reader) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, u.url+"/api/servers/"+uploadTestServerUuid+path, body)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer test-token")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	return res.StatusCode, b
}

func (u *uploadTest) postJSON(t *testing.T, path string, data any) (int, []byte) {
	t.Helper()
	b, err := json.Marshal(data)
	require.NoError(t, err)
	return u.request(t, http.MethodPost, path, map[string]string{"Content-Type": "application/json"}, bytes.NewReader(b))
}

// chunkedUpload runs a full chunked upload of content to p and returns the
// status of the finalize request.
func (u *uploadTest) chunkedUpload(t *testing.T, p string, content []byte, chunkSize int) int {
	t.Helper()
	status, body := u.postJSON(t, "/files/upload/init", map[string]any{
		"path":       p,
		"file_size":  len(content),
		"chunk_size": chunkSize,
	})
	require.Equal(t, http.StatusOK, status, string(body))
	var init struct {
		UploadID    string `json:"upload_id"`
		TotalChunks int    `json:"total_chunks"`
	}
	require.NoError(t, json.Unmarshal(body, &init))

	for i := 0; i < init.TotalChunks; i++ {
		end := min((i+1)*chunkSize, len(content))
		status, body := u.request(t, http.MethodPost, "/files/upload/chunk", map[string]string{
			"X-Upload-Id":    init.UploadID,
			"X-Chunk-Index":  strconv.Itoa(i),
			"X-Total-Chunks": strconv.Itoa(init.TotalChunks),
			"Content-Type":   "application/octet-stream",
		}, bytes.NewReader(content[i*chunkSize:end]))
		require.Equal(t, http.StatusOK, status, string(body))
	}

	status, _ = u.postJSON(t, "/files/upload/finalize", map[string]any{"upload_id": init.UploadID})
	return status
}

func TestChunkedUploadWritesFile(t *testing.T) {
	u := newUploadTest(t, 10, 0)
	content := bytes.Repeat([]byte("abcdefgh"), 20_000)

	require.Equal(t, http.StatusOK, u.chunkedUpload(t, "/dir/file.bin", content, 64*1024))

	got, err := os.ReadFile(filepath.Join(u.root, "dir", "file.bin"))
	require.NoError(t, err)
	require.Equal(t, content, got)
}

// A path that walks above the server root is resolved against the root instead.
func TestChunkedUploadStaysInsideRoot(t *testing.T) {
	u := newUploadTest(t, 10, 0)
	content := []byte("contained")

	require.Equal(t, http.StatusOK, u.chunkedUpload(t, "../../escape.bin", content, 64*1024))

	_, err := os.Stat(filepath.Join(filepath.Dir(u.root), "escape.bin"))
	require.ErrorIs(t, err, os.ErrNotExist)
	got, err := os.ReadFile(filepath.Join(u.root, "escape.bin"))
	require.NoError(t, err)
	require.Equal(t, content, got)
}

// Chunked uploads count against the server's disk limit.
func TestChunkedUploadRespectsDiskLimit(t *testing.T) {
	u := newUploadTest(t, 10, 1)

	status, _ := u.postJSON(t, "/files/upload/init", map[string]any{
		"path":       "big.bin",
		"file_size":  2 << 20,
		"chunk_size": 1 << 20,
	})
	require.GreaterOrEqual(t, status, http.StatusBadRequest)
}

// A chunk larger than the session's chunk size is refused.
func TestChunkedUploadRejectsOversizedChunk(t *testing.T) {
	u := newUploadTest(t, 10, 0)

	status, body := u.postJSON(t, "/files/upload/init", map[string]any{
		"path":       "file.bin",
		"file_size":  128 * 1024,
		"chunk_size": 64 * 1024,
	})
	require.Equal(t, http.StatusOK, status, string(body))
	var init struct {
		UploadID string `json:"upload_id"`
	}
	require.NoError(t, json.Unmarshal(body, &init))

	status, _ = u.request(t, http.MethodPost, "/files/upload/chunk", map[string]string{
		"X-Upload-Id":    init.UploadID,
		"X-Chunk-Index":  "0",
		"X-Total-Chunks": "2",
	}, bytes.NewReader(bytes.Repeat([]byte("a"), 1<<20)))
	require.Equal(t, http.StatusBadRequest, status)
}

func TestChunkedUploadRejectsInvalidSizes(t *testing.T) {
	u := newUploadTest(t, 10, 0)

	for _, sizes := range [][2]int{{-1, 64 * 1024}, {1024, -1}, {1 << 20, 1}} {
		status, _ := u.postJSON(t, "/files/upload/init", map[string]any{
			"path":       "file.bin",
			"file_size":  sizes[0],
			"chunk_size": sizes[1],
		})
		require.Equal(t, http.StatusBadRequest, status, "file_size=%d chunk_size=%d", sizes[0], sizes[1])
	}
}

func TestFileLinesReadsFile(t *testing.T) {
	u := newUploadTest(t, 10, 0)
	require.NoError(t, os.MkdirAll(u.root, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(u.root, "log.txt"), []byte("one\ntwo\nthree\n"), 0o644))

	status, body := u.request(t, http.MethodGet, "/files/lines?file=/log.txt&start=2&end=3", nil, nil)
	require.Equal(t, http.StatusOK, status, string(body))
	require.Contains(t, string(body), `"content":"two"`)
	require.Contains(t, string(body), `"content":"three"`)
}

// The line endpoints cannot read files outside of the server root, either
// through the path or through a symlink.
func TestFileLinesStaysInsideRoot(t *testing.T) {
	u := newUploadTest(t, 10, 0)
	require.NoError(t, os.MkdirAll(u.root, 0o755))
	outside := filepath.Join(filepath.Dir(u.root), "outside.txt")
	require.NoError(t, os.WriteFile(outside, []byte("secret\n"), 0o644))
	require.NoError(t, os.Symlink(outside, filepath.Join(u.root, "link.txt")))

	for _, p := range []string{"../outside.txt", "a/../../outside.txt", "link.txt"} {
		for _, endpoint := range []string{"/files/lines?start=1&end=1&file=", "/files/metadata?file="} {
			status, body := u.request(t, http.MethodGet, endpoint+p, nil, nil)
			require.NotEqual(t, http.StatusOK, status, "%s%s", endpoint, p)
			require.NotContains(t, string(body), "secret")
		}

		status, _ := u.postJSON(t, "/files/replace-line", map[string]any{"file": p, "line_number": 1, "content": "changed"})
		require.NotEqual(t, http.StatusNoContent, status, p)
	}

	got, err := os.ReadFile(outside)
	require.NoError(t, err)
	require.Equal(t, "secret\n", string(got))
}
