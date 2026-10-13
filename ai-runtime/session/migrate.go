package session

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// SessionFile 描述 session root 下发现的单个持久化会话文件。
type SessionFile struct {
	ID   string
	Path string
}

// SessionPath 返回 session ID 对应的安全文件路径。
func SessionPath(root, id string) (string, error) {
	if err := validateSessionID(id); err != nil {
		return "", err
	}
	return filepath.Join(root, "sessions", id+".jsonl"), nil
}

// ListSessionFiles 扫描新的 per-session 文件布局。
func ListSessionFiles(root string) ([]SessionFile, error) {
	dir := filepath.Join(root, "sessions")
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return []SessionFile{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("session: list files: %w", err)
	}

	files := make([]SessionFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".jsonl")
		if err := validateSessionID(id); err != nil {
			continue
		}
		files = append(files, SessionFile{ID: id, Path: filepath.Join(dir, entry.Name())})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].ID < files[j].ID })
	return files, nil
}

// MigrateLegacy converts current.jsonl and archive/*.jsonl into one file per session.
// Each source is renamed only after its lifecycle record is durable, so retries are safe.
func MigrateLegacy(root string) error {
	if root == "" {
		return fmt.Errorf("session: empty migration root")
	}
	if err := os.MkdirAll(filepath.Join(root, "sessions"), 0o700); err != nil {
		return fmt.Errorf("session: create migration directory: %w", err)
	}
	if err := os.Chmod(filepath.Join(root, "sessions"), 0o700); err != nil {
		return fmt.Errorf("session: restrict migration directory: %w", err)
	}

	current := filepath.Join(root, "current.jsonl")
	if err := migrateFile(root, current, SessionStatusLive, ""); err != nil {
		return err
	}

	archiveDir := filepath.Join(root, "archive")
	entries, err := os.ReadDir(archiveDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("session: read legacy archive: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".jsonl")
		if err := migrateFile(root, filepath.Join(archiveDir, entry.Name()), SessionStatusArchived, id); err != nil {
			return err
		}
	}
	return nil
}

func migrateFile(root, source string, status SessionStatus, preferredID string) error {
	info, err := os.Stat(source)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("session: stat legacy file %s: %w", source, err)
	}
	if info.IsDir() {
		return fmt.Errorf("session: legacy path is a directory: %s", source)
	}

	recs, err := loadRecords(source)
	if err != nil {
		return fmt.Errorf("session: read legacy file %s: %w", source, err)
	}
	if !hasMigratableRecords(recs) {
		return nil
	}

	id, err := migrationID(source, preferredID)
	if err != nil {
		return err
	}
	var destination string
	id, destination, err = migrationDestination(root, id, source)
	if err != nil {
		return err
	}

	st, err := OpenPath(source)
	if err != nil {
		return fmt.Errorf("session: open legacy file %s: %w", source, err)
	}
	meta, hasMeta := st.SessionMeta()
	if hasMeta && validateSessionID(meta.ID) == nil {
		id, destination, err = migrationDestination(root, meta.ID, source)
		if err != nil {
			st.Close()
			return err
		}
	}
	if !hasMeta {
		if err := st.SetSessionMeta(id, status, info.ModTime()); err != nil {
			st.Close()
			return fmt.Errorf("session: mark legacy file %s: %w", source, err)
		}
	} else if meta.ID != id || meta.Status != status {
		if err := st.SetSessionMeta(id, status, meta.CreatedAt); err != nil {
			st.Close()
			return fmt.Errorf("session: normalize legacy metadata %s: %w", source, err)
		}
	}
	if err := st.Close(); err != nil {
		return fmt.Errorf("session: close legacy file %s: %w", source, err)
	}
	if err := os.Rename(source, destination); err != nil {
		return fmt.Errorf("session: move legacy file %s: %w", source, err)
	}
	return nil
}

func hasMigratableRecords(recs []Record) bool {
	for _, rec := range recs {
		switch rec.Kind {
		case kindMsg, kindCompact, kindHead, kindCheckpoint, kindCursor, kindSession:
			return true
		}
	}
	return false
}

func migrationDestination(root, preferredID, source string) (string, string, error) {
	id := preferredID
	destination, err := SessionPath(root, id)
	if err != nil {
		return "", "", err
	}
	if _, err := os.Stat(destination); os.IsNotExist(err) {
		return id, destination, nil
	} else if err != nil {
		return "", "", fmt.Errorf("session: stat migration destination %s: %w", destination, err)
	}

	id, err = migrationID(source, "")
	if err != nil {
		return "", "", err
	}
	for suffix := 0; ; suffix++ {
		candidate := id
		if suffix > 0 {
			candidate = fmt.Sprintf("%s-%d", id, suffix)
		}
		destination, err = SessionPath(root, candidate)
		if err != nil {
			return "", "", err
		}
		if _, err := os.Stat(destination); os.IsNotExist(err) {
			return candidate, destination, nil
		} else if err != nil {
			return "", "", fmt.Errorf("session: stat migration destination %s: %w", destination, err)
		}
	}
}

func migrationID(source, preferredID string) (string, error) {
	if preferredID != "" {
		if err := validateSessionID(preferredID); err == nil {
			return preferredID, nil
		}
	}
	data, err := os.ReadFile(source)
	if err != nil {
		return "", fmt.Errorf("session: read legacy file for id %s: %w", source, err)
	}
	hash := sha256.Sum256(data)
	return "sess-" + hex.EncodeToString(hash[:8]), nil
}

// ValidateSessionID validates an external session identifier before it reaches storage or routing.
func ValidateSessionID(id string) error {
	return validateSessionID(id)
}

func validateSessionID(id string) error {
	if id == "" || id == "." || id == ".." {
		return fmt.Errorf("invalid session id %q", id)
	}
	if filepath.Base(id) != id || strings.ContainsAny(id, `/\\`) || strings.HasPrefix(id, ".") {
		return fmt.Errorf("invalid session id %q", id)
	}
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		return fmt.Errorf("invalid session id %q", id)
	}
	return nil
}
