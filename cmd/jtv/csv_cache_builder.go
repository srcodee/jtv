package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// Background builders.
//
// Slow cache work runs in a detached copy of jtv started in builder mode
// through environment variables, so the user's session never waits for it:
//
//   - the cache builder writes the cache of a CSV file after the first load
//     answered from memory;
//   - the key builder writes the sorted key table of one column the first
//     time a query looks values up in it (see key_lookup.go).
//
// A lock file next to the output keeps concurrent runs from building the
// same file twice.

const (
	cacheBuilderEnv       = "JTV_INTERNAL_CACHE_BUILD"
	cacheBuilderSourceEnv = "JTV_INTERNAL_CACHE_SOURCE"
	cacheBuilderCommaEnv  = "JTV_INTERNAL_CACHE_COMMA"
	cacheBuilderHeaderEnv = "JTV_INTERNAL_CACHE_HEADERS"
	cacheBuilderNoHdrEnv  = "JTV_INTERNAL_CACHE_HEADERLESS"

	builderParentEnv    = "JTV_INTERNAL_BUILDER_PARENT"
	keyBuilderEnv       = "JTV_INTERNAL_KEY_BUILD"
	keyBuilderCacheEnv  = "JTV_INTERNAL_KEY_CACHE"
	keyBuilderColumnEnv = "JTV_INTERNAL_KEY_COLUMN"

	// staleCacheLock is how long a lock may exist before a new builder may
	// assume the previous one died.
	staleCacheLock = 30 * time.Minute
)

func isCacheBuilderProcess() bool {
	return os.Getenv(cacheBuilderEnv) != "" || os.Getenv(keyBuilderEnv) != ""
}

// startCacheBuilder launches the cache builder unless one is already
// running for this cache file. Failures are ignored: the cache is only an
// optimization.
func startCacheBuilder(cacheFile, path string, comma rune, headers []string, hasHeader bool) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return
	}
	startBuilder(cacheFile,
		cacheBuilderEnv+"="+cacheFile,
		cacheBuilderSourceEnv+"="+absPath,
		cacheBuilderCommaEnv+"="+string(comma),
		cacheBuilderHeaderEnv+"="+strings.Join(headers, "\x1f"),
		cacheBuilderNoHdrEnv+"="+strconv.FormatBool(!hasHeader),
	)
}

// startKeyBuilder launches the key builder for one column of a cache.
func startKeyBuilder(cacheFile, column, keyFile string) {
	// The automatic build may have finished since this session looked.
	if _, err := os.Stat(keyFile); err == nil {
		return
	}
	startBuilder(keyFile,
		keyBuilderEnv+"="+keyFile,
		keyBuilderCacheEnv+"="+cacheFile,
		keyBuilderColumnEnv+"="+column,
	)
}

// startBuilder takes output's lock and starts a detached builder process.
func startBuilder(output string, env ...string) {
	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
		return
	}
	lock := output + ".lock"
	if info, err := os.Stat(lock); err == nil && (time.Since(info.ModTime()) > staleCacheLock || !builderRunning(lock)) {
		os.Remove(lock)
	}
	lockFile, err := os.OpenFile(lock, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return // another builder holds the lock
	}
	lockFile.Close()

	if exe, err := os.Executable(); err == nil {
		cmd := exec.Command(exe)
		cmd.Env = append(os.Environ(), env...)
		cmd.Env = append(cmd.Env, builderParentEnv+"="+strconv.Itoa(os.Getpid()))
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := cmd.Start(); err == nil {
			// Waiting runs check the pid to tell a live builder from a dead one.
			os.WriteFile(lock, []byte(strconv.Itoa(cmd.Process.Pid)), 0o644)
			cmd.Process.Release()
			return
		}
	}
	os.Remove(lock)
}

// runCacheBuilder is the entry point of a detached builder process.
func runCacheBuilder() int {
	// Stay out of the way of the jtv sessions the user is waiting on.
	unix.Setpriority(unix.PRIO_PROCESS, 0, 10)
	if dir, ok := csvCacheDir(); ok {
		removeAbandonedWork(dir)
	}
	if keyFile := os.Getenv(keyBuilderEnv); keyFile != "" {
		defer os.Remove(keyFile + ".lock")
		if _, err := os.Stat(keyFile); err == nil {
			return 0 // built by another builder in the meantime
		}
		if err := buildKeyFile(os.Getenv(keyBuilderCacheEnv), os.Getenv(keyBuilderColumnEnv), keyFile); err != nil {
			return 1
		}
		return 0
	}

	cacheFile := os.Getenv(cacheBuilderEnv)
	defer os.Remove(cacheFile + ".lock")
	// A CLI run exits right after printing its answer and frees the memory
	// of its own load; measure free memory only then, so the builder can
	// take the fast in-memory path. A session that stays open keeps its
	// memory, and the builder takes the safe path.
	if parent, err := strconv.Atoi(os.Getenv(builderParentEnv)); err == nil {
		for deadline := time.Now().Add(3 * time.Second); processAlive(parent) && time.Now().Before(deadline); {
			time.Sleep(20 * time.Millisecond)
		}
	}
	source := os.Getenv(cacheBuilderSourceEnv)
	comma := []rune(os.Getenv(cacheBuilderCommaEnv))
	headers := strings.Split(os.Getenv(cacheBuilderHeaderEnv), "\x1f")
	if source == "" || len(comma) != 1 {
		return 2
	}
	hasHeader := os.Getenv(cacheBuilderNoHdrEnv) != "true"
	if err := buildCSVCache(cacheFile, source, comma[0], headers, hasHeader); err != nil {
		return 1
	}
	removeStaleCaches(filepath.Dir(cacheFile), strings.SplitN(filepath.Base(cacheFile), "-", 2)[0], cacheFile)
	// The cache is usable now; build the key tables right after, still in
	// the background, so lookups are fast without anyone waiting for them.
	os.Remove(cacheFile + ".lock")
	buildAllKeyFiles(cacheFile)
	return 0
}

// buildAllKeyFiles builds the key file of every qualifying column of a
// cache that does not have one yet. Each takes the same lock as an
// on-demand build, so the two never build the same file twice.
func buildAllKeyFiles(cacheFile string) {
	db, err := openDuckDBAt(cacheFile + "?access_mode=READ_ONLY")
	if err != nil {
		return
	}
	var columns []keyColumn
	if rows, err := db.Query("select ordinal, name from jtv_key_candidates order by ordinal"); err == nil {
		for rows.Next() {
			var column keyColumn
			if rows.Scan(&column.ordinal, &column.name) == nil {
				columns = append(columns, column)
			}
		}
		rows.Close()
	}
	// Close before building: the key builder attaches the cache itself.
	db.Close()
	for _, column := range columns {
		keyFile := keyFilePath(cacheFile, column.ordinal)
		if _, err := os.Stat(keyFile); err == nil {
			continue
		}
		lock := keyFile + ".lock"
		lockFile, err := os.OpenFile(lock, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err != nil {
			continue // an on-demand builder is on it
		}
		// The pid marks the lock as held by a live builder.
		lockFile.WriteString(strconv.Itoa(os.Getpid()))
		lockFile.Close()
		buildKeyFile(cacheFile, column.name, keyFile)
		os.Remove(lock)
	}
}

// removeAbandonedWork deletes work files and spill directories left by
// builders that died, once they are older than any live builder could be.
func removeAbandonedWork(dir string) {
	for _, pattern := range []string{".work-*", ".key-work-*", ".building-*", ".spill-*"} {
		matches, _ := filepath.Glob(filepath.Join(dir, pattern))
		for _, match := range matches {
			if info, err := os.Stat(match); err == nil && time.Since(info.ModTime()) > staleCacheLock {
				os.RemoveAll(match)
			}
		}
	}
}

// lockOutput takes output's builder lock for this process, so other runs
// wait instead of building the same file, and returns its release.
func lockOutput(output string) func() {
	lock := output + ".lock"
	if err := os.MkdirAll(filepath.Dir(lock), 0o755); err != nil {
		return func() {}
	}
	file, err := os.OpenFile(lock, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return func() {}
	}
	file.WriteString(strconv.Itoa(os.Getpid()))
	file.Close()
	return func() { os.Remove(lock) }
}

// builderWaitLimit bounds how long a run waits for a running cache builder
// before loading the file itself.
const builderWaitLimit = 5 * time.Minute

// waitForBuilder waits while a live builder holds output's lock and reports
// whether output exists afterwards. It returns at once when no builder is
// running or the lock's builder has died.
func waitForBuilder(output string) bool {
	deadline := time.Now().Add(builderWaitLimit)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(output); err == nil {
			return true
		}
		if !builderRunning(output + ".lock") {
			_, err := os.Stat(output)
			return err == nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// builderRunning reports whether lock is held by a builder process that is
// still alive.
func builderRunning(lock string) bool {
	data, err := os.ReadFile(lock)
	if err != nil {
		return false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		// Just created; the starter writes the pid right after.
		info, statErr := os.Stat(lock)
		return statErr == nil && time.Since(info.ModTime()) < 5*time.Second
	}
	return processAlive(pid)
}
