package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
)

// CSV cache.
//
// Loading a large CSV file means parsing and typing every row. The loaded
// table is therefore saved as a DuckDB database file in the user cache
// directory, and later runs on the same unchanged file open that database
// instead. The cache key covers the file's path, size and modification time,
// the delimiter, the header and the DuckDB version, so editing the file or
// upgrading jtv builds a fresh cache. Each source file keeps one cache file.

// csvCacheMinSize is the smallest file worth caching; small files load
// faster than a cache can be checked.
var csvCacheMinSize int64 = 8 << 20

// csvCacheDisabled is set by --no-cache.
var csvCacheDisabled bool

// backgroundCacheBuild makes the first load of a file answer from memory
// while a detached jtv process writes the cache, instead of making the
// user wait for the cache to be written. Tests build it synchronously.
var backgroundCacheBuild = true

const csvCacheFormat = "jtv-csv-cache-5"

func csvCacheDir() (string, bool) {
	if csvCacheDisabled || os.Getenv("JTV_NO_CACHE") != "" {
		return "", false
	}
	if dir := os.Getenv("JTV_CACHE_DIR"); dir != "" {
		return dir, true
	}
	base, err := os.UserCacheDir()
	if err != nil {
		return "", false
	}
	return filepath.Join(base, "jtv"), true
}

// loadCSVCached returns the dataset for a local CSV file from the cache,
// building the cache first when needed. ok is false when caching does not
// apply or fails; the caller then loads the file without a cache, and calls
// afterLoad, when set, once that load is done to build the cache in the
// background without competing with it for CPU.
func loadCSVCached(path string, comma rune, headers []string, hasHeader bool, source string) (ds *Dataset, ok bool, afterLoad func()) {
	dir, enabled := csvCacheDir()
	if !enabled {
		return nil, false, nil
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() < csvCacheMinSize {
		return nil, false, nil
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, false, nil
	}
	pathKey := shortHash(absPath)
	contentKey := shortHash(strings.Join([]string{
		csvCacheFormat,
		duckDBVersion(),
		fmt.Sprint(info.Size()),
		fmt.Sprint(info.ModTime().UnixNano()),
		string(comma),
		strconv.FormatBool(hasHeader),
		strings.Join(headers, "\x00"),
	}, "\x01"))
	cacheFile := filepath.Join(dir, pathKey+"-"+contentKey+".duckdb")

	if ds, err := openCachedCSV(cacheFile, headers, source); err == nil {
		return ds, true, nil
	}
	// A builder already working on this cache finishes sooner if this run
	// waits for it than if it loads the file again next to it, competing
	// for CPU (on 100 million rows each such load took 16 seconds and kept
	// the cache from appearing for over a minute).
	if waitForBuilder(cacheFile) {
		if ds, err := openCachedCSV(cacheFile, headers, source); err == nil {
			return ds, true, nil
		}
	}
	if backgroundCacheBuild && enoughMemoryToBuild(info.Size()) {
		return nil, false, func() { startCacheBuilder(cacheFile, path, comma, headers, hasHeader) }
	}
	// A file too big to load in memory is loaded on disk anyway, so that
	// load builds the cache itself instead of building it twice. Key tables
	// still follow in the background.
	unlock := lockOutput(cacheFile)
	err = buildCSVCache(cacheFile, path, comma, headers, hasHeader)
	unlock()
	if err != nil {
		return nil, false, nil
	}
	removeStaleCaches(dir, pathKey, cacheFile)
	ds, err = openCachedCSV(cacheFile, headers, source)
	if err != nil {
		return nil, false, nil
	}
	if backgroundCacheBuild {
		afterLoad = func() {
			for _, column := range ds.keyCandidates {
				startKeyBuilder(cacheFile, column.name, keyFilePath(cacheFile, column.ordinal))
			}
		}
	}
	return ds, true, afterLoad
}

func openCachedCSV(cacheFile string, headers []string, source string) (*Dataset, error) {
	if _, err := os.Stat(cacheFile); err != nil {
		return nil, err
	}
	db, err := openDuckDBAt(cacheFile + "?access_mode=READ_ONLY")
	if err != nil {
		return nil, err
	}
	ds, err := csvDatasetFromDB(db, headers, source)
	if err != nil {
		db.Close()
		return nil, err
	}
	ds.cacheFile = cacheFile
	ds.keyTables, ds.keyCandidates = loadKeyTables(db, cacheFile)
	return ds, nil
}

// buildCSVCache loads the CSV file into a new database file and moves it
// into place only once it is complete, so readers never see a partial cache.
func buildCSVCache(cacheFile, path string, comma rune, headers []string, hasHeader bool) (err error) {
	dir := filepath.Dir(cacheFile)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	workName, err := reserveTempPath(dir, ".work-*.duckdb")
	if err != nil {
		return err
	}
	finalName, err := reserveTempPath(dir, ".building-*.duckdb")
	if err != nil {
		return err
	}
	spillDir, err := os.MkdirTemp(dir, ".spill-")
	if err != nil {
		return err
	}
	defer func() {
		removeDuckDBFiles(workName)
		os.RemoveAll(spillDir)
		if err != nil {
			removeDuckDBFiles(finalName)
		}
	}()

	// With enough free memory the data is built in memory, as fast as a
	// normal load. Otherwise (an interactive session may already hold the
	// data) the builder works in a disk-backed database with a memory limit
	// instead of loading everything into RAM again: on 100 million rows 9
	// seconds against 35. Either way only the result is copied into the
	// cache file: tables from discarded typed read attempts would otherwise
	// leave dead space (219MB instead of 70MB for 10 million rows).
	var db *sql.DB
	if info, statErr := os.Stat(path); statErr == nil && enoughMemoryToBuild(info.Size()) {
		db, err = openDuckDB()
	} else {
		db, err = openDuckDBAt(workName)
		if err == nil {
			for _, setting := range []string{
				"set memory_limit = " + sqlString(buildMemoryLimit()),
				"set temp_directory = " + sqlString(spillDir),
			} {
				if _, err = db.Exec(setting); err != nil {
					break
				}
			}
		}
	}
	if err != nil {
		if db != nil {
			db.Close()
		}
		return err
	}
	defer db.Close()
	if err := buildCSVTables(db, path, comma, headers, hasHeader, true); err != nil {
		return err
	}
	if err := recordKeyCandidates(db, headers); err != nil {
		return err
	}
	var catalog string
	if err := db.QueryRow("select current_database()").Scan(&catalog); err != nil {
		return err
	}
	if _, err := db.Exec("attach " + sqlString(finalName) + " as jtv_cache"); err != nil {
		return err
	}
	if _, err := db.Exec("copy from database " + quoteIdent(catalog) + " to jtv_cache"); err != nil {
		return err
	}
	if _, err := db.Exec("detach jtv_cache"); err != nil {
		return err
	}
	os.Remove(finalName + ".wal")
	return os.Rename(finalName, cacheFile)
}

// buildMemoryLimit is the DuckDB memory limit for loads and builds that
// work on disk: half the free memory, at least 1GB. DuckDB spills the rest
// to disk.
func buildMemoryLimit() string {
	limit := int64(1 << 30)
	if available, ok := availableMemory(); ok && available/2 > limit {
		limit = available / 2
	}
	return fmt.Sprintf("%dMB", limit>>20)
}

// availableMemory returns the memory the system can still hand out.
// JTV_MEM_AVAILABLE (bytes) overrides it, to test large-file behavior on
// small files.
func availableMemory() (int64, bool) {
	if override := os.Getenv("JTV_MEM_AVAILABLE"); override != "" {
		n, err := strconv.ParseInt(override, 10, 64)
		return n, err == nil
	}
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "MemAvailable:" {
			availableKB, err := strconv.ParseInt(fields[1], 10, 64)
			return availableKB * 1024, err == nil
		}
	}
	return 0, false
}

// enoughMemoryToBuild reports whether the system has room to build the cache
// of a CSV file of the given size in memory. A load peaks at about 2.3 times
// the file size (4.2GB for 1.85GB); 1GB more is kept free for everything
// else. Without /proc/meminfo it plays safe.
func enoughMemoryToBuild(fileSize int64) bool {
	available, ok := availableMemory()
	return ok && available >= fileSize*5/2+1<<30
}

// reserveTempPath returns an unused path in dir for DuckDB to create.
func reserveTempPath(dir, pattern string) (string, error) {
	tmp, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return "", err
	}
	name := tmp.Name()
	tmp.Close()
	os.Remove(name) // DuckDB creates the file itself
	return name, nil
}

func removeDuckDBFiles(name string) {
	os.Remove(name)
	os.Remove(name + ".wal")
}

// removeStaleCaches deletes older caches of the same source file.
func removeStaleCaches(dir, pathKey, keep string) {
	matches, _ := filepath.Glob(filepath.Join(dir, pathKey+"-*.duckdb"))
	// Key files of the kept cache are named after it and stay.
	keepPrefix := strings.TrimSuffix(keep, ".duckdb") + "."
	for _, match := range matches {
		if match != keep && !strings.HasPrefix(match, keepPrefix) {
			os.Remove(match)
			os.Remove(match + ".wal")
		}
	}
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:20]
}

// duckDBVersion identifies the DuckDB build, because database files written
// by one version are not guaranteed to open in another.
func duckDBVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dep := range info.Deps {
		if dep.Path == "github.com/duckdb/duckdb-go/v2" {
			return dep.Version
		}
	}
	return "unknown"
}

// csvDatasetFromDB describes a database built by buildCSVTables, reading
// the field metadata recorded in jtv_meta.
func csvDatasetFromDB(db *sql.DB, headers []string, source string) (*Dataset, error) {
	rows, err := db.Query("select name, type, sample from jtv_meta order by ordinal")
	if err != nil {
		return nil, err
	}
	columnTypes := map[string]string{}
	fieldSamples := map[string]string{}
	for rows.Next() {
		var name, typ string
		var sample sql.NullString
		if err := rows.Scan(&name, &typ, &sample); err != nil {
			rows.Close()
			return nil, err
		}
		columnTypes[name] = typ
		if sample.Valid && sample.String != "" {
			fieldSamples[name] = sample.String
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, header := range headers {
		if _, ok := columnTypes[header]; !ok {
			return nil, fmt.Errorf("cached table is missing column %q", header)
		}
	}
	var rowCount int
	if err := db.QueryRow("select n from jtv_row_count").Scan(&rowCount); err != nil {
		return nil, err
	}
	if rowCount == 0 {
		return nil, fmt.Errorf("CSV input requires a header and at least one data row")
	}

	fieldTypes := make(map[string]string, len(headers))
	fieldSet := map[string]struct{}{"raw": {}}
	textFields := map[string]bool{"raw": true}
	for _, header := range headers {
		fieldSet[header] = struct{}{}
		fieldTypes[header] = jtvTypeName(columnTypes[header])
		if columnTypes[header] == "VARCHAR" {
			textFields[strings.ToLower(header)] = true
		}
	}
	fields := sortedKeys(fieldSet)
	arrayPaths := map[string]struct{}{}
	return &Dataset{
		Source:       source,
		Fields:       fields,
		FieldLabels:  buildFieldLabels(fields, arrayPaths),
		FieldTypes:   fieldTypes,
		FieldSamples: fieldSamples,
		ArrayFields:  buildArrayFields(fields, arrayPaths),
		RowCount:     rowCount,
		db:           db,
		textFields:   textFields,
		csv:          true,
	}, nil
}
