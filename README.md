# jtv

`jtv` is a JSON/CSV viewer and SQL query tool with table, CSV, JSON, and
streaming output.

It accepts JSON arrays, single JSON values/objects, NDJSON/JSON Lines, and CSV.
Data is loaded into an in-memory [DuckDB](https://duckdb.org) table named
`input`, so queries run on a parallel, columnar engine: grouping 10 million CSV
rows takes a fraction of a second. Nested JSON fields are flattened into
dot-path columns, so fields can be queried as `user.name` without quoting in
most queries.

The table name is optional. `select *` automatically means
`select * from input`, and the internal `raw` column is hidden from `*`.

## Install

DuckDB is linked through cgo, so a C/C++ toolchain is required (for example
`apt install build-essential`). jtv builds on Linux and macOS.

```bash
CGO_ENABLED=1 go install github.com/srcodee/jtv/cmd/jtv@latest
```

## Build

```bash
CGO_ENABLED=1 go build -buildvcs=false -o jtv ./cmd/jtv
```

## Usage

```bash
./jtv -f data.json
./jtv -f https://example.test/api/users
./jtv -f data.ndjson
./jtv -f data.csv
cat data.json | ./jtv
./jtv -f data.json -q "select *"
./jtv -f https://example.test/api/users -q "select id, user.name"
./jtv -f https://example.test/api/users --header "Authorization: Bearer ..." -q "select id, user.name"
./jtv -f data.json -q "select user.name, count(*) group by user.name"
cat data.ndjson | ./jtv -q "select status, count(*) group by status"
curl -s https://dummyjson.com/comments | ./jtv
tail -f ok.txt | ./jtv --stream -q "select time, status, duration_s, db_count"
./jtv -f data.json -q "select user.name, order.total" --csv
./jtv -f data.json -q "select user.name, order.total" --json
./jtv -f data.json -q "select user.name, order.total" --md
./jtv -f data.json -q "select user.name, order.total" -o result.csv
./jtv -f data.json -q "select user.name, order.total" -o result.json
./jtv -f data.json -q "select user.name, order.total" -o result.md
./jtv --version
```

Use `-f` for files. Piped stdin is detected automatically, so `curl ... | ./jtv`
works without `-f -`. Positional input files are rejected; use `-f path`.

Without `-q`, `jtv` starts an interactive prompt with SQL autocomplete.

## Flags

```text
-f FILE     input JSON, NDJSON, or CSV file; use - for stdin
-q SQL      SQL query to execute
-o FILE     write query output to file; .csv, .json, and .md are supported
--md        write query output as Markdown
--method METHOD
            HTTP method for -f URL or request file
--header HEADER
            HTTP header for -f URL or request file; can be repeated
--data BODY
            HTTP request body for -f URL or request file
--delimiter DELIM
            CSV delimiter: auto, comma, semicolon, pipe, tab, or one character
--root FIELD
            use a JSON field as the dataset root
--show-request
            print parsed HTTP request without fetching it
--debug-request
            print safe HTTP request/response debug info
--save-response FILE
            write fetched HTTP response body to file
--csv       write query output as CSV
--json      write query output as pretty JSON
--stream    read NDJSON continuously and run -q for each line
--version   print version and exit
-config FILE
            read defaults from FILE instead of the user config path
--no-config ignore config file
--no-cache  do not read or write the cache for large CSV files
--csv-header auto|yes|no
            whether a CSV starts with a header row (default: detect)
```

If no output flag is set, results are printed as an ASCII table. With `-o`, the
format is inferred from the file extension. An empty or `.csv` extension writes
CSV; `.json` writes JSON; `.md` writes Markdown.

`--csv` and `--json` override the configured default output format for a single
command.

`-q` also accepts `ls`, `schema`, `.schema`, and `preview [N]` for quick
non-interactive inspection.

## Input

JSON input can be an array, object, scalar, or NDJSON stream:

```json
[
  {"id": 1, "user": {"name": "Ana", "active": true}},
  {"id": 2, "user": {"name": "Budi", "active": false}}
]
```

CSV input usually starts with a header row:

```csv
id,status,user.name,user.active
1,ok,Ana,1
2,fail,Budi,0
```

Files without a header row are detected: when the first row looks like the
data below it (numbers above numbers, not a name above numbers), it is kept
as data and the columns are named `col1`, `col2`, ... Files of text columns
only cannot be told apart and are read with a header. Use `--csv-header yes`
or `--csv-header no` to decide yourself.

Give columns better names with `rename`, in interactive mode or with `-q`.
Names are saved for the file and used every time it is opened, also by
`jtv serve`; renaming is instant and does not rebuild any cache:

```bash
jtv -f crm.csv -q "rename col1 phone"
jtv -f crm.csv -q "rename col2 code"
jtv -f crm.csv -q "select * where phone = 620000000040"
jtv -f crm.csv -q "rename"        # list renamed fields
jtv -f crm.csv -q "rename reset"  # back to col1, col2
```

Saved names live in the user config directory (`~/.config/jtv/columns` on
Linux).

CSV headers are trimmed. Empty headers become `column_N`, duplicate headers get
a numeric suffix, and values are trimmed.

Each CSV column gets one type: integer when every value is an integer, number
when every value is numeric, boolean (stored as `1`/`0`) when every value is
`true`/`false`, and text otherwise. Empty cells are `NULL`; `field = ''` on a
numeric column is read as `field is null`. A column that holds mostly numbers
and a few words, such as phone numbers with test entries, is text, but still
compares with numbers the way you would expect:

```sql
select * where phone = 620000000040;        -- matches the text '620000000040'
select count(*) where phone > 620009000000; -- compares numerically, skipping words
select phone order by phone desc;           -- numbers in numeric order, then words
```

### Large CSV files

Large local CSV files (8MB and up) are read by DuckDB directly and cached as a
DuckDB database file in the user cache directory (`~/.cache/jtv` on Linux).
The first run answers from memory while a background process writes the
cache; later runs open the cache instead of parsing the file again. A file of
10 million rows loads in about 1.5 seconds the first time and about 0.1
seconds afterwards. The cache is rebuilt automatically when the file changes.
Use `--no-cache` (or `JTV_NO_CACHE=1`) to skip it, and `JTV_CACHE_DIR` to move
it.

The cache can also keep a sorted copy of each mostly-unique column, such as
phone numbers or ids, so lookups like `phone = 620000000040`, `phone in (...)` and
`phone like '62000%'` read only the blocks holding the matches: finding one
number among 100 million rows takes about 0.13 seconds. The background builder
works on disk with a 1GB memory limit, so it can run next to an interactive
session holding the data.

Files larger than the free memory still load: when a file would not fit in
memory (a load needs about 2.5 times the file size), jtv loads it into a
DuckDB database on disk with a memory limit of half the free memory, and
when caching is on, that load is the cache itself. A 100 million row file
with only 2GB free took 29 seconds and 1.2GB the first time, instead of 9
seconds and 4.2GB in memory; later runs use the cache as usual.

Results of `-q` are written as they are produced, so exporting all rows of a
large file needs about 1GB of memory rather than memory for the whole result:

```bash
jtv -f big.csv -q "select * where status = 'ok'" -o ok.csv
```

CSV files written with `-o` are produced by DuckDB itself when the result has
only integer, boolean and text columns, about ten times faster (10 million
rows in under a second). The file is byte for byte what jtv's own writer
produces; results with decimals, dates or text starting with a space use
jtv's writer.

### Keeping a file loaded

Each run starts a process and opens the data, which costs about 0.1 seconds
even when the query itself takes a few milliseconds. When you run many queries
against the same file, keep it loaded:

```bash
jtv serve -f big.csv            # loads once, then waits in the background
jtv -f big.csv -q "select ..."  # answered by the server in ~20-40ms
jtv stop -f big.csv
```

`jtv -f FILE -q ...` uses a running server for FILE automatically and runs
locally otherwise, so scripts need no change. The server stops after 30
minutes without queries (`--idle 2h`, or `--idle 0` to keep it), and it
stops by itself when FILE changes so stale data is never served.

After the cache, the same background process builds the lookup tables for
mostly-unique columns. Until they are ready a lookup scans the column (about
1-3 seconds on 100 million rows); on a new 100 million row file they are ready
about a minute after the first run, with nobody having to search first. A run
started while the cache is being built waits for it instead of loading the
file a second time. The builders work in memory when the system has room for
it and on disk within 1GB otherwise, for example next to an open interactive
session holding the data.

Measured on a 100 million row, 1.85GB pipe-delimited file (WSL2, 12 cores,
6.8GB RAM): first load 9 seconds; with the cache, `count(*)` 0.13 seconds,
`group by` 0.4 seconds, one-number lookup 0.13 seconds (0.04 seconds through
`jtv serve`); exporting 41 million filtered rows to CSV 33 seconds using under
1GB.

CSV delimiters are auto-detected from comma, semicolon, pipe (`|`), and tab.
Use `--delimiter` to force a delimiter:

```bash
./jtv -f data.psv --delimiter '|' -q "select id, name"
./jtv -f data.tsv --delimiter tab -q "select id, name"
```

`-f` can read public HTTP(S) URLs directly:

```bash
./jtv -f https://example.test/api/users -q "select id, user.name"
./jtv -f https://example.test/api/users --root data -q "select id, email"
```

`-f` can also read a file containing an HTTP request and use the response as
the dataset. Supported request formats are:

```text
curl 'https://example.test/api/users' \
  -H 'Accept: application/json' \
  -H 'X-Token: ...' \
  -b 'session=...'

curl ^"https://example.test/api/users^" ^
  -H ^"Accept: application/json^" ^
  -b ^"session=...^"

Invoke-WebRequest -Uri "https://example.test/api/users" `
  -Headers @{"Accept"="application/json"} `
  -ContentType "application/json"

GET /api/users HTTP/1.1
Host: example.test
Accept: application/json
Cookie: session=...
```

Request files are parsed and fetched by `jtv`; they are not executed as shell
scripts. Keep request files private because they often contain cookies, session
tokens, or authorization headers.

Request helpers:

```bash
./jtv -f request.txt --show-request
./jtv -f request.txt --debug-request -q "select * limit 1"
./jtv -f request.txt --save-response response.json -q "select status"
./jtv -f response.json -q "select status"
./jtv -f https://example.test/api/users --method POST --header "Authorization: Bearer ..." --data '{"active":true}' -q "select id"
```

`--show-request` and `--debug-request` redact sensitive header names such as
`Cookie` and `Authorization`.

Default headers can be stored in the config file by host. Request files override
configured defaults when they provide the same header:

```toml
[headers.example.test]
User-Agent = "jtv"
Authorization = "Bearer ..."
```

## Stream Mode

`--stream` reads one NDJSON object per line and runs the query against each line
as its own dataset:

```bash
tail -f ok.txt | ./jtv --stream -q "select time, status, duration_s, db_count"
tail -f ok.txt | ./jtv --stream --csv -q "select time, status, duration_s"
tail -f ok.txt | ./jtv --stream --json -q "select time, duration_s, db_count"
```

Stream mode requires `-q`. It does not support `-o`; redirect stdout instead.
Invalid lines or query errors are reported to stderr and the stream continues.
With `--json`, stream output is JSON Lines. With `--csv`, the header is written
once.

Lines that are already waiting, such as a whole log file or a burst from
`tail -f`, are answered in batches when the result is guaranteed to be the
same as line by line: consecutive lines with the same fields and types, and
a query that only selects and filters rows. 20,000 log lines then take about
half a second instead of 20-45 seconds. Queries that aggregate, sort, limit
or de-duplicate per line, lines with arrays, and Markdown output still run
line by line, and a line arriving on its own is answered right away.

## Config

`jtv` reads optional defaults from `~/.config/jtv/config.toml`:

```toml
output = "table"  # table, csv, or json
pagesize = 25
```

Use `-config path/to/config.toml` to choose another config file or `--no-config`
to ignore config. CLI flags always win over config values.

## Interactive Commands

```text
help                  show help
ls                    list detected fields
schema                list detected fields
ls TEXT               search fields
ls TEXT -n 20         search fields and limit output
preview [N]           show the first N rows
next, n               show the next page
prev, p               show the previous page
page N                jump to page N
pagesize N            set page size
sum FIELD             sum numeric values
max FIELD             show maximum numeric value
min FIELD             show minimum numeric value
avg FIELD             show average numeric value
bar FIELD             count by FIELD as a bar chart
top FIELD [N]         top values by count
hist FIELD            histogram for numeric FIELD
line X Y              sparkline trend
line full X Y         multiline connected line
line points X Y       point/scatter view
bar SELECT ...        bar chart from SQL label/value columns
chart bar SELECT ...  same as bar SELECT ...
csv FILE              export the last query result to CSV
json FILE             export the last query result to JSON
export FILE           export by extension: .csv, .json, or .md
clear                 clear the screen
exit, quit            exit
```

Interactive `select` queries without an explicit top-level `limit` are paged.
The default page size is 10. Use `next`, `prev`, `page N`, and `pagesize N` to
navigate results. Query history is available through the up/down arrow keys
when autocomplete is available.

Dot commands such as `.help`, `.ls`, `.schema`, `.preview`, `.csv`, `.json`, `.md`,
`.clear`, `.exit`, and `.quit` are also supported.

## Query Model

Rows are available through the implicit `input` table:

```sql
select *;
select user.name, user.email where user.active = true;
select status, count(*) group by status order by count(*) desc;
select status, count(*) as total group by status having total > 1;
select id, user.name from input where user.name like 'A%';
```

Nested objects are flattened into dot-path columns. Arrays are auto-expanded
into rows, but query paths stay simple:

```sql
select id, orders.users.id;
```

`ls` shows array paths with `[]` so complex JSON is easier to inspect:

```text
orders[].users[].id
```

Scalar top-level fields are de-duplicated automatically for simple field-only
selects when the data has been expanded through arrays:

```sql
select total;
select total, comments.id;
```

The first query returns one distinct `total` value. The second query includes an
array field, so it returns one row per expanded array item.

The original row is also available as `raw`, but it is not included in
`select *`.

Formatted numeric strings can be converted inside SQL with `int()`, `float()`,
`number()`, or `money()`. This is useful for values such as `Rp. 20.000` that
should be used with comparisons or aggregate functions:

```sql
select product.name where int(product.price) > 10000;
select product.name where money(product.price) <= 7500;
select sum(int(product.price)), max(int(product.price)), min(float(product.price));
```

The converter ignores surrounding text and currency symbols, understands
thousand separators like `20.000`, and supports decimal formats such as
`1.234,50`.

Common SQL and helper functions:

```text
count(*)       count rows
sum(value)     sum numeric values
avg(value)     average numeric values
min(value)     minimum value
max(value)     maximum value
int(value)     convert formatted text to an integer
float(value)   convert formatted text to a decimal number
number(value)  convert formatted text to a decimal number
money(value)   alias for number(value), useful for currency strings
date(value)    normalize a date/time to YYYY-MM-DD
year(value)    extract the year from a date/time
month(value)   extract the month number from a date/time
regexp_like(value, pattern)
              return true when value matches a regular expression
```

Shortcut query commands are available in `-q` and interactive mode:

```text
fields         alias for schema
head [N]       alias for preview [N]
uniq FIELD     distinct values
count [FIELD]  count rows, or count rows by FIELD
```

Queries run on DuckDB with a few SQLite-compatible adjustments, so queries
written for earlier versions keep working: `like` is case-insensitive,
double-quoted words that are not field names are strings (`like "62000%"`),
groups without `order by` come back sorted by key, and rows without
`order by` keep file order. Two differences remain: `/` divides exactly
(`5/2` is `2.5`), and `typeof()` returns DuckDB type names such as `BIGINT`.

If a query references a field that does not exist, `jtv` suggests the nearest
detected field when it can:

```text
no such column: user.nmae; did you mean user.name?
```

## Charts

Chart commands are available in interactive mode:

```text
bar comments.likes
top comments.user.username 10
hist comments.likes
line comments.id comments.likes
line full comments.id comments.likes
line points comments.id comments.likes
line select comments.id, comments.likes order by comments.id
line full select comments.id, comments.likes order by comments.id
bar select comments.likes, count(*) as total group by comments.likes
chart bar select comments.user.username, count(*) as total group by comments.user.username order by total desc limit 10
```

`bar` expects two columns when given a SQL query: a label and a numeric value.
`hist` and `line` require numeric values.

## Examples

This repository includes sample files:

```bash
./jtv -f examples/users.json -q "select id, user.name, status"
./jtv -f examples/users.csv -q "select status, count(*) group by status"
./jtv -f examples/orders.json -q "select id, orders.users.id"
```

Filter rows:

```bash
./jtv -f examples/users.json -q "select id, user.name where status = 'ok'"
./jtv -f examples/users.csv -q "select id, user.name where user.active = 1"
```

Group and sort values:

```bash
./jtv -f examples/users.json -q "select user.name, count(*) as total group by user.name order by total desc"
./jtv -f examples/users.csv -q "select status, count(*) as total group by status order by total desc"
```

Work with formatted prices:

```bash
./jtv -f products.json -q "select rows.product, int(rows.price) as price where int(rows.price) > 10000"
./jtv -f products.json -q "select min(int(rows.price)) as cheapest, max(int(rows.price)) as highest, sum(int(rows.price)) as total"
./jtv -f products.json -q "sum rows.price"
./jtv -f products.json -q "max rows.price where int(rows.price) > 10000"
```

Inspect nested arrays:

```bash
./jtv -f examples/orders.json -q "select id, orders.id, orders.users.name"
./jtv -f examples/orders.json -q "select orders.users.name, count(*) as total group by orders.users.name"
```

Export results:

```bash
./jtv -f examples/users.json -q "select id, user.name, status" --csv
./jtv -f examples/users.json -q "select id, user.name, status" --json
./jtv -f examples/users.json -q "select id, user.name, status" --md
./jtv -f examples/users.json -q "select id, user.name, status" -o users.csv
./jtv -f examples/users.json -q "select id, user.name, status" -o users.json
./jtv -f examples/users.json -q "select id, user.name, status" -o users.md
```

Explore interactively:

```bash
./jtv -f examples/users.json
```
