# jtv

`jtv` is a JSON/CSV viewer and SQL query tool with table, CSV, JSON, and
streaming output.

It accepts JSON arrays, single JSON values/objects, NDJSON/JSON Lines, and CSV.
Data is loaded into an in-memory SQLite table named `input`. Nested JSON fields
are flattened into dot-path columns, so fields can be queried as `user.name`
without quoting in most queries.

The table name is optional. `select *` automatically means
`select * from input`, and the internal `raw` column is hidden from `*`.

## Install

```bash
go install github.com/srcodee/jtv/cmd/jtv@latest
```

## Build

```bash
go build -buildvcs=false -o jtv ./cmd/jtv
go build -buildvcs=false -o jtv-mcp ./cmd/jtv-mcp
```

## Usage

```bash
./jtv -f data.json
./jtv -f data.ndjson
./jtv -f data.csv
cat data.json | ./jtv
./jtv -f data.json -q "select *"
./jtv -f data.json -q "select user.name, count(*) group by user.name"
cat data.ndjson | ./jtv -q "select status, count(*) group by status"
curl -s https://dummyjson.com/comments | ./jtv
tail -f ok.txt | ./jtv --stream -q "select time, status, duration_s, db_count"
./jtv -f data.json -q "select user.name, order.total" --csv
./jtv -f data.json -q "select user.name, order.total" --json
./jtv -f data.json -q "select user.name, order.total" -o result.csv
./jtv -f data.json -q "select user.name, order.total" -o result.json
./jtv --version
```

Use `-f` for files. Piped stdin is detected automatically, so `curl ... | ./jtv`
works without `-f -`. Positional input files are rejected; use `-f path`.

Without `-q`, `jtv` starts an interactive prompt with SQL autocomplete.

## Flags

```text
-f FILE     input JSON, NDJSON, or CSV file; use - for stdin
-q SQL      SQL query to execute
-o FILE     write query output to file; .csv and .json are supported
--csv       write query output as CSV
--json      write query output as pretty JSON
--stream    read NDJSON continuously and run -q for each line
--version   print version and exit
-config FILE
            read defaults from FILE instead of the user config path
--no-config ignore config file
```

If no output flag is set, results are printed as an ASCII table. With `-o`, the
format is inferred from the file extension. An empty or `.csv` extension writes
CSV; `.json` writes JSON.

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

CSV input requires a header row and at least one data row:

```csv
id,status,user.name,user.active
1,ok,Ana,1
2,fail,Budi,0
```

CSV headers are trimmed. Empty headers become `column_N`, duplicate headers get
a numeric suffix, and scalar CSV values are parsed as booleans, integers, or
floats when possible.

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
export FILE           export by extension: .csv or .json
clear                 clear the screen
exit, quit            exit
```

Interactive `select` queries without an explicit top-level `limit` are paged.
The default page size is 10. Use `next`, `prev`, `page N`, and `pagesize N` to
navigate results. Query history is available through the up/down arrow keys
when autocomplete is available.

Dot commands such as `.help`, `.ls`, `.schema`, `.preview`, `.csv`, `.json`,
`.clear`, `.exit`, and `.quit` are also supported.

## Query Model

Rows are available through the implicit `input` table:

```sql
select *;
select user.name, user.email where user.active = true;
select status, count(*) group by status order by count(*) desc;
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

Inspect nested arrays:

```bash
./jtv -f examples/orders.json -q "select id, orders.id, orders.users.name"
./jtv -f examples/orders.json -q "select orders.users.name, count(*) as total group by orders.users.name"
```

Export results:

```bash
./jtv -f examples/users.json -q "select id, user.name, status" --csv
./jtv -f examples/users.json -q "select id, user.name, status" --json
./jtv -f examples/users.json -q "select id, user.name, status" -o users.csv
./jtv -f examples/users.json -q "select id, user.name, status" -o users.json
```

Explore interactively:

```bash
./jtv -f examples/users.json
```

## MCP Server

`jtv` also includes an MCP server for AI clients that can call tools over stdio:

```bash
go build -buildvcs=false -o jtv-mcp ./cmd/jtv-mcp
./jtv-mcp
```

Available tools:

```text
jtv_query    query inline data or a local file with SQL
jtv_schema   list detected flattened fields
jtv_preview  preview the first rows
jtv_stream_query
             run a query independently for each NDJSON line
```

Each tool accepts either `data` or `file_path`. `jtv_query` also requires
`query`.

Example `tools/call` request:

```json
{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"jtv_query","arguments":{"file_path":"examples/users.json","query":"select user.name, count(*) as total group by user.name"}}}
```

Example stream query request:

```json
{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"jtv_stream_query","arguments":{"data":"{\"time\":\"t1\",\"status\":\"ok\"}\n{\"time\":\"t2\",\"status\":\"fail\"}","query":"select time, status"}}}
```
