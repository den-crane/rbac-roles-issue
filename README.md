# rbac-roles-issue

Naive test to reproduce https://github.com/ClickHouse/ClickHouse/issues/34412

## initialize

```bash
docker run --rm  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 -e CLICKHOUSE_PASSWORD=abc -p 9000:9000/tcp clickhouse/clickhouse-server:22.3.9

pip install --upgrade clickhouse-driver
```

## run

```bash
python3 cl-roles-issue.py
```
