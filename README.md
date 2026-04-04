# MT5 Data Pipeline for AI/ML Training

A production-grade MetaTrader 5 data pipeline that connects to multiple broker terminals, collects every available data category, builds canonical merged tick streams, constructs bars for all timeframes, and exports model-ready datasets for training trading bots and neural networks.

## Features

- **Dual-broker ingestion** — connect to 2+ MT5 terminals, collect raw ticks with full provenance
- **Historical backfill** — chunked download with checkpoint-resume, gap detection
- **Live collection** — continuous tick polling with graceful shutdown and periodic snapshots
- **Canonical merge** — quality-scored best-execution tick stream from multiple brokers
- **Bar builder** — construct OHLCV bars for all 21 MT5 timeframes from canonical ticks
- **Dataset export** — model-ready parquet files with features, labels, and train/val/test splits
- **Durable storage** — Hive-partitioned Parquet with zstd compression, SQLite checkpoints

## Architecture

```
MT5 Terminal A ──┐                              ┌── Built Bars (M1…MN1)
                 ├── Raw Ticks ── Canonical ────┤
MT5 Terminal B ──┘    (per broker)   Merge      └── Dataset (features + labels)

Snapshots: symbol metadata, account state, terminal state,
           active orders/positions, market book, history orders/deals
```

### Directory Layout

```
data/
├── raw/
│   ├── broker_a/
│   │   ├── ticks/symbol=XAUUSD/date=2024-01-15/part-0000.parquet
│   │   ├── native_bars/symbol=XAUUSD/timeframe=M1/date=.../
│   │   ├── symbol_metadata/date=.../
│   │   ├── account_state/date=.../
│   │   └── ...
│   └── broker_b/
│       └── ...
├── canonical/
│   └── ticks/symbol=XAUUSD/date=2024-01-15/part-0000.parquet
├── bars/
│   └── symbol=XAUUSD/timeframe=M1/date=.../part-0000.parquet
├── datasets/
│   └── XAUUSD/
│       ├── train.parquet
│       ├── val.parquet
│       └── test.parquet
└── checkpoints.db
```

## Requirements

- **Python 3.11+**
- **MetaTrader 5** terminal(s) installed on Windows
- MT5 Python package (Windows only)

## Installation

```bash
# Clone and install in editable mode
cd Datapipe
pip install -e ".[dev]"
```

### Dependencies

| Package | Purpose |
|---------|---------|
| MetaTrader5 | Terminal connectivity |
| polars | Data transforms |
| pyarrow | Parquet I/O |
| pydantic | Config & data validation |
| typer[all] | CLI framework |
| structlog | Structured logging |
| tenacity | Retry logic |
| PyYAML | Config loading |
| rich | Terminal output |

## Configuration

### 1. Create secrets environment file

```bash
cp config/example_secrets.env .env
# Edit .env with your broker credentials
```

### 2. Edit pipeline config

Copy and customise `config/pipeline.yaml`:

```yaml
brokers:
  broker_a:
    terminal_path: "C:/Program Files/MetaTrader 5 BrokerA/terminal64.exe"
    login: 12345678
    password: "${MT5_BROKER_A_PASSWORD}"   # resolved from env
    server: "BrokerA-Live"
    priority: 1
    symbol_map:
      XAUUSD: "XAUUSD"

  broker_b:
    terminal_path: "C:/Program Files/MetaTrader 5 BrokerB/terminal64.exe"
    login: 87654321
    password: "${MT5_BROKER_B_PASSWORD}"
    server: "BrokerB-Live"
    priority: 2
    symbol_map:
      XAUUSD: "XAUUSDm"   # broker-specific suffix
```

See [config/pipeline.yaml](config/pipeline.yaml) for the complete example with all sections.

## Usage

All commands are available via the `mt5pipe` CLI:

```bash
mt5pipe --help
```

### One-Command Advanced Orchestrator (Windows TUI)

This project includes a full-screen orchestrator that:

- Installs dependencies automatically
- Detects both configured MT5 terminals from Windows process data
- Runs the end-to-end pipeline with progress bars, ETA, and live logs

For non-technical users, run a single file:

```bash
RUN_ME.bat
```

It will automatically:

- create `.venv` if missing
- install/update dependencies
- launch the interactive wizard TUI

Run it with:

```bash
python -m mt5pipe.tools.super_pipeline_tui --from 2024-01-01 --to 2024-06-01
```

Run in wizard mode (no required arguments):

```bash
python -m mt5pipe.tools.super_pipeline_tui --interactive
```

Optional flags:

```bash
python -m mt5pipe.tools.super_pipeline_tui \
    --from 2024-01-01 --to 2024-06-01 \
    --config config/pipeline.yaml \
    --symbol XAUUSD \
    --dataset-name xau_v1 \
    --live-after --live-broker broker_a --enable-book
```

If you prefer a direct command after reinstalling editable deps:

```bash
mt5pipe-super --from 2024-01-01 --to 2024-06-01
```

### Historical Backfill

```bash
# Backfill ticks for a date range
mt5pipe backfill ticks --broker broker_a --symbol XAUUSD \
    --start 2024-01-01 --end 2024-06-01 --config config/pipeline.yaml

# Backfill native bars
mt5pipe backfill bars --broker broker_a --symbol XAUUSD \
    --start 2024-01-01 --end 2024-06-01 --config config/pipeline.yaml

# Backfill historical orders and deals
mt5pipe backfill history-orders --broker broker_a \
    --start 2024-01-01 --end 2024-06-01 --config config/pipeline.yaml

mt5pipe backfill history-deals --broker broker_a \
    --start 2024-01-01 --end 2024-06-01 --config config/pipeline.yaml

# Capture symbol metadata snapshot
mt5pipe backfill symbol-metadata --broker broker_a \
    --config config/pipeline.yaml
```

### Live Collection

```bash
# Start continuous tick collection with periodic snapshots
mt5pipe live collect --broker broker_a --symbol XAUUSD \
    --config config/pipeline.yaml

# Press Ctrl+C for graceful shutdown (flushes buffers, writes checkpoint)
```

### Canonical Merge

```bash
# Merge ticks from all brokers into canonical stream
mt5pipe merge canonical --symbol XAUUSD \
    --start 2024-01-01 --end 2024-06-01 --config config/pipeline.yaml
```

### Build Bars

```bash
# Build bars for a specific timeframe
mt5pipe bars build --symbol XAUUSD --timeframe M5 \
    --start 2024-01-01 --end 2024-06-01 --config config/pipeline.yaml

# Build all 21 timeframes at once
mt5pipe bars build --symbol XAUUSD --timeframe all \
    --start 2024-01-01 --end 2024-06-01 --config config/pipeline.yaml
```

### Build Dataset

```bash
# Build model-ready dataset with features, labels, and splits
mt5pipe dataset build --symbol XAUUSD \
    --start 2024-01-01 --end 2024-06-01 --config config/pipeline.yaml
```

### Status & Validation

```bash
# Show ingestion checkpoint summary
mt5pipe status show --config config/pipeline.yaml

# Validate parquet file integrity
mt5pipe status validate --config config/pipeline.yaml
```

## Data Categories Collected

| Category | Source | Partitioning |
|----------|--------|-------------|
| Raw ticks | `copy_ticks_range` | broker / symbol / date |
| Native bars | `copy_rates_range` | broker / symbol / timeframe / date |
| Symbol metadata | `symbol_info` | broker / date |
| Symbol universe | All visible symbols | broker / date |
| Market book | `market_book_get` | broker / symbol / date |
| Account state | `account_info` | broker / date |
| Terminal state | `terminal_info` | broker / date |
| Active orders | `orders_get` | broker / date |
| Active positions | `positions_get` | broker / date |
| Historical orders | `history_orders_get` | broker / date |
| Historical deals | `history_deals_get` | broker / date |
| Canonical ticks | Merged from brokers | symbol / date |
| Built bars | From canonical ticks | symbol / timeframe / date |
| Datasets | From built bars | symbol / split |

## Dataset Features

The exported dataset includes:

- **Time features**: cyclical hour-of-day, day-of-week (sin/cos encoded)
- **Session features**: Asia, London, New York, overlap indicators
- **Spread quality**: spread_mean, spread_max, spread_volatility per bar
- **HTF context**: lagged H1 and H4 bar features via asof join (no leakage)
- **Labels**: future returns at multiple horizons, direction labels, triple-barrier labels with MAE/MFE

### Train/Val/Test Split

Splits are strictly temporal (no shuffling) with configurable ratios (default 70/15/15). Walk-forward cross-validation is also supported.

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=mt5pipe --cov-report=term-missing

# Run specific test module
pytest tests/test_bar_builder.py -v
```

## Design Principles

- **UTC everywhere** — all timestamps stored as UTC; `time_msc` (millisecond epoch) is the primary tick key
- **Append-only raw data** — raw ingested data is never modified
- **Broker separation** — each broker's data in its own directory tree
- **Checkpoint resume** — every backfill operation can resume from where it stopped
- **No look-ahead bias** — HTF features use asof join backward; temporal splits enforce ordering
- **Process-global MT5** — the MT5 Python API only supports one terminal per process; the pipeline handles this by operating on one broker at a time

## License

MIT
#   D a t a P i p e l i n e  
 #   D a t a P i p e l i n e  
 