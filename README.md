Program to run a sample feeds of transactions

# Transparency

I use AI at work using codex. I have no such feature on my personal dev environment, I am wary of using other AI models unless I have used them in production.

I did utilize Google AI:
- researching, and debating ideas, and branches of thought
- skeleton code generation (copy + paste)
- fast iterations (although quite panful without something like a vs code codex integration)
- generating tests. I tried to eye them over manually, but I was coming short on time.

# Possible optimizations that weren't done

- mmap for faster reads OR io_uring
- if there was a guaranteed watermark, or late delivery cutoff, we can save on resources (memory + disk). Just like how windowing/trigger policies//watermarks keeps resource usage low for one node of a streaming procecessing system.

# Assumptions

- the true determination of order, is the order the data comes in the file, not the transaction id.
- chargeback can only be done on a client/tx pair:
 - currently in dispute
- dispute can only be done on a client/tx pair:
 - not in any dispute/resolve/chargeback
- resolve can only be done on a client/tx pair:
 - currently in dispute
- the client id space is u16, transactions is u32, or 
 - 65,535 clients
 - 4,294,967,295 transactions
 - just holding max transactions in memory for just float 32 (amount/money):
    - 17.1798692 gigabytes
- The only type of transaction that can be disputed, is a deposit, nothing else.

# Design

I utilized serde/async_csv for reading into structs. I tried to utilize the Rust typesystem as much as possible, to enforce error detectoin early on.

Since the memory usage was high, I wanted to use a memory/disk-based databsae. My initial thoughts were SQLite. Upon further research, SQL is not so good for high write-throughput (this is a write-heavy program). Primary keys on SQLite are b-trees (and I think other indexes too, unlike PostgreSQL). 

RocksDB is LSM, which prioritizes high write throughput, with some tuning required, otherwise the background flush/compaction process can forever be competing with the speed of incomign writes.

I decided to use RocksDB with a custom model for data:

The database is partitioned into two specific Column Families (CF) to separate high-frequency balance updates from large-scale transaction history.

## Balances CF
Stores the current financial standing for all 65,535 possible clients.
- **Key**: `u16` (2 bytes) — Client ID.
- **Value**: `[u8; 9]` (9 bytes) — Binary bitpacked state.


| Offset | Field | Type | Description |
|:---|:---|:---|:---|
| `0` | `frozen` | `u8` | `0x01` if account is locked, else `0x00` |
| `1-4` | `available` | `f32` | Liquid funds available for withdrawal |
| `5-8` | `held` | `f32` | Funds currently locked in active disputes |

## Ledger CF
A historical record of transactions required for dispute validation.
- **Key**: `[u8; 6]` — (2 bytes Client ID) + (4 bytes Transaction ID).
- **Value**: `[u8; 5]` (5 bytes) — Binary bitpacked metadata.


| Offset | Field | Type | Description |
|:---|:---|:---|:---|
| `0-3` | `amount` | `f32` | Original transaction amount |
| `4` | `status` | `u8` | `0`: Normal, `1`: Disputed, `3`: ChargedBack |



# Benchmark

- Example run on a 100mb file (randomly generated)
```bash
cargo run --release -- large_input.csv > output.csv 2> /dev/null  14.22s user 5.93s system 99% cpu 20.329 total
```
