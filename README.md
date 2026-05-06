Program to run a sample feeds of transactions

# Transparency

I use AI at work using codex. I have no such feature on my personal dev environment, I am wary of using other AI models unless I have used them in production.

I did utilize Google AI:
- researching, and debating ideas, and branches of thought
- skeleton code generation (copy + paste)
- fast iterations (although quite panful without something like a vs code codex integration)

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

# Benchmark

- Example run on a 100mb file (randomly generated)
```bash
cargo run --release -- large_input.csv > output.csv 2> /dev/null  14.22s user 5.93s system 99% cpu 20.329 total
```
