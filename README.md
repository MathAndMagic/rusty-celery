<div align="center">
    <br>
    <img src="img/rusty-celery-logo-transparent.png"/>
    <br>
    <br>
    <p>
    A Rust implementation of <a href="https://github.com/celery/celery">Celery</a> for producing and consuming asynchronous tasks with a distributed message queue.
    </p>
    <hr/>
</div>
<p align="center">
    <a href="https://github.com/rusty-celery/rusty-celery/actions">
        <img alt="Build" src="https://github.com/rusty-celery/rusty-celery/workflows/CI/badge.svg?event=push&branch=main">
    </a>
    <a href="https://github.com/rusty-celery/rusty-celery/blob/main/LICENSE">
        <img alt="License" src="https://img.shields.io/github/license/rusty-celery/rusty-celery.svg?color=blue&cachedrop">
    </a>
    <a href="https://crates.io/crates/celery">
        <img alt="Crates" src="https://img.shields.io/crates/v/celery.svg?color=blue">
    </a>
    <a href="https://docs.rs/celery/">
        <img alt="Docs" src="https://img.shields.io/badge/docs.rs-API%20docs-blue">
    </a>
    <a href="https://github.com/rusty-celery/rusty-celery/issues?q=is%3Aissue+is%3Aopen+label%3A%22Status%3A+Help+Wanted%22">
        <img alt="Help wanted" src="https://img.shields.io/github/issues/rusty-celery/rusty-celery/Status%3A%20Help%20Wanted?label=Help%20Wanted">
    </a>
</p>
<br/>


We welcome contributions from everyone regardless of your experience level with Rust. For complete beginners, see [HACKING_QUICKSTART.md](https://github.com/rusty-celery/rusty-celery/blob/main/HACKING_QUICKSTART.md).

If you already know the basics of Rust but are new to Celery, check out the [Rusty Celery Book](https://rusty-celery.github.io/) or the original Python [Celery Project](http://www.celeryproject.org/).

## Quick start

Define tasks by decorating functions with the [`task`](https://docs.rs/celery/*/celery/attr.task.html) attribute.

```rust
use celery::prelude::*;

#[celery::task]
fn add(x: i32, y: i32) -> TaskResult<i32> {
    Ok(x + y)
}
```

Create an app with the [`app`](https://docs.rs/celery/*/celery/macro.celery_app.html) macro
and register your tasks with it:

```rust
let my_app = celery::app!(
    broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
    tasks = [add],
    task_routes = [
        "*" => "celery",
    ],
).await?;
```

Then send tasks to a queue with

```rust
my_app.send_task(add::new(1, 2)).await?;
```

And consume tasks as a worker from a queue with

```rust
my_app.consume().await?;
```

## Examples

The [`examples/`](https://github.com/rusty-celery/rusty-celery/tree/main/examples) directory contains:

- a simple Celery app implemented in Rust using an AMQP broker ([`examples/celery_app.rs`](https://github.com/rusty-celery/rusty-celery/blob/main/examples/celery_app.rs)),
- the same Celery app implemented in Python ([`examples/celery_app.py`](https://github.com/rusty-celery/rusty-celery/blob/main/examples/celery_app.py)),
- and a Beat app implemented in Rust ([`examples/beat_app.rs`](https://github.com/rusty-celery/rusty-celery/blob/main/examples/beat_app.rs)).

### Prerequisites

If you already have an AMQP broker running you can set the environment variable `AMQP_ADDR` to your broker's URL (e.g., `amqp://localhost:5672//`, where
the second slash at the end is the name of the [default vhost](https://www.rabbitmq.com/access-control.html#default-state)).
Otherwise simply run the helper script:

```bash
./scripts/brokers/amqp.sh
```

This will download and run the official [RabbitMQ](https://www.rabbitmq.com/) image (RabbitMQ is a popular AMQP broker).

### Run the examples

![](./img/demo.gif)

#### Run Rust Celery app

You can consume tasks with:

```bash
cargo run --example celery_app consume
```

And you can produce tasks with:

```bash
cargo run --example celery_app produce [task_name]
```

Current supported tasks for this example are: `add`, `buggy_task`, `long_running_task` and `bound_task`

#### Run Python Celery app

Similarly, you can consume or produce tasks from Python by running


```bash
python examples/celery_app.py consume [task_name]
```

or

```bash
python examples/celery_app.py produce
```

You'll need to have Python 3 installed, along with the requirements listed in the `requirements.txt` file.  You'll also have to provide a task name. This example implements 4 tasks: `add`, `buggy_task`, `long_running_task` and `bound_task`

#### Run Rust Beat app

You can start the Rust beat with:

```bash
cargo run --example beat_app
```

And then you can consume tasks from Rust or Python as explained above.

## Road map and current state

✅ = Supported and mostly stable, although there may be a few incomplete features.<br/>
⚠️ = Partially implemented and under active development.<br/>
🔴 = Not supported yet but on-deck to be implemented soon.

### Core

|                  | Status  | Tracking  |
| ---------------- |:-------:| --------- |
| Protocol         | ⚠️      | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Protocol%20Feature?label=Issues)](https://github.com/rusty-celery/rusty-celery/issues?q=is%3Aissue+label%3A%22Protocol+Feature%22+is%3Aopen) |
| Producers        | ✅      | |
| Consumers        | ✅      | |
| Brokers          | ✅      | |
| Beat             | ✅      | |
| Backends         | 🔴      | |
| [Baskets](https://github.com/rusty-celery/rusty-celery/issues/53) | 🔴      | |

### Brokers

|       | Status | Tracking |
| ----- |:------:| -------- |
| AMQP  | ✅     | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Broker%3A%20AMQP?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Broker%3A%20AMQP) |
| Redis | ✅     | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Broker%3A%20Redis?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Broker%3A%20Redis) |

### Backends

|             | Status | Tracking |
| ----------- |:------:| -------- |
| RPC         | 🔴     | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Backend%3A%20RPC?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Backend%3A%20RPC) |
| Redis       | 🔴     | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Backend%3A%20Redis?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Backend%3A%20Redis) |

### Redis recovery contract

- `RedisBrokerBuilder::with_runtime` shares `RedisBrokerRuntime` across endpoint replacements, so existing consumers and outstanding deliveries retain the same capacity ownership.
- A Redis connection acquisition or command has a five-second deadline; connection installation and explicit reconnect honor the supplied timeout, including lock acquisition.
- Explicit reconnect requires the actual connection to report the primary role within the supplied reconnect deadline.
- Replay-safe claim, send, acknowledgement, and retry commands make at most two attempts; their total transport budget is at most twenty seconds.
- `Broker::health_snapshot` reads local consumer progress synchronously; each active consumer must poll within thirty seconds, unless owned deliveries fill prefetch capacity.
- Missing queues remain unhealthy even while other queues fill capacity; each active consumer receives only one resume grace after capacity becomes available, until it polls again.
- Idle queues poll every second, and readiness probes cannot refresh consumer progress.
- `Broker::health_check` checks the role of the actual broker connection and rejects replicas; producer-only brokers do not require a consumer heartbeat.
- Queue messages retain the existing Celery JSON format and Redis list names; outstanding messages retain `_celery.<queue>_process_map`, keyed by correlation ID.
- Claim atomically records the payload in the process map before removing it from the queue; a stable claim ID reconciles a lost reply within the bounded retry budget.
- A cancelled claim or process crash can leave a durable process-map record; this transport does not reclaim those records or promise automatic restart redelivery.
- Acknowledgement compares the original payload before deletion, so an old attempt cannot delete a different retried payload with the same task ID.
- Task retry atomically enqueues the next attempt and removes the matching pending record; it also supports tasks acknowledged before execution.
- Temporary claim/send/retry result keys expire after five minutes; they deduplicate transport retries, not separate calls to send a business task.
- The existing process map requires unique concurrent task IDs; concurrently delivering identical payloads with the same correlation ID does not create independent pending records.
- Delivery drop releases only local prefetch capacity; it does not delete durable pending records.
- Redis connection drivers are owned by the runtime and aborted on timeout, cancellation, or endpoint replacement.
- `Celery::consume_from` owns delivery handlers across consumer reconnects and waits through business execution, retry publication, and acknowledgement during warm shutdown.
- Acquired deliveries waiting for a future ETA also remain owned during warm shutdown; callers that require a shutdown deadline must bound the consumer future.
- SIGTERM or SIGINT interrupts reconnect backoff before draining active delivery handlers.
