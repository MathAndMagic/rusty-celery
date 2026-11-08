use super::*;
use crate::protocol::{MessageHeaders, MessageProperties};
use futures::StreamExt;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

fn message(id: &str) -> Message {
    Message {
        properties: MessageProperties {
            correlation_id: id.into(),
            content_type: "application/json".into(),
            content_encoding: "utf-8".into(),
            reply_to: None,
            delivery_info: None,
        },
        headers: MessageHeaders {
            id: id.into(),
            task: "recovery_test".into(),
            ..Default::default()
        },
        raw_body: b"[[],{},{}]".to_vec(),
    }
}
fn url() -> String {
    std::env::var("REDIS_ADDR").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into())
}
async fn setup() -> (Arc<RedisBrokerRuntime>, Box<dyn Broker>, String) {
    let runtime = Arc::new(RedisBrokerRuntime::new(1));
    let broker = RedisBrokerBuilder::new(&url())
        .with_runtime(runtime.clone())
        .build(2)
        .await
        .unwrap();
    (runtime, broker, format!("recovery-{}", Uuid::new_v4()))
}
async fn next(stream: &mut Box<dyn DeliveryStream>) -> Box<dyn super::super::Delivery> {
    tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .unwrap()
        .unwrap()
        .ok()
        .unwrap()
}
async fn count(runtime: &RedisBrokerRuntime, command: &str, key: &str) -> i64 {
    runtime.command(redis::cmd(command).arg(key)).await.unwrap()
}

#[tokio::test]
async fn generation_replacement_keeps_delivery_capacity_and_stream_transport() {
    let (runtime, old, queue) = setup().await;
    let (_, mut old_stream) = old
        .consume(&queue, Box::new(|e| panic!("{}", e)))
        .await
        .unwrap();
    old.send(&message("first"), &queue).await.unwrap();
    let first = next(&mut old_stream).await;
    let replacement = RedisBrokerBuilder::new(&url())
        .with_runtime(runtime.clone())
        .build(2)
        .await
        .unwrap();
    let (_, mut new_stream) = replacement
        .consume(&queue, Box::new(|e| panic!("{}", e)))
        .await
        .unwrap();
    replacement.send(&message("second"), &queue).await.unwrap();
    assert_eq!(replacement.health_snapshot().in_flight_deliveries, 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), new_stream.next())
            .await
            .is_err()
    );
    replacement.ack(first.as_ref()).await.unwrap();
    replacement.ack(first.as_ref()).await.unwrap();
    assert_eq!(replacement.health_snapshot().in_flight_deliveries, 0);
    // This stream predates replacement and obtains the new transport on its next poll.
    let second = next(&mut old_stream).await;
    assert_eq!(
        second.try_deserialize_message().unwrap().headers.id,
        "second"
    );
    drop(second);
    assert_eq!(runtime.health_snapshot().in_flight_deliveries, 0);
    assert_eq!(
        count(&runtime, "HLEN", &format!("_celery.{queue}_process_map")).await,
        1
    );
}

#[tokio::test]
async fn retry_is_atomic_idempotent_and_old_ack_preserves_next_attempt() {
    let (runtime, broker, queue) = setup().await;
    let (_, mut stream) = broker
        .consume(&queue, Box::new(|e| panic!("{}", e)))
        .await
        .unwrap();
    broker.send(&message("retry"), &queue).await.unwrap();
    let first = next(&mut stream).await;
    // Early acknowledgement must still permit a task-requested retry.
    broker.ack(first.as_ref()).await.unwrap();
    let eta = Some(Utc::now());
    broker.retry(first.as_ref(), eta).await.unwrap();
    broker.retry(first.as_ref(), eta).await.unwrap();
    assert_eq!(count(&runtime, "LLEN", &queue).await, 1);
    let second = next(&mut stream).await;
    let retried = second.try_deserialize_message().unwrap();
    assert_eq!(retried.headers.retries, Some(1));
    assert_eq!(retried.headers.eta, eta);
    broker.ack(first.as_ref()).await.unwrap();
    assert_eq!(
        count(&runtime, "HLEN", &format!("_celery.{queue}_process_map")).await,
        1
    );
    assert_eq!(runtime.health_snapshot().in_flight_deliveries, 1);
    broker.ack(second.as_ref()).await.unwrap();
    assert_eq!(runtime.health_snapshot().in_flight_deliveries, 0);
}

#[tokio::test]
async fn transport_dedup_does_not_suppress_separate_business_sends() {
    let (runtime, broker, queue) = setup().await;
    let task = message("same-id");
    broker.send(&task, &queue).await.unwrap();
    broker.send(&task, &queue).await.unwrap();
    assert_eq!(count(&runtime, "LLEN", &queue).await, 2);
}

#[tokio::test]
async fn health_uses_oldest_consumer_and_owned_capacity_not_readiness() {
    let (runtime, broker, queue) = setup().await;
    assert!(broker.health_snapshot().is_live); // Producer-only broker.
    let (tag, mut first) = broker
        .consume(&queue, Box::new(|e| panic!("{}", e)))
        .await
        .unwrap();
    let (_, _second) = broker
        .consume("other-queue", Box::new(|e| panic!("{}", e)))
        .await
        .unwrap();
    let stale = Instant::now() - CONSUMER_PROGRESS_TIMEOUT - Duration::from_secs(1);
    *runtime.consumers.lock().unwrap()[&tag]
        .progress
        .lock()
        .unwrap() = stale;
    assert!(!broker.health_check().await.unwrap().is_live);
    broker.send(&message("long-running"), &queue).await.unwrap();
    let delivery = next(&mut first).await;
    for state in runtime.consumers.lock().unwrap().values() {
        *state.progress.lock().unwrap() = stale;
    }
    assert!(broker.health_snapshot().is_live); // Real owned work fills prefetch; no task-duration limit.
    broker.ack(delivery.as_ref()).await.unwrap();
    assert!(broker.health_snapshot().is_live); // Capacity release starts a fresh polling deadline.
    for state in runtime.consumers.lock().unwrap().values() {
        *state.resume_grace.lock().unwrap() = Some(stale);
    }
    assert!(!broker.health_snapshot().is_live);
    // A real idle poll refreshes its own progress even when no tasks arrive.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), first.next())
            .await
            .is_err()
    );
    assert!(
        runtime.consumers.lock().unwrap()[&tag]
            .progress
            .lock()
            .unwrap()
            .elapsed()
            < Duration::from_secs(1)
    );
}

// A local TCP fault injector forwards Redis requests and discards exactly one EVAL reply
// after Redis has executed it. Holding that reply also exercises caller cancellation.
struct Proxy {
    url: String,
    armed: Arc<AtomicBool>,
    executed: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn proxy(hold: bool) -> Proxy {
    let info = Client::open(url()).unwrap().get_connection_info().clone();
    let target = match info.addr {
        redis::ConnectionAddr::Tcp(host, port) => format!("{host}:{port}"),
        _ => panic!("TCP Redis required for fault tests"),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let executed = Arc::new(Notify::new());
    let (a, e) = (armed.clone(), executed.clone());
    let task = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let (client, _) = listener.accept().await.unwrap();
            let server = TcpStream::connect(&target).await.unwrap();
            let (armed, executed) = (a.clone(), e.clone());
            connections.spawn(async move {
                let (mut cr, mut cw) = client.into_split();
                let (mut sr, mut sw) = server.into_split();
                let discard = AtomicBool::new(false);
                let requests = async {
                    let mut buf = [0; 16384];
                    loop {
                        let n = cr.read(&mut buf).await?;
                        if n == 0 {
                            return Ok::<_, io::Error>(());
                        }
                        if buf[..n].windows(4).any(|part| part == b"EVAL")
                            && armed.swap(false, Ordering::SeqCst)
                        {
                            discard.store(true, Ordering::SeqCst);
                        }
                        sw.write_all(&buf[..n]).await?;
                    }
                };
                let replies = async {
                    let mut buf = [0; 16384];
                    loop {
                        let n = sr.read(&mut buf).await?;
                        if n == 0 {
                            return Ok::<_, io::Error>(());
                        }
                        if discard.swap(false, Ordering::SeqCst) {
                            executed.notify_one();
                            if hold {
                                futures::future::pending::<()>().await;
                            }
                            return Ok(());
                        }
                        cw.write_all(&buf[..n]).await?;
                    }
                };
                tokio::select! { _ = requests => {}, _ = replies => {} }
            });
        }
    });
    Proxy {
        url: format!("redis://{addr}/{}", info.redis.db),
        armed,
        executed,
        task,
    }
}

#[tokio::test]
async fn lost_claim_ack_and_retry_replies_reconcile_without_duplicate_work() {
    let proxy = proxy(false).await;
    let runtime = Arc::new(RedisBrokerRuntime::new(1));
    let broker = RedisBrokerBuilder::new(&proxy.url)
        .with_runtime(runtime.clone())
        .build(2)
        .await
        .unwrap();
    let queue = format!("lost-reply-{}", Uuid::new_v4());
    let (_, mut stream) = broker
        .consume(&queue, Box::new(|e| panic!("{}", e)))
        .await
        .unwrap();
    proxy.armed.store(true, Ordering::SeqCst);
    broker.send(&message("first"), &queue).await.unwrap();
    assert_eq!(count(&runtime, "LLEN", &queue).await, 1);
    broker.send(&message("second"), &queue).await.unwrap();
    proxy.armed.store(true, Ordering::SeqCst);
    let first = next(&mut stream).await;
    assert_eq!(first.try_deserialize_message().unwrap().headers.id, "first");
    assert_eq!(count(&runtime, "LLEN", &queue).await, 1);
    proxy.armed.store(true, Ordering::SeqCst);
    broker.retry(first.as_ref(), None).await.unwrap();
    assert_eq!(count(&runtime, "LLEN", &queue).await, 2);
    let second = next(&mut stream).await;
    proxy.armed.store(true, Ordering::SeqCst);
    broker.ack(second.as_ref()).await.unwrap();
    assert_eq!(runtime.health_snapshot().in_flight_deliveries, 0);
    let retry = next(&mut stream).await;
    assert_eq!(
        retry.try_deserialize_message().unwrap().headers.retries,
        Some(1)
    );
    broker.ack(retry.as_ref()).await.unwrap();
    assert_eq!(count(&runtime, "LLEN", &queue).await, 0);
}

#[tokio::test]
async fn cancelled_ambiguous_claim_retains_durable_pending_record() {
    let proxy = proxy(true).await;
    let (_, direct, queue) = setup().await;
    direct.send(&message("orphan"), &queue).await.unwrap();
    let runtime = Arc::new(RedisBrokerRuntime::new(1));
    let broker = RedisBrokerBuilder::new(&proxy.url)
        .with_runtime(runtime.clone())
        .build(2)
        .await
        .unwrap();
    let (_, mut stream) = broker
        .consume(&queue, Box::new(|e| panic!("{}", e)))
        .await
        .unwrap();
    proxy.armed.store(true, Ordering::SeqCst);
    tokio::select! { _ = stream.next() => panic!("reply should be held"), _ = proxy.executed.notified() => {} }
    drop(stream); // Cancels claim future before reply is observed.
                  // The cancelled request closes its owned driver; a fresh transport can answer immediately.
    assert!(
        tokio::time::timeout(Duration::from_secs(1), broker.health_check())
            .await
            .unwrap()
            .is_ok()
    );
    assert_eq!(runtime.health_snapshot().in_flight_deliveries, 0);
    assert_eq!(runtime.occupied.load(Ordering::SeqCst), 0);
    let direct_runtime = Arc::new(RedisBrokerRuntime::new(1));
    let _fresh = RedisBrokerBuilder::new(&url())
        .with_runtime(direct_runtime.clone())
        .build(2)
        .await
        .unwrap();
    assert_eq!(count(&direct_runtime, "LLEN", &queue).await, 0);
    assert_eq!(
        count(
            &direct_runtime,
            "HLEN",
            &format!("_celery.{queue}_process_map")
        )
        .await,
        1
    );
    // Reconnection never silently deletes pending records and does not promise restart redelivery.
}

#[tokio::test]
async fn connection_waiter_uses_endpoint_selected_after_serialized_replacement() {
    let (runtime, _, _) = setup().await;
    runtime.transport.write().unwrap().as_mut().unwrap().manager = None;
    let lock = runtime.connect_lock.lock().await;
    let connection = runtime.connection();
    tokio::pin!(connection);
    tokio::select! { _ = &mut connection => panic!("lock must block connection"), _ = tokio::time::sleep(Duration::from_millis(10)) => {} }
    let mut info = Client::open(url()).unwrap().get_connection_info().clone();
    info.redis.db = 14;
    runtime.transport.write().unwrap().as_mut().unwrap().client = Client::open(info).unwrap();
    drop(lock);
    let (mut connection, _) = connection.await.unwrap();
    let info: String = redis::cmd("CLIENT")
        .arg("INFO")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(info.split_whitespace().any(|field| field == "db=14"));
}

#[tokio::test]
async fn connection_handshake_has_finite_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("redis://{}/1", listener.local_addr().unwrap());
    let held = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.unwrap();
        futures::future::pending::<()>().await;
    });
    let start = Instant::now();
    let result = RedisBrokerBuilder::new(&url).build(1).await;
    assert!(result.is_err());
    assert!(start.elapsed() < Duration::from_secs(2));
    held.abort();
}

#[tokio::test]
async fn missing_queue_is_visible_and_only_owned_recovery_refreshes_it() {
    let (runtime, broker, queue) = setup().await;
    let (_, missing) = broker.consume(&queue, Box::new(|_| {})).await.unwrap();
    let (_, _healthy) = broker
        .consume("still-active", Box::new(|_| {}))
        .await
        .unwrap();
    drop(missing);
    *runtime.stopped.lock().unwrap().get_mut(&queue).unwrap() =
        Instant::now() - CONSUMER_PROGRESS_TIMEOUT - Duration::from_secs(1);
    assert!(!broker.health_check().await.unwrap().is_live);
    runtime.record_consumer_reconnect();
    assert!(broker.health_snapshot().is_live);
    let (_, _replacement) = broker.consume(&queue, Box::new(|_| {})).await.unwrap();
    assert!(runtime.stopped.lock().unwrap().is_empty());
}

#[tokio::test]
async fn unlimited_prefetch_remains_unlimited_for_delayed_tasks() {
    let runtime = Arc::new(RedisBrokerRuntime::new(0));
    let broker = RedisBrokerBuilder::new(&url())
        .with_runtime(runtime)
        .build(2)
        .await
        .unwrap();
    broker.increase_prefetch_count().await.unwrap();
    broker.decrease_prefetch_count().await.unwrap();
    assert_eq!(broker.health_snapshot().prefetch_limit, 0);
}

#[tokio::test]
async fn command_timeout_recovers_transport_without_poisoning_local_liveness() {
    let (runtime, broker, queue) = setup().await;
    let start = Instant::now();
    let result: Result<Option<(String, String)>, _> = runtime
        .command(redis::cmd("BLPOP").arg(&queue).arg(0))
        .await;
    assert!(result.unwrap_err().is_connection_error());
    assert!(start.elapsed() < Duration::from_secs(7));
    assert!(broker.health_snapshot().is_live);
    assert!(broker.health_check().await.unwrap().is_live);
}

#[tokio::test]
async fn readiness_checks_actual_connection_role_without_killing_producer_liveness() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("redis://{}/0", listener.local_addr().unwrap());
    let replica = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 512];
        let n = socket.read(&mut request).await.unwrap();
        assert!(request[..n].windows(4).any(|part| part == b"ROLE"));
        socket.write_all(b"*1\r\n$5\r\nslave\r\n").await.unwrap();
    });
    let broker = RedisBrokerBuilder::new(&url).build(1).await.unwrap();
    assert!(broker.health_check().await.is_err());
    assert!(broker.health_snapshot().is_live);
    replica.await.unwrap();
}

#[tokio::test]
async fn malformed_claim_and_wrong_pending_type_leave_original_queue_message() {
    let (runtime, _, queue) = setup().await;
    let state = Arc::new(ConsumerState {
        cancelled: AtomicBool::new(false),
        progress: Mutex::new(Instant::now()),
        resume_grace: Mutex::new(None),
    });
    let channel = Channel {
        runtime: runtime.clone(),
        queue: queue.clone(),
    };
    runtime
        .command::<i64>(redis::cmd("LPUSH").arg(&queue).arg("123"))
        .await
        .unwrap();
    assert!(channel.clone().fetch(state.clone()).await.is_err());
    assert_eq!(count(&runtime, "LLEN", &queue).await, 1);
    runtime
        .command::<String>(
            redis::cmd("LSET").arg(&queue).arg(0).arg(
                message("blocked")
                    .json_serialized(Some(DeliveryInfo::for_redis_default()))
                    .unwrap(),
            ),
        )
        .await
        .unwrap();
    runtime
        .command::<String>(
            redis::cmd("SET")
                .arg(channel.process_map())
                .arg("wrong-type"),
        )
        .await
        .unwrap();
    assert!(channel.fetch(state).await.is_err());
    assert_eq!(count(&runtime, "LLEN", &queue).await, 1);
    assert_eq!(runtime.occupied.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn producer_reconnection_never_creates_a_consumer_heartbeat_requirement() {
    let (runtime, broker, _) = setup().await;
    broker.reconnect(1).await.unwrap();
    runtime.record_consumer_reconnect();
    assert!(runtime.last_progress.lock().unwrap().is_none());
    let health = broker.health_check().await.unwrap();
    assert!(health.is_live);
    assert_eq!(health.active_consumers, 0);
    assert_eq!(health.last_consumer_progress_age, None);
}

#[tokio::test]
async fn timed_out_rpc_closes_the_owned_connection_driver() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("redis://{}/0", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 512];
        assert!(socket.read(&mut request).await.unwrap() > 0);
        // Deliberately keep the TCP connection open without replying to ROLE.
        // Deadline cancellation must close the socket, not leave a detached driver waiting.
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(7), socket.read(&mut request))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    let broker = RedisBrokerBuilder::new(&url).build(1).await.unwrap();
    assert!(broker
        .health_check()
        .await
        .unwrap_err()
        .is_connection_error());
    server.await.unwrap();
}

#[tokio::test]
async fn old_concurrent_cancellation_cannot_close_replacement_driver() {
    let (runtime, _, _) = setup().await;
    let (_, generation) = runtime.connection().await.unwrap();
    let first = CommandGuard {
        runtime: &runtime,
        generation,
        completed: false,
    };
    let delayed = CommandGuard {
        runtime: &runtime,
        generation,
        completed: false,
    };
    drop(first); // One failed request invalidates their shared original transport.
    let (mut replacement, _) = runtime.connection().await.unwrap();
    drop(delayed); // Another original request observes cancellation after replacement.
    let response: String = redis::cmd("PING")
        .query_async(&mut replacement)
        .await
        .unwrap();
    assert_eq!(response, "PONG");
}

#[tokio::test]
async fn other_queue_completions_cannot_extend_a_stale_consumers_resume_grace() {
    let (runtime, broker, queue) = setup().await;
    let (stale_tag, stale_stream) = broker
        .consume("stale-queue", Box::new(|_| {}))
        .await
        .unwrap();
    let (_, mut active) = broker
        .consume(&queue, Box::new(|e| panic!("{}", e)))
        .await
        .unwrap();
    let stale = Instant::now() - CONSUMER_PROGRESS_TIMEOUT - Duration::from_secs(1);
    let stale_state = runtime.consumers.lock().unwrap()[&stale_tag].clone();
    *stale_state.progress.lock().unwrap() = stale;
    *stale_state.resume_grace.lock().unwrap() = Some(stale);
    for index in 0..3 {
        broker
            .send(&message(&format!("other-{index}")), &queue)
            .await
            .unwrap();
        let delivery = next(&mut active).await;
        assert!(broker.health_snapshot().is_live); // Owned work currently fills all capacity.
        broker.ack(delivery.as_ref()).await.unwrap();
        assert!(!broker.health_snapshot().is_live); // Other work cannot renew this queue's grace.
    }
    drop(stale_stream);
    *runtime
        .stopped
        .lock()
        .unwrap()
        .get_mut("stale-queue")
        .unwrap() = stale;
    broker.send(&message("full"), &queue).await.unwrap();
    let full = next(&mut active).await;
    assert!(!broker.health_snapshot().is_live); // Full capacity cannot hide a missing queue.
    broker.ack(full.as_ref()).await.unwrap();
}

#[tokio::test]
async fn direct_reconnect_rejects_a_replica_instead_of_resetting_retry_budget() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("redis://{}/0", listener.local_addr().unwrap());
    let replica = tokio::spawn(async move {
        let (mut initial, _) = listener.accept().await.unwrap();
        let mut request = [0; 512];
        assert_eq!(initial.read(&mut request).await.unwrap(), 0); // Reconnect replaced initial socket.
        let (mut replacement, _) = listener.accept().await.unwrap();
        let n = replacement.read(&mut request).await.unwrap();
        assert!(request[..n].windows(4).any(|part| part == b"ROLE"));
        replacement
            .write_all(b"*1\r\n$5\r\nslave\r\n")
            .await
            .unwrap();
    });
    let broker = RedisBrokerBuilder::new(&url).build(1).await.unwrap();
    assert!(broker.reconnect(1).await.unwrap_err().is_connection_error());
    replica.await.unwrap();
}
