//! Redis transport with bounded I/O and shared consumer ownership across reconnections.
use super::{Broker, BrokerBuilder, BrokerHealth, DeliveryError, DeliveryStream};
use crate::{
    error::{BrokerError, ProtocolError},
    protocol::{Delivery, DeliveryInfo, Message, TryDeserializeMessage},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{Future, Stream};
use redis::{aio::MultiplexedConnection, Client, FromRedisValue};
use std::{
    collections::HashMap,
    fmt, io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::Notify;
use uuid::Uuid;

const COMMAND_TIMEOUT: Duration = Duration::from_secs(5);
const CONSUMER_PROGRESS_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_secs(1);
// Retry results outlive the bounded command/reconnect budget. These are not delivery leases.
const CLAIM_RESULT_RETENTION_SECONDS: u32 = 300;

const CLAIM: &str = r#"
local previous = redis.call('GET', KEYS[3])
if previous then return previous end
local payload = redis.call('LINDEX', KEYS[1], -1)
if not payload then return false end
local ok, decoded = pcall(cjson.decode, payload)
if not ok or type(decoded) ~= 'table' or type(decoded.properties) ~= 'table'
    or type(decoded.properties.correlation_id) ~= 'string' then
    return redis.error_reply('invalid Celery delivery; payload retained in queue')
end
redis.call('HSET', KEYS[2], decoded.properties.correlation_id, payload)
redis.call('RPOP', KEYS[1])
redis.call('SET', KEYS[3], payload, 'EX', ARGV[1])
return payload
"#;
const SEND: &str = r#"
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
redis.call('LPUSH', KEYS[1], ARGV[1])
redis.call('SET', KEYS[2], '1', 'EX', ARGV[2])
return 1
"#;
const ACK: &str = r#"
if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
    return redis.call('HDEL', KEYS[1], ARGV[1])
end
return 0
"#;
const RETRY: &str = r#"
if redis.call('EXISTS', KEYS[3]) == 1 then return 0 end
local current = redis.call('HGET', KEYS[2], ARGV[1])
if current and current ~= ARGV[2] then
    return redis.error_reply('delivery ownership changed before retry')
end
redis.call('LPUSH', KEYS[1], ARGV[3])
if current then redis.call('HDEL', KEYS[2], ARGV[1]) end
redis.call('SET', KEYS[3], '1', 'EX', ARGV[4])
return 1
"#;

fn io_error(message: impl Into<String>) -> BrokerError {
    BrokerError::IoError(io::Error::new(
        io::ErrorKind::ConnectionAborted,
        message.into(),
    ))
}
async fn bounded<T>(
    name: &str,
    duration: Duration,
    operation: impl Future<Output = Result<T, redis::RedisError>>,
) -> Result<T, BrokerError> {
    tokio::time::timeout(duration, operation)
        .await
        .map_err(|_| io_error(format!("Redis {name} exceeded {}s", duration.as_secs())))?
        .map_err(BrokerError::from)
}

struct ConsumerState {
    cancelled: AtomicBool,
    progress: Mutex<Instant>,
}
impl ConsumerState {
    fn progress(&self) {
        *self.progress.lock().unwrap() = Instant::now();
    }
}

struct ManagedConnection {
    connection: MultiplexedConnection,
    driver: tokio::task::JoinHandle<()>,
}
impl ManagedConnection {
    async fn connect(client: &Client, deadline: Duration) -> Result<Self, BrokerError> {
        let (connection, driver) = bounded(
            "connect",
            deadline,
            client.create_multiplexed_tokio_connection(),
        )
        .await?;
        Ok(Self {
            connection,
            driver: tokio::spawn(driver),
        })
    }
}
impl Drop for ManagedConnection {
    fn drop(&mut self) {
        // redis 0.22's detached default driver waits for outstanding replies on drop.
        // Explicit ownership closes a blackholed socket when its deadline expires.
        self.driver.abort();
    }
}
struct CommandGuard<'a> {
    runtime: &'a RedisBrokerRuntime,
    generation: u64,
    completed: bool,
}
impl Drop for CommandGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            let mut current = self.runtime.transport.write().unwrap();
            if let Some(current) = current.as_mut() {
                if current.generation == self.generation {
                    current.manager = None;
                }
            }
        }
    }
}
struct Transport {
    client: Client,
    manager: Option<ManagedConnection>,
    generation: u64,
}

/// Shared across broker replacement. Active streams and deliveries always obtain the latest
/// transport; their capacity and ownership never reset when Sentinel publishes a new primary.
pub struct RedisBrokerRuntime {
    transport: RwLock<Option<Transport>>,
    connect_lock: tokio::sync::Mutex<()>,
    generation: AtomicU64,
    closed: AtomicBool,
    prefetch: AtomicUsize,
    unlimited: bool,
    occupied: AtomicUsize,
    in_flight: AtomicUsize,
    consumers: Mutex<HashMap<String, Arc<ConsumerState>>>,
    stopped: Mutex<HashMap<String, Instant>>,
    last_progress: Mutex<Option<Instant>>,
    capacity_released: Mutex<Option<Instant>>,
    wake: Notify,
}
impl RedisBrokerRuntime {
    pub fn new(prefetch: u16) -> Self {
        Self {
            transport: RwLock::new(None),
            connect_lock: tokio::sync::Mutex::new(()),
            generation: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            prefetch: AtomicUsize::new(prefetch as usize),
            unlimited: prefetch == 0,
            occupied: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            consumers: Mutex::new(HashMap::new()),
            stopped: Mutex::new(HashMap::new()),
            last_progress: Mutex::new(None),
            capacity_released: Mutex::new(None),
            wake: Notify::new(),
        }
    }
    fn progress(&self) {
        *self.last_progress.lock().unwrap() = Some(Instant::now());
    }
    /// Records a bounded recovery attempt owned by the consumer control loop.
    /// Readiness checks, producers, and endpoint-notification listeners must not call this.
    pub fn record_consumer_reconnect(&self) {
        let consuming = {
            let mut progress = self.last_progress.lock().unwrap();
            if progress.is_some() {
                *progress = Some(Instant::now());
                true
            } else {
                false
            }
        };
        if consuming {
            for stopped in self.stopped.lock().unwrap().values_mut() {
                *stopped = Instant::now();
            }
        }
    }
    pub fn health_snapshot(&self) -> BrokerHealth {
        let consumers = self.consumers.lock().unwrap();
        let active = consumers.len();
        let in_flight = self.in_flight.load(Ordering::SeqCst);
        let limit = self.prefetch.load(Ordering::SeqCst);
        let age = consumers
            .values()
            .map(|state| state.progress.lock().unwrap().elapsed())
            .chain(self.stopped.lock().unwrap().values().map(Instant::elapsed))
            .max()
            .or_else(|| {
                self.last_progress
                    .lock()
                    .unwrap()
                    .map(|time| time.elapsed())
            });
        BrokerHealth {
            is_live: !matches!(age, Some(age) if age >= CONSUMER_PROGRESS_TIMEOUT)
                || (active > 0 && limit > 0 && in_flight >= limit)
                || matches!(*self.capacity_released.lock().unwrap(), Some(time) if time.elapsed() < CONSUMER_PROGRESS_TIMEOUT),
            active_consumers: active,
            in_flight_deliveries: in_flight,
            prefetch_limit: limit,
            last_consumer_progress_age: age,
        }
    }
    async fn install(&self, client: Option<Client>, deadline: Duration) -> Result<(), BrokerError> {
        tokio::time::timeout(deadline, async {
            let _lock = self.connect_lock.lock().await;
            let client = client
                .or_else(|| {
                    self.transport
                        .read()
                        .unwrap()
                        .as_ref()
                        .map(|current| current.client.clone())
                })
                .ok_or(BrokerError::NotConnected)?;
            // Publish the desired endpoint before connecting: an old failed request must never
            // install its stale endpoint over a newly discovered Sentinel primary.
            let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
            *self.transport.write().unwrap() = Some(Transport {
                client: client.clone(),
                manager: None,
                generation,
            });
            let manager = ManagedConnection::connect(&client, deadline).await?;
            self.transport.write().unwrap().as_mut().unwrap().manager = Some(manager);
            self.closed.store(false, Ordering::SeqCst);
            self.wake.notify_waiters();
            Ok(())
        })
        .await
        .map_err(|_| io_error("Redis connection replacement timed out"))?
    }
    async fn connection(&self) -> Result<(MultiplexedConnection, u64), BrokerError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(BrokerError::NotConnected);
        }
        tokio::time::timeout(COMMAND_TIMEOUT, async {
            let _lock = self.connect_lock.lock().await;
            let client = {
                let current = self.transport.read().unwrap();
                let current = current.as_ref().ok_or(BrokerError::NotConnected)?;
                if let Some(manager) = &current.manager {
                    return Ok((manager.connection.clone(), current.generation));
                }
                current.client.clone()
            };
            let manager = ManagedConnection::connect(&client, COMMAND_TIMEOUT).await?;
            let connection = manager.connection.clone();
            let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
            let mut current = self.transport.write().unwrap();
            let current = current.as_mut().unwrap();
            current.generation = generation;
            current.manager = Some(manager);
            Ok((connection, generation))
        })
        .await
        .map_err(|_| io_error("Redis connection acquisition timed out"))?
    }
    async fn command<T: FromRedisValue>(&self, cmd: &redis::Cmd) -> Result<T, BrokerError> {
        let (mut connection, generation) = self.connection().await?;
        let mut guard = CommandGuard {
            runtime: self,
            generation,
            completed: false,
        };
        let result = bounded("command", COMMAND_TIMEOUT, cmd.query_async(&mut connection)).await;
        guard.completed = result.is_ok();
        result
    }
    // Only replay operations whose Lua script reconciles a lost reply using stable ownership.
    async fn replayable<T: FromRedisValue>(&self, cmd: &redis::Cmd) -> Result<T, BrokerError> {
        match self.command(cmd).await {
            Err(error) if error.is_connection_error() => self.command(cmd).await,
            result => result,
        }
    }
    async fn slot(self: &Arc<Self>, state: &ConsumerState) -> Result<Slot, BrokerError> {
        loop {
            if state.cancelled.load(Ordering::SeqCst) {
                return Err(BrokerError::NotConnected);
            }
            let notified = self.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let limit = self.prefetch.load(Ordering::SeqCst);
            if self
                .occupied
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                    if limit == 0 || count < limit {
                        count.checked_add(1)
                    } else {
                        None
                    }
                })
                .is_ok()
            {
                return Ok(Slot {
                    runtime: self.clone(),
                    delivered: false,
                });
            }
            state.progress();
            tokio::select! { _ = notified => {}, _ = tokio::time::sleep(POLL_INTERVAL) => {} }
        }
    }
}
struct Slot {
    runtime: Arc<RedisBrokerRuntime>,
    delivered: bool,
}
impl Slot {
    fn delivered(mut self) -> Self {
        self.delivered = true;
        self.runtime.in_flight.fetch_add(1, Ordering::SeqCst);
        self
    }
}
impl Drop for Slot {
    fn drop(&mut self) {
        self.runtime.occupied.fetch_sub(1, Ordering::SeqCst);
        if self.delivered {
            let previous = self.runtime.in_flight.fetch_sub(1, Ordering::SeqCst);
            let limit = self.runtime.prefetch.load(Ordering::SeqCst);
            if limit > 0 && previous == limit {
                // A legitimately capacity-blocked consumer gets a fresh polling deadline
                // when work releases its slot, even after a very long task.
                *self.runtime.capacity_released.lock().unwrap() = Some(Instant::now());
            }
        }
        self.runtime.wake.notify_waiters();
    }
}

pub struct RedisBrokerBuilder {
    url: String,
    prefetch: u16,
    runtime: Option<Arc<RedisBrokerRuntime>>,
}
impl RedisBrokerBuilder {
    pub fn with_runtime(mut self, runtime: Arc<RedisBrokerRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }
}
#[async_trait]
impl BrokerBuilder for RedisBrokerBuilder {
    fn new(url: &str) -> Self {
        Self {
            url: url.into(),
            prefetch: 10,
            runtime: None,
        }
    }
    fn prefetch_count(mut self: Box<Self>, count: u16) -> Box<dyn BrokerBuilder> {
        self.prefetch = count;
        self
    }
    fn declare_queue(self: Box<Self>, _name: &str) -> Box<dyn BrokerBuilder> {
        self
    }
    fn heartbeat(self: Box<Self>, _heartbeat: Option<u16>) -> Box<dyn BrokerBuilder> {
        self
    }
    async fn build(&self, connection_timeout: u32) -> Result<Box<dyn Broker>, BrokerError> {
        let client =
            Client::open(self.url.as_str()).map_err(|_| io_error("invalid Redis broker URL"))?;
        let runtime = self
            .runtime
            .clone()
            .unwrap_or_else(|| Arc::new(RedisBrokerRuntime::new(self.prefetch)));
        runtime
            .install(
                Some(client),
                Duration::from_secs(connection_timeout.max(1) as u64),
            )
            .await?;
        Ok(Box::new(RedisBroker { runtime }))
    }
}
pub struct RedisBroker {
    runtime: Arc<RedisBrokerRuntime>,
}
#[derive(Clone)]
struct Channel {
    runtime: Arc<RedisBrokerRuntime>,
    queue: String,
}
impl Channel {
    fn process_map(&self) -> String {
        format!("_celery.{}_process_map", self.queue)
    }
    async fn fetch(self, state: Arc<ConsumerState>) -> Result<RedisDelivery, BrokerError> {
        loop {
            let slot = self.runtime.slot(&state).await?;
            let claim_id = Uuid::new_v4();
            let mut cmd = redis::cmd("EVAL");
            cmd.arg(CLAIM)
                .arg(3)
                .arg(&self.queue)
                .arg(self.process_map())
                .arg(format!("_celery.{}_claim:{claim_id}", self.queue))
                .arg(CLAIM_RESULT_RETENTION_SECONDS);
            let result: Result<Option<String>, _> = self.runtime.replayable(&cmd).await;
            state.progress();
            if let Some(raw) = result? {
                // A malformed body remains durably recorded rather than disappearing between pop and parse.
                let delivery: Delivery = serde_json::from_str(&raw)?;
                return Ok(RedisDelivery {
                    channel: self,
                    delivery,
                    raw,
                    retry_id: claim_id.to_string(),
                    slot: Mutex::new(Some(slot.delivered())),
                });
            }
            drop(slot);
            tokio::select! { _ = self.runtime.wake.notified() => {}, _ = tokio::time::sleep(POLL_INTERVAL) => {} }
        }
    }
    async fn send(&self, message: &Message) -> Result<(), BrokerError> {
        let raw = message.json_serialized(Some(DeliveryInfo::for_redis_default()))?;
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(SEND)
            .arg(2)
            .arg(&self.queue)
            .arg(format!("_celery.{}_send:{}", self.queue, Uuid::new_v4()))
            .arg(raw)
            .arg(CLAIM_RESULT_RETENTION_SECONDS);
        self.runtime.replayable::<i64>(&cmd).await?;
        Ok(())
    }
}
struct RedisDelivery {
    channel: Channel,
    delivery: Delivery,
    raw: String,
    retry_id: String,
    slot: Mutex<Option<Slot>>,
}
impl fmt::Debug for RedisDelivery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisDelivery")
            .field("queue", &self.channel.queue)
            .field("task_id", &self.delivery.headers.id)
            .finish()
    }
}
impl TryDeserializeMessage for RedisDelivery {
    fn try_deserialize_message(&self) -> Result<Message, ProtocolError> {
        self.delivery.try_deserialize_message()
    }
}
#[async_trait]
impl super::Delivery for RedisDelivery {
    async fn resend(
        &self,
        _broker: &dyn Broker,
        eta: Option<DateTime<Utc>>,
    ) -> Result<(), BrokerError> {
        let mut message = self.try_deserialize_message()?;
        message.headers.retries = Some(message.headers.retries.unwrap_or_default() + 1);
        message.headers.eta = eta;
        let raw = message.json_serialized(Some(DeliveryInfo::for_redis_default()))?;
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(RETRY)
            .arg(3)
            .arg(&self.channel.queue)
            .arg(self.channel.process_map())
            .arg(format!(
                "_celery.{}_retry:{}",
                self.channel.queue, self.retry_id
            ))
            .arg(&self.delivery.properties.correlation_id)
            .arg(&self.raw)
            .arg(raw)
            .arg(CLAIM_RESULT_RETENTION_SECONDS);
        self.channel.runtime.replayable::<i64>(&cmd).await?;
        self.slot.lock().unwrap().take();
        Ok(())
    }
    async fn remove(&self) -> Result<(), BrokerError> {
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(ACK)
            .arg(1)
            .arg(self.channel.process_map())
            .arg(&self.delivery.properties.correlation_id)
            .arg(&self.raw);
        self.channel.runtime.replayable::<i64>(&cmd).await?;
        self.slot.lock().unwrap().take();
        Ok(())
    }
    async fn ack(&self) -> Result<(), BrokerError> {
        self.remove().await
    }
}

type Fetch = Pin<Box<dyn Future<Output = Result<RedisDelivery, BrokerError>> + Send>>;
struct Consumer {
    channel: Channel,
    tag: String,
    state: Arc<ConsumerState>,
    error_handler: Box<dyn Fn(BrokerError) + Send + Sync>,
    fetch: Option<Fetch>,
}
impl DeliveryStream for Consumer {}
impl Stream for Consumer {
    type Item = Result<Box<dyn super::Delivery>, Box<dyn DeliveryError>>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.state.cancelled.load(Ordering::SeqCst) {
            return Poll::Ready(None);
        }
        if self.fetch.is_none() {
            self.fetch = Some(Box::pin(self.channel.clone().fetch(self.state.clone())));
        }
        match self.fetch.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.fetch = None;
                match result {
                    Ok(delivery) => Poll::Ready(Some(Ok(Box::new(delivery)))),
                    Err(error) => {
                        (self.error_handler)(error);
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
            }
        }
    }
}
impl Drop for Consumer {
    fn drop(&mut self) {
        self.channel
            .runtime
            .consumers
            .lock()
            .unwrap()
            .remove(&self.tag);
        if !self.state.cancelled.load(Ordering::SeqCst) {
            self.channel
                .runtime
                .stopped
                .lock()
                .unwrap()
                .insert(self.channel.queue.clone(), Instant::now());
        }
        self.channel.runtime.progress();
    }
}
#[async_trait]
impl Broker for RedisBroker {
    fn health_snapshot(&self) -> BrokerHealth {
        self.runtime.health_snapshot()
    }
    async fn health_check(&self) -> Result<BrokerHealth, BrokerError> {
        let role: Vec<redis::Value> = self.runtime.command(&redis::cmd("ROLE")).await?;
        if role
            .first()
            .and_then(|value| redis::from_redis_value::<String>(value).ok())
            .as_deref()
            != Some("master")
        {
            return Err(io_error("Redis broker endpoint is not the primary"));
        }
        Ok(self.health_snapshot())
    }
    async fn consume(
        &self,
        queue: &str,
        error_handler: Box<dyn Fn(BrokerError) + Send + Sync + 'static>,
    ) -> Result<(String, Box<dyn DeliveryStream>), BrokerError> {
        self.runtime.stopped.lock().unwrap().remove(queue);
        let tag = Uuid::new_v4().to_string();
        let state = Arc::new(ConsumerState {
            cancelled: AtomicBool::new(false),
            progress: Mutex::new(Instant::now()),
        });
        self.runtime
            .consumers
            .lock()
            .unwrap()
            .insert(tag.clone(), state.clone());
        self.runtime.progress();
        Ok((
            tag.clone(),
            Box::new(Consumer {
                channel: Channel {
                    runtime: self.runtime.clone(),
                    queue: queue.into(),
                },
                tag,
                state,
                error_handler,
                fetch: None,
            }),
        ))
    }
    async fn cancel(&self, tag: &str) -> Result<(), BrokerError> {
        if let Some(state) = self.runtime.consumers.lock().unwrap().get(tag) {
            state.cancelled.store(true, Ordering::SeqCst);
        }
        self.runtime.wake.notify_waiters();
        Ok(())
    }
    async fn ack(&self, delivery: &dyn super::Delivery) -> Result<(), BrokerError> {
        delivery.ack().await
    }
    async fn retry(
        &self,
        delivery: &dyn super::Delivery,
        eta: Option<DateTime<Utc>>,
    ) -> Result<(), BrokerError> {
        delivery.resend(self, eta).await
    }
    async fn send(&self, message: &Message, queue: &str) -> Result<(), BrokerError> {
        Channel {
            runtime: self.runtime.clone(),
            queue: queue.into(),
        }
        .send(message)
        .await
    }
    async fn increase_prefetch_count(&self) -> Result<(), BrokerError> {
        if self.runtime.unlimited {
            return Ok(());
        }
        self.runtime.prefetch.fetch_add(1, Ordering::SeqCst);
        self.runtime.wake.notify_waiters();
        Ok(())
    }
    async fn decrease_prefetch_count(&self) -> Result<(), BrokerError> {
        if self.runtime.unlimited {
            return Ok(());
        }
        self.runtime
            .prefetch
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_sub(1)
            })
            .ok();
        Ok(())
    }
    async fn close(&self) -> Result<(), BrokerError> {
        for state in self.runtime.consumers.lock().unwrap().values() {
            state.cancelled.store(true, Ordering::SeqCst);
        }
        self.runtime.wake.notify_waiters();
        let result = self.runtime.command::<String>(&redis::cmd("QUIT")).await;
        self.runtime.closed.store(true, Ordering::SeqCst);
        result.map(|_| ())
    }
    fn safe_url(&self) -> String {
        "redis://[managed-connection]".into()
    }
    async fn reconnect(&self, connection_timeout: u32) -> Result<(), BrokerError> {
        self.runtime.record_consumer_reconnect();
        self.runtime
            .install(None, Duration::from_secs(connection_timeout.max(1) as u64))
            .await
    }
    #[cfg(test)]
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}

#[cfg(test)]
mod tests;
