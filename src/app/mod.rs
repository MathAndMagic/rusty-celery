use colored::Colorize;
use futures::stream::StreamExt;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::error::Error;
use std::sync::Arc;
use tokio::select;

#[cfg(unix)]
use tokio::signal::unix::{signal, Signal, SignalKind};

use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::time::{self, Duration};
use tokio_stream::StreamMap;

mod trace;

use crate::broker::{
    broker_builder_from_url, build_and_connect, configure_task_routes, Broker, BrokerBuilder,
    Delivery,
};
use crate::error::{BrokerError, CeleryError, TraceError};
use crate::protocol::{Message, MessageContentType};
use crate::routing::Rule;
use crate::task::{AsyncResult, Signature, Task, TaskOptions};
use trace::{build_tracer, TraceBuilder, TracerTrait};

struct Config {
    name: String,
    hostname: String,
    broker_builder: Box<dyn BrokerBuilder>,
    broker_override: Option<Box<dyn Broker>>,
    broker_connection_timeout: u32,
    broker_connection_retry: bool,
    broker_connection_max_retries: u32,
    broker_connection_retry_delay: u32,
    default_queue: String,
    task_options: TaskOptions,
    task_routes: Vec<(String, String)>,
}

/// Used to create a [`Celery`] app with a custom configuration.
pub struct CeleryBuilder {
    config: Config,
}

impl CeleryBuilder {
    /// Get a [`CeleryBuilder`] for creating a [`Celery`] app with a custom configuration.
    pub fn new(name: &str, broker_url: &str) -> Self {
        Self {
            config: Config {
                name: name.into(),
                hostname: format!(
                    "{}@{}",
                    name,
                    hostname::get()
                        .ok()
                        .and_then(|sys_hostname| sys_hostname.into_string().ok())
                        .unwrap_or_else(|| "unknown".into())
                ),
                broker_builder: broker_builder_from_url(broker_url),
                broker_override: None,
                broker_connection_timeout: 2,
                broker_connection_retry: true,
                broker_connection_max_retries: 5,
                broker_connection_retry_delay: 5,
                default_queue: "celery".into(),
                task_options: TaskOptions::default(),
                task_routes: vec![],
            },
        }
    }

    /// Provide a fully constructed broker to use instead of building/connecting via URL.
    pub fn with_broker(mut self, broker: Box<dyn Broker>) -> Self {
        self.config.broker_override = Some(broker);
        self
    }

    /// Set the node name of the app. Defaults to `"{name}@{sys hostname}"`.
    ///
    /// *This field should probably be named "nodename" to avoid confusion with the
    /// system hostname, but we're trying to be consistent with Python Celery.*
    pub fn hostname(mut self, hostname: &str) -> Self {
        self.config.hostname = hostname.into();
        self
    }

    /// Set the name of the default queue to something other than "celery".
    pub fn default_queue(mut self, queue_name: &str) -> Self {
        self.config.default_queue = queue_name.into();
        self
    }

    /// Set the prefetch count. The default value depends on the broker implementation,
    /// but it's recommended that you always set this to a value that works best
    /// for your application.
    ///
    /// This may take some tuning, as it depends on a lot of factors, such
    /// as whether your tasks are IO bound (higher prefetch count is better) or CPU bound (lower
    /// prefetch count is better).
    pub fn prefetch_count(mut self, prefetch_count: u16) -> Self {
        self.config.broker_builder = self.config.broker_builder.prefetch_count(prefetch_count);
        self
    }

    /// Set the broker heartbeat. The default value depends on the broker implementation.
    pub fn heartbeat(mut self, heartbeat: Option<u16>) -> Self {
        self.config.broker_builder = self.config.broker_builder.heartbeat(heartbeat);
        self
    }

    /// Set an app-level time limit for tasks (see [`TaskOptions::time_limit`]).
    pub fn task_time_limit(mut self, task_time_limit: u32) -> Self {
        self.config.task_options.time_limit = Some(task_time_limit);
        self
    }

    /// Set an app-level hard time limit for tasks (see [`TaskOptions::hard_time_limit`]).
    ///
    /// *Note that this is really only for compatability with Python workers*.
    /// `time_limit` and `hard_time_limit` are treated the same by Rust workers, and if both
    /// are set, the minimum of the two will be used.
    pub fn task_hard_time_limit(mut self, task_hard_time_limit: u32) -> Self {
        self.config.task_options.hard_time_limit = Some(task_hard_time_limit);
        self
    }

    /// Set an app-level maximum number of retries for tasks (see [`TaskOptions::max_retries`]).
    pub fn task_max_retries(mut self, task_max_retries: u32) -> Self {
        self.config.task_options.max_retries = Some(task_max_retries);
        self
    }

    /// Set an app-level minimum retry delay for tasks (see [`TaskOptions::min_retry_delay`]).
    pub fn task_min_retry_delay(mut self, task_min_retry_delay: u32) -> Self {
        self.config.task_options.min_retry_delay = Some(task_min_retry_delay);
        self
    }

    /// Set an app-level maximum retry delay for tasks (see [`TaskOptions::max_retry_delay`]).
    pub fn task_max_retry_delay(mut self, task_max_retry_delay: u32) -> Self {
        self.config.task_options.max_retry_delay = Some(task_max_retry_delay);
        self
    }

    /// Set whether by default `UnexpectedError`s should be retried for (see
    /// [`TaskOptions::retry_for_unexpected`]).
    pub fn task_retry_for_unexpected(mut self, retry_for_unexpected: bool) -> Self {
        self.config.task_options.retry_for_unexpected = Some(retry_for_unexpected);
        self
    }

    /// Set whether by default a task is acknowledged before or after execution (see
    /// [`TaskOptions::acks_late`]).
    pub fn acks_late(mut self, acks_late: bool) -> Self {
        self.config.task_options.acks_late = Some(acks_late);
        self
    }

    /// Set default serialization format a task will have (see [`TaskOptions::content_type`]).
    pub fn task_content_type(mut self, content_type: MessageContentType) -> Self {
        self.config.task_options.content_type = Some(content_type);
        self
    }

    /// Add a routing rule.
    pub fn task_route(mut self, pattern: &str, queue: &str) -> Self {
        self.config.task_routes.push((pattern.into(), queue.into()));
        self
    }

    /// Set a timeout in seconds before giving up establishing a connection to a broker.
    pub fn broker_connection_timeout(mut self, timeout: u32) -> Self {
        self.config.broker_connection_timeout = timeout;
        self
    }

    /// Set whether or not to automatically try to re-establish connection to the AMQP broker.
    pub fn broker_connection_retry(mut self, retry: bool) -> Self {
        self.config.broker_connection_retry = retry;
        self
    }

    /// Set the maximum number of retries before we give up trying to re-establish connection
    /// to the AMQP broker.
    pub fn broker_connection_max_retries(mut self, max_retries: u32) -> Self {
        self.config.broker_connection_max_retries = max_retries;
        self
    }

    /// Set the number of seconds to wait before re-trying the connection with the broker.
    pub fn broker_connection_retry_delay(mut self, retry_delay: u32) -> Self {
        self.config.broker_connection_retry_delay = retry_delay;
        self
    }

    /// Construct a [`Celery`] app with the current configuration.
    pub async fn build(self) -> Result<Celery, CeleryError> {
        if let Some(broker) = self.config.broker_override {
            let mut task_routes = Vec::with_capacity(self.config.task_routes.len());
            for (pattern, queue) in &self.config.task_routes {
                task_routes.push(Rule::new(pattern, queue).map_err(BrokerError::from)?);
            }

            return Ok(Celery {
                name: self.config.name,
                hostname: self.config.hostname,
                broker,
                default_queue: self.config.default_queue,
                task_options: self.config.task_options,
                task_routes,
                task_trace_builders: RwLock::new(HashMap::new()),
                broker_connection_timeout: self.config.broker_connection_timeout,
                broker_connection_retry: self.config.broker_connection_retry,
                broker_connection_max_retries: self.config.broker_connection_max_retries,
                broker_connection_retry_delay: self.config.broker_connection_retry_delay,
            });
        }

        // Declare default queue to broker.
        let broker_builder = self
            .config
            .broker_builder
            .declare_queue(&self.config.default_queue);

        let (broker_builder, task_routes) =
            configure_task_routes(broker_builder, &self.config.task_routes)?;

        let broker = build_and_connect(
            broker_builder,
            self.config.broker_connection_timeout,
            if self.config.broker_connection_retry {
                self.config.broker_connection_max_retries
            } else {
                0
            },
            self.config.broker_connection_retry_delay,
        )
        .await?;

        Ok(Celery {
            name: self.config.name,
            hostname: self.config.hostname,
            broker,
            default_queue: self.config.default_queue,
            task_options: self.config.task_options,
            task_routes,
            task_trace_builders: RwLock::new(HashMap::new()),
            broker_connection_timeout: self.config.broker_connection_timeout,
            broker_connection_retry: self.config.broker_connection_retry,
            broker_connection_max_retries: self.config.broker_connection_max_retries,
            broker_connection_retry_delay: self.config.broker_connection_retry_delay,
        })
    }
}

/// A [`Celery`] app is used to produce or consume tasks asynchronously. This is the struct that is
/// created with the [`app!`](crate::app!) macro.
pub struct Celery {
    /// An arbitrary, human-readable name for the app.
    pub name: String,

    /// Node name of the app.
    pub hostname: String,

    /// The app's broker.
    pub broker: Box<dyn Broker>,

    /// The default queue to send and receive from.
    pub default_queue: String,

    /// Default task options.
    pub task_options: TaskOptions,

    /// A vector of routing rules in the order of their importance.
    task_routes: Vec<Rule>,

    /// Mapping of task name to task tracer factory. Used to create a task tracer
    /// from an incoming message.
    task_trace_builders: RwLock<HashMap<String, TraceBuilder>>,

    broker_connection_timeout: u32,
    broker_connection_retry: bool,
    broker_connection_max_retries: u32,
    broker_connection_retry_delay: u32,
}

impl Celery {
    /// Print a pretty ASCII art logo and configuration settings.
    ///
    /// This is useful and fun to print from a worker application right after
    /// the [`Celery`] app is initialized.
    pub async fn display_pretty(&self) {
        // Cool ASCII logo with hostname.
        let banner = format!(
            r#"
  _________________          >_<
 /  ______________ \         | |
/  /              \_\  ,---. | | ,---. ,--.--.,--. ,--.
| /   .<      >.      | .-. :| || .-. :|  .--' \  '  /
| |   (        )      \   --.| |\   --.|  |     \   /
| |    --o--o--        `----'`-' `----'`--'   .-'  /
| |  _/        \_   __                         `--'
| | / \________/ \ / /
| \    |      |   / /
 \ \_____________/ /    {}
  \_______________/
"#,
            self.hostname
        );
        println!("{}", banner.truecolor(255, 102, 0));

        // Broker.
        println!("{}", "[broker]".bold());
        println!(" {}", self.broker.safe_url());
        println!();

        // Registered tasks.
        println!("{}", "[tasks]".bold());
        for task in self.task_trace_builders.read().await.keys() {
            println!(" . {task}");
        }
        println!();
    }

    /// Send a task to a remote worker. Returns an [`AsyncResult`] with the task ID of the task
    /// if it was successfully sent.
    pub async fn send_task<T: Task>(
        &self,
        mut task_sig: Signature<T>,
    ) -> Result<AsyncResult, CeleryError> {
        task_sig.options.update(&self.task_options);
        let maybe_queue = task_sig.queue.take();
        let queue = maybe_queue.as_deref().unwrap_or_else(|| {
            crate::routing::route(T::NAME, &self.task_routes).unwrap_or(&self.default_queue)
        });
        let message = Message::try_from(task_sig)?;
        info!(
            "Sending task {}[{}] to {}",
            T::NAME,
            message.task_id(),
            queue,
        );
        self.broker.send(&message, queue).await?;
        Ok(AsyncResult::new(message.task_id()))
    }

    /// Register a task.
    pub async fn register_task<T: Task + 'static>(&self) -> Result<(), CeleryError> {
        let mut task_trace_builders = self.task_trace_builders.write().await;
        if task_trace_builders.contains_key(T::NAME) {
            Err(CeleryError::TaskRegistrationError(T::NAME.into()))
        } else {
            task_trace_builders.insert(T::NAME.into(), Box::new(build_tracer::<T>));
            debug!("Registered task {}", T::NAME);
            Ok(())
        }
    }

    async fn get_task_tracer(
        self: &Arc<Self>,
        message: Message,
    ) -> Result<Box<dyn TracerTrait>, Box<dyn Error + Send + Sync + 'static>> {
        let task_trace_builders = self.task_trace_builders.read().await;
        if let Some(build_tracer) = task_trace_builders.get(&message.headers.task) {
            Ok(build_tracer(
                self.clone(),
                message,
                self.task_options,
                self.hostname.clone(),
            )
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)?)
        } else {
            Err(
                Box::new(CeleryError::UnregisteredTaskError(message.headers.task))
                    as Box<dyn Error + Send + Sync + 'static>,
            )
        }
    }

    /// Tries converting a delivery into a `Message`, executing the corresponding task,
    /// and communicating with the broker.
    async fn try_handle_delivery(
        self: &Arc<Self>,
        delivery: Box<dyn Delivery>,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        // Coerce the delivery into a protocol message.
        let message = match delivery.try_deserialize_message() {
            Ok(message) => message,
            Err(e) => {
                // This is a naughty message that we can't handle, so we'll ack it with
                // the broker so it gets deleted.
                self.broker
                    .ack(delivery.as_ref())
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)?;
                return Err(Box::new(e));
            }
        };

        // Try deserializing the message to create a task wrapped in a task tracer.
        // (The tracer handles all of the logic of directly interacting with the task
        // to execute it and run the post-execution functions).
        let mut tracer = match self.get_task_tracer(message).await {
            Ok(tracer) => tracer,
            Err(e) => {
                // Even though the message meta data was okay, we failed to deserialize
                // the body of the message for some reason, so ack it with the broker
                // to delete it and return an error.
                self.broker
                    .ack(delivery.as_ref())
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)?;
                return Err(e);
            }
        };

        if tracer.is_delayed() {
            // Task has an ETA, so we need to increment the prefetch count so that
            // we can receive other tasks while we wait for the ETA.
            if let Err(e) = self.broker.increase_prefetch_count().await {
                // If for some reason this operation fails, we should stop tracing
                // this task and send it back to the broker to retry.
                // Otherwise we could reach the prefetch_count and end up blocking
                // other deliveries if there are a high number of messages with a
                // future ETA.
                self.broker
                    .retry(delivery.as_ref(), None)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)?;
                self.broker
                    .ack(delivery.as_ref())
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)?;
                return Err(Box::new(e));
            };

            // Then wait for the task to be ready.
            tracer.wait().await;
        }

        // If acks_late is false, we acknowledge the message before tracing it.
        if !tracer.acks_late() {
            self.broker
                .ack(delivery.as_ref())
                .await
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)?;
        }

        // Try tracing the task now.
        // NOTE: we don't need to log errors from the trace here since the tracer
        // handles all errors at it's own level or the task level. In this function
        // we only log errors at the broker and delivery level.
        if let Err(TraceError::Retry(retry_eta)) = tracer.trace().await {
            // If retry error -> retry the task.
            self.broker
                .retry(delivery.as_ref(), retry_eta)
                .await
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)?;
        }

        // If we have not done it before, we have to acknowledge the message now.
        if tracer.acks_late() {
            self.broker
                .ack(delivery.as_ref())
                .await
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)?;
        }

        // If we had increased the prefetch count above due to a future ETA, we have
        // to decrease it back down to restore balance to the universe.
        if tracer.is_delayed() {
            self.broker
                .decrease_prefetch_count()
                .await
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)?;
        }

        Ok(())
    }

    /// Wraps `try_handle_delivery` to catch any and all errors that might occur.
    async fn handle_delivery(self: Arc<Self>, delivery: Box<dyn Delivery>) {
        if let Err(e) = self.try_handle_delivery(delivery).await {
            error!("{}", e);
        }
    }

    /// Close channels and connections.
    pub async fn close(&self) -> Result<(), CeleryError> {
        Ok(self.broker.close().await?)
    }

    /// Consume tasks from the default queue.
    pub async fn consume(self: &Arc<Self>) -> Result<(), CeleryError> {
        let queues = &[&self.default_queue.clone()[..]];
        Self::consume_from(self, queues).await
    }

    /// Consume tasks from any number of queues.
    pub async fn consume_from(self: &Arc<Self>, queues: &[&str]) -> Result<(), CeleryError> {
        self.consume_with_signals(queues, &mut Ender::new()?).await
    }

    async fn consume_with_signals(
        self: &Arc<Self>,
        queues: &[&str],
        ender: &mut impl SignalSource,
    ) -> Result<(), CeleryError> {
        // Delivery handlers outlive an individual broker connection. Own them until their
        // business work and acknowledgement/retry finish, including recovery and shutdown.
        let mut tasks = DeliveryTasks::new();
        loop {
            let result = self.clone()._consume_from(queues, &mut tasks, ender).await;
            if !self.broker_connection_retry {
                return result;
            }

            if let Err(err) = result {
                match err {
                    CeleryError::BrokerError(broker_err) => {
                        if broker_err.is_connection_error() {
                            error!("Broker connection failed");
                        } else {
                            return Err(CeleryError::BrokerError(broker_err));
                        }
                    }
                    _ => return Err(err),
                };
            } else {
                return result;
            }

            let mut reconnect_successful: bool = false;
            for _ in 0..self.broker_connection_max_retries {
                info!("Trying to re-establish connection with broker");
                let reconnect = async {
                    time::sleep(Duration::from_secs(
                        self.broker_connection_retry_delay as u64,
                    ))
                    .await;
                    self.broker.reconnect(self.broker_connection_timeout).await
                };
                let reconnected = select! {
                    biased;
                    _ = ender.wait() => {
                        info!("Warm shutdown during broker recovery...");
                        return tasks.drain(ender).await;
                    }
                    result = reconnect => result,
                };

                match reconnected {
                    Err(err) => {
                        if err.is_connection_error() {
                            continue;
                        }
                        return Err(CeleryError::BrokerError(err));
                    }
                    Ok(_) => {
                        info!("Successfully reconnected with broker");
                        reconnect_successful = true;
                        break;
                    }
                };
            }

            if !reconnect_successful {
                return Err(CeleryError::BrokerError(BrokerError::NotConnected));
            }
        }
    }

    #[allow(clippy::cognitive_complexity)]
    async fn _consume_from(
        self: Arc<Self>,
        queues: &[&str],
        tasks: &mut DeliveryTasks,
        ender: &mut impl SignalSource,
    ) -> Result<(), CeleryError> {
        if queues.is_empty() {
            return Err(CeleryError::NoQueueToConsume);
        }

        info!("Consuming from {:?}", queues);

        // Stream of errors from broker. The capacity here is arbitrary because a single
        // error from the broker should trigger this method to return early.
        let (broker_error_tx, mut broker_error_rx) = mpsc::channel::<BrokerError>(100);

        // Stream of deliveries from the queue.
        let mut stream_map = StreamMap::new();
        let mut consumer_tags = vec![];
        for queue in queues {
            let broker_error_tx = broker_error_tx.clone();

            let (consumer_tag, consumer) = self
                .broker
                .consume(
                    queue,
                    Box::new(move |e| {
                        broker_error_tx.clone().try_send(e).ok();
                    }),
                )
                .await?;
            stream_map.insert(queue, consumer);
            consumer_tags.push(consumer_tag);
        }

        // This is the main loop where we receive deliveries and pass them off
        // to be handled by spawning `self.handle_delivery`.
        // At the same time we are also listening for a SIGINT (Ctrl+C) or SIGTERM interruption.
        // If that occurs we break from this loop and move to the warm shutdown loop
        // if there are still delivery handlers, including acquired tasks waiting for ETA
        // and tasks publishing their retry or final acknowledgement.
        loop {
            select! {
                maybe_delivery_result = stream_map.next() => {
                    if let Some((queue, delivery_result)) = maybe_delivery_result {
                        match delivery_result {
                            Ok(delivery) => {
                                debug!("Received delivery from {}: {:?}", queue, delivery);
                                tasks.handles.spawn(self.clone().handle_delivery(delivery));
                            }
                            Err(e) => {
                                error!("Deliver failed: {}", e);
                            }
                        }
                    }
                },
                ending = ender.wait() => {
                    if let Ok(SigType::Interrupt) = ending {
                        warn!("Ope! Hitting Ctrl+C again will terminate all running tasks!");
                    }
                    info!("Warm shutdown...");
                    break;
                },
                finished = tasks.handles.join_next(), if !tasks.handles.is_empty() => {
                    DeliveryTasks::record_completion(finished);
                },
                maybe_broker_error = broker_error_rx.recv() => {
                    if let Some(broker_error) = maybe_broker_error {
                        error!("{}", broker_error);
                        return Err(broker_error.into());
                    }
                }
            };
        }

        // Cancel consumers.
        for consumer_tag in consumer_tags {
            debug!("Cancelling consumer {}", consumer_tag);
            self.broker.cancel(&consumer_tag).await?;
        }

        tasks.drain(ender).await?;

        info!("No more pending tasks. See ya!");

        Ok(())
    }
}

struct DeliveryTasks {
    handles: tokio::task::JoinSet<()>,
}

impl DeliveryTasks {
    fn new() -> Self {
        Self {
            handles: tokio::task::JoinSet::new(),
        }
    }

    fn record_completion(result: Option<Result<(), tokio::task::JoinError>>) {
        if let Some(Err(error)) = result {
            error!("Delivery handler failed: {}", error);
        }
    }

    async fn drain(&mut self, ender: &mut impl SignalSource) -> Result<(), CeleryError> {
        if !self.handles.is_empty() {
            info!("Waiting on {} delivery handlers...", self.handles.len());
        }
        while !self.handles.is_empty() {
            select! {
                ending = ender.wait() => {
                    if let Ok(SigType::Interrupt) = ending {
                        warn!("Okay fine, shutting down now. See ya!");
                        return Err(CeleryError::ForcedShutdown);
                    }
                }
                finished = self.handles.join_next() => Self::record_completion(finished),
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
trait SignalSource: Send {
    async fn wait(&mut self) -> Result<SigType, std::io::Error>;
}

#[allow(unused)]
enum SigType {
    /// Equivalent to SIGINT on unix systems.
    Interrupt,
    /// Equivalent to SIGTERM on unix systems.
    Terminate,
}

/// The ender listens for signals.
#[cfg(unix)]
struct Ender {
    sigint: Signal,
    sigterm: Signal,
}

#[cfg(unix)]
impl Ender {
    fn new() -> Result<Self, std::io::Error> {
        let sigint = signal(SignalKind::interrupt())?;
        let sigterm = signal(SignalKind::terminate())?;

        Ok(Ender { sigint, sigterm })
    }
}

#[cfg(unix)]
#[async_trait::async_trait]
impl SignalSource for Ender {
    /// Waits for either an interrupt or terminate.
    async fn wait(&mut self) -> Result<SigType, std::io::Error> {
        let sigtype;

        select! {
            _ = self.sigint.recv() => {
                sigtype = SigType::Interrupt
            },
            _ = self.sigterm.recv() => {
                sigtype = SigType::Terminate
            }
        }

        Ok(sigtype)
    }
}

#[cfg(windows)]
struct Ender;

#[cfg(windows)]
impl Ender {
    fn new() -> Result<Self, std::io::Error> {
        Ok(Ender)
    }
}

#[cfg(windows)]
#[async_trait::async_trait]
impl SignalSource for Ender {
    async fn wait(&mut self) -> Result<SigType, std::io::Error> {
        tokio::signal::ctrl_c().await?;

        Ok(SigType::Interrupt)
    }
}

#[cfg(test)]
mod tests;
