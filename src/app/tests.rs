use super::{Celery, CeleryBuilder};
use crate::broker::mock::MockBroker;
use crate::protocol::MessageContentType;
use crate::task::{Request, Signature, Task, TaskOptions, TaskResult};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

async fn build_basic_app() -> Celery {
    let celery = CeleryBuilder::new("mock-app", "mock://localhost:8000")
        .build()
        .await
        .unwrap();
    celery.register_task::<AddTask>().await.unwrap();
    celery.register_task::<MultiplyTask>().await.unwrap();
    celery
}

async fn build_configured_app() -> Celery {
    let celery = CeleryBuilder::new("mock-app", "mock://localhost:8000")
        .task_time_limit(10)
        .task_max_retries(100)
        .task_content_type(MessageContentType::Yaml)
        .build()
        .await
        .unwrap();
    celery.register_task::<AddTask>().await.unwrap();
    celery.register_task::<MultiplyTask>().await.unwrap();
    celery
}

// We can't use the #[task] macro from inside the crate, unfortunately, so we have
// to implement tasks the old fashion way.

struct AddTask {
    request: Request<Self>,
    options: TaskOptions,
}

impl AddTask {
    fn new(x: i32, y: i32) -> Signature<Self> {
        Signature::<Self>::new(AddParams { x, y })
    }
}

#[async_trait]
impl Task for AddTask {
    const NAME: &'static str = "add";
    const ARGS: &'static [&'static str] = &["x", "y"];

    type Params = AddParams;
    type Returns = i32;

    fn from_request(request: Request<Self>, options: TaskOptions) -> Self {
        Self { request, options }
    }

    fn request(&self) -> &Request<Self> {
        &self.request
    }

    fn options(&self) -> &TaskOptions {
        &self.options
    }

    async fn run(&self, params: Self::Params) -> TaskResult<Self::Returns> {
        Ok(params.x + params.y)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct AddParams {
    x: i32,
    y: i32,
}

struct MultiplyTask {
    request: Request<Self>,
    options: TaskOptions,
}

impl MultiplyTask {
    fn new(x: i32, y: i32) -> Signature<Self> {
        Signature::<Self>::new(MultiplyParams { x, y })
    }
}

#[async_trait]
impl Task for MultiplyTask {
    const NAME: &'static str = "multiply";
    const ARGS: &'static [&'static str] = &["x", "y"];
    const DEFAULTS: TaskOptions = TaskOptions {
        time_limit: Some(5),
        hard_time_limit: Some(10),
        expires: None,
        max_retries: Some(1000),
        min_retry_delay: None,
        max_retry_delay: None,
        retry_for_unexpected: None,
        acks_late: None,
        content_type: None,
    };

    type Params = MultiplyParams;
    type Returns = i32;

    fn from_request(request: Request<Self>, options: TaskOptions) -> Self {
        Self { request, options }
    }

    fn request(&self) -> &Request<Self> {
        &self.request
    }

    fn options(&self) -> &TaskOptions {
        &self.options
    }

    async fn run(&self, params: Self::Params) -> TaskResult<Self::Returns> {
        Ok(params.x * params.y)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct MultiplyParams {
    x: i32,
    y: i32,
}

#[tokio::test]
async fn test_app_name() {
    let app = build_basic_app().await;
    assert!(app.name == "mock-app");
}

#[tokio::test]
async fn test_add_task_registered() {
    let app = build_basic_app().await;
    assert!(app.task_trace_builders.read().await.contains_key("add"));
}

#[tokio::test]
async fn test_send_task() {
    let app = build_basic_app().await;
    let result = app.send_task(AddTask::new(1, 2)).await.unwrap();
    let mock_broker = app.broker.into_any().downcast::<MockBroker>().unwrap();
    let sent_tasks = mock_broker.sent_tasks.read().await;
    let message = &sent_tasks.get(&result.task_id).unwrap().0;
    assert!(message.headers.task == "add");
}

#[tokio::test]
async fn test_send_task_with_countdown() {
    let app = build_basic_app().await;
    let result = app
        .send_task(AddTask::new(1, 2).with_countdown(2))
        .await
        .unwrap();
    let mock_broker = app.broker.into_any().downcast::<MockBroker>().unwrap();
    let sent_tasks = mock_broker.sent_tasks.read().await;
    let message = &sent_tasks.get(&result.task_id).unwrap().0;
    assert!(message.headers.eta.is_some());
}

#[tokio::test]
async fn test_send_task_with_eta() {
    let app = build_basic_app().await;
    let result = app
        .send_task(AddTask::new(1, 2).with_eta(DateTime::<Utc>::from(SystemTime::now())))
        .await
        .unwrap();
    let mock_broker = app.broker.into_any().downcast::<MockBroker>().unwrap();
    let sent_tasks = mock_broker.sent_tasks.read().await;
    let message = &sent_tasks.get(&result.task_id).unwrap().0;
    assert!(message.headers.eta.is_some());
}

#[tokio::test]
async fn test_send_task_with_expires_in() {
    let app = build_basic_app().await;
    let result = app
        .send_task(AddTask::new(1, 2).with_expires_in(10))
        .await
        .unwrap();
    let mock_broker = app.broker.into_any().downcast::<MockBroker>().unwrap();
    let sent_tasks = mock_broker.sent_tasks.read().await;
    let message = &sent_tasks.get(&result.task_id).unwrap().0;
    assert!(message.headers.expires.is_some());
}

#[tokio::test]
async fn test_send_task_with_expires() {
    let app = build_basic_app().await;
    let dt = DateTime::<Utc>::from(SystemTime::now()) + Duration::seconds(10);
    let result = app
        .send_task(AddTask::new(1, 2).with_expires(dt))
        .await
        .unwrap();
    let mock_broker = app.broker.into_any().downcast::<MockBroker>().unwrap();
    let sent_tasks = mock_broker.sent_tasks.read().await;
    let message = &sent_tasks.get(&result.task_id).unwrap().0;
    assert!(message.headers.expires.is_some());
}

#[tokio::test]
async fn test_send_task_with_content_type() {
    let app = build_basic_app().await;
    let result = app
        .send_task(AddTask::new(1, 2).with_content_type(MessageContentType::Yaml))
        .await
        .unwrap();
    let mock_broker = app.broker.into_any().downcast::<MockBroker>().unwrap();
    let sent_tasks = mock_broker.sent_tasks.read().await;
    let message = &sent_tasks.get(&result.task_id).unwrap().0;
    assert!(message.properties.content_type == "application/x-yaml");
}

#[tokio::test]
async fn test_send_task_with_time_limit() {
    let app = build_basic_app().await;
    let result = app
        .send_task(AddTask::new(1, 2).with_time_limit(5))
        .await
        .unwrap();
    let mock_broker = app.broker.into_any().downcast::<MockBroker>().unwrap();
    let sent_tasks = mock_broker.sent_tasks.read().await;
    let message = &sent_tasks.get(&result.task_id).unwrap().0;
    assert!(message.headers.timelimit == (None, Some(5)));
}

#[tokio::test]
async fn test_send_task_with_hard_time_limit() {
    let app = build_basic_app().await;
    let result = app
        .send_task(AddTask::new(1, 2).with_hard_time_limit(5))
        .await
        .unwrap();
    let mock_broker = app.broker.into_any().downcast::<MockBroker>().unwrap();
    let sent_tasks = mock_broker.sent_tasks.read().await;
    let message = &sent_tasks.get(&result.task_id).unwrap().0;
    assert!(message.headers.timelimit == (Some(5), None));
}

#[tokio::test]
async fn test_configured_app_send_task_app_defaults() {
    let app = build_configured_app().await;
    let result = app.send_task(AddTask::new(1, 2)).await.unwrap();
    let mock_broker = app.broker.into_any().downcast::<MockBroker>().unwrap();
    let sent_tasks = mock_broker.sent_tasks.read().await;
    let message = &sent_tasks.get(&result.task_id).unwrap().0;
    assert!(message.headers.timelimit == (None, Some(10)));
    assert!(message.properties.content_type == "application/x-yaml");
}

#[tokio::test]
async fn test_configured_app_send_task_task_defaults() {
    let app = build_configured_app().await;
    let result = app.send_task(MultiplyTask::new(1, 2)).await.unwrap();
    let mock_broker = app.broker.into_any().downcast::<MockBroker>().unwrap();
    let sent_tasks = mock_broker.sent_tasks.read().await;
    let message = &sent_tasks.get(&result.task_id).unwrap().0;
    assert!(message.headers.timelimit == (Some(10), Some(5)));
    assert!(message.properties.content_type == "application/x-yaml");
}

#[tokio::test]
async fn test_configured_app_send_task_request_overrides() {
    let app = build_configured_app().await;
    let result = app
        .send_task(
            MultiplyTask::new(1, 2)
                .with_time_limit(2)
                .with_content_type(MessageContentType::Json),
        )
        .await
        .unwrap();
    let mock_broker = app.broker.into_any().downcast::<MockBroker>().unwrap();
    let sent_tasks = mock_broker.sent_tasks.read().await;
    let message = &sent_tasks.get(&result.task_id).unwrap().0;
    assert!(message.headers.timelimit == (Some(10), Some(2)));
    assert!(message.properties.content_type == "application/json");
}

struct TestSignals(tokio::sync::mpsc::UnboundedReceiver<super::SigType>);

#[async_trait]
impl super::SignalSource for TestSignals {
    async fn wait(&mut self) -> Result<super::SigType, std::io::Error> {
        Ok(self
            .0
            .recv()
            .await
            .expect("test signal source remains open"))
    }
}

#[derive(Clone, Copy, PartialEq)]
enum FinalizationPhase {
    Ack,
    Retry,
}

struct FinalizationGate {
    phase: FinalizationPhase,
    started: std::sync::Arc<tokio::sync::Notify>,
    release: tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

struct RecoveryTestBroker {
    consumes: std::sync::atomic::AtomicUsize,
    gate: Option<FinalizationGate>,
}

impl RecoveryTestBroker {
    fn new() -> Self {
        Self {
            consumes: std::sync::atomic::AtomicUsize::new(0),
            gate: None,
        }
    }

    async fn finalize(&self, phase: FinalizationPhase) {
        if let Some(gate) = &self.gate {
            if gate.phase == phase {
                let release = gate.release.lock().await.take();
                if let Some(release) = release {
                    gate.started.notify_one();
                    release.await.unwrap();
                }
            }
        }
    }
}
struct PendingDeliveryStream;

impl crate::broker::DeliveryStream for PendingDeliveryStream {}
impl futures::Stream for PendingDeliveryStream {
    type Item = Result<Box<dyn crate::broker::Delivery>, Box<dyn crate::broker::DeliveryError>>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Pending
    }
}

#[async_trait]
impl crate::broker::Broker for RecoveryTestBroker {
    fn safe_url(&self) -> String {
        "mock://recovery".into()
    }
    async fn consume(
        &self,
        _: &str,
        error_handler: Box<dyn Fn(crate::error::BrokerError) + Send + Sync + 'static>,
    ) -> Result<(String, Box<dyn crate::broker::DeliveryStream>), crate::error::BrokerError> {
        if self
            .consumes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            == 0
        {
            error_handler(crate::error::BrokerError::NotConnected);
        }
        Ok(("consumer".into(), Box::new(PendingDeliveryStream)))
    }
    async fn cancel(&self, _: &str) -> Result<(), crate::error::BrokerError> {
        Ok(())
    }
    async fn ack(&self, _: &dyn crate::broker::Delivery) -> Result<(), crate::error::BrokerError> {
        self.finalize(FinalizationPhase::Ack).await;
        Ok(())
    }
    async fn retry(
        &self,
        _: &dyn crate::broker::Delivery,
        _: Option<DateTime<Utc>>,
    ) -> Result<(), crate::error::BrokerError> {
        self.finalize(FinalizationPhase::Retry).await;
        Ok(())
    }
    async fn send(
        &self,
        _: &crate::protocol::Message,
        _: &str,
    ) -> Result<(), crate::error::BrokerError> {
        Ok(())
    }
    async fn increase_prefetch_count(&self) -> Result<(), crate::error::BrokerError> {
        Ok(())
    }
    async fn decrease_prefetch_count(&self) -> Result<(), crate::error::BrokerError> {
        Ok(())
    }
    async fn close(&self) -> Result<(), crate::error::BrokerError> {
        Ok(())
    }
    async fn reconnect(&self, _: u32) -> Result<(), crate::error::BrokerError> {
        Ok(())
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}

#[tokio::test]
async fn consumer_generation_reconnect_preserves_shutdown_tasks() {
    use super::{DeliveryTasks, SigType};
    use std::sync::Arc;
    use tokio::time::{timeout, Duration};

    let app = Arc::new(
        CeleryBuilder::new("recovery", "mock://recovery")
            .with_broker(Box::new(RecoveryTestBroker::new()))
            .build()
            .await
            .unwrap(),
    );
    let mut tasks = DeliveryTasks::new();
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
    // Ownership begins at spawn, before the handler gets its first poll.
    tasks.handles.spawn(async {
        finish_rx.await.unwrap();
    });
    let (signal_tx, signal_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut signals = TestSignals(signal_rx);

    app.clone()
        ._consume_from(&["celery"], &mut tasks, &mut signals)
        .await
        .expect_err("the first generation loses its broker connection");
    app.broker.reconnect(2).await.unwrap();
    signal_tx.send(SigType::Terminate).unwrap();
    let next_generation = app
        .clone()
        ._consume_from(&["celery"], &mut tasks, &mut signals);
    tokio::pin!(next_generation);
    assert!(timeout(Duration::from_millis(10), &mut next_generation)
        .await
        .is_err());

    finish_tx.send(()).unwrap();
    timeout(Duration::from_secs(1), next_generation)
        .await
        .expect("shutdown must observe completion from the old generation")
        .unwrap();
}

#[tokio::test]
async fn shutdown_during_reconnect_backoff_stops_recovery_promptly() {
    use super::SigType;
    use std::sync::Arc;
    use tokio::time::{timeout, Duration};

    let app = Arc::new(
        CeleryBuilder::new("recovery", "mock://recovery")
            .with_broker(Box::new(RecoveryTestBroker::new()))
            .broker_connection_retry_delay(60)
            .build()
            .await
            .unwrap(),
    );
    let (signal_tx, signal_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut signals = TestSignals(signal_rx);
    let consume = app.consume_with_signals(&["celery"], &mut signals);
    tokio::pin!(consume);
    assert!(timeout(Duration::from_millis(10), &mut consume)
        .await
        .is_err());
    signal_tx.send(SigType::Terminate).unwrap();
    timeout(Duration::from_secs(1), consume)
        .await
        .expect("SIGTERM must interrupt reconnect backoff")
        .unwrap();
}

#[derive(Debug)]
struct TestDelivery(crate::protocol::Message);

impl crate::protocol::TryDeserializeMessage for TestDelivery {
    fn try_deserialize_message(
        &self,
    ) -> Result<crate::protocol::Message, crate::error::ProtocolError> {
        Ok(self.0.clone())
    }
}

#[async_trait]
impl crate::broker::Delivery for TestDelivery {
    async fn ack(&self) -> Result<(), crate::error::BrokerError> {
        Ok(())
    }
    async fn remove(&self) -> Result<(), crate::error::BrokerError> {
        Ok(())
    }
    async fn resend(
        &self,
        _: &dyn crate::broker::Broker,
        _: Option<DateTime<Utc>>,
    ) -> Result<(), crate::error::BrokerError> {
        Ok(())
    }
}

struct RetryTask {
    request: Request<Self>,
    options: TaskOptions,
}

#[async_trait]
impl Task for RetryTask {
    const NAME: &'static str = "retry_during_shutdown";
    const ARGS: &'static [&'static str] = &[];
    type Params = ();
    type Returns = ();

    fn from_request(request: Request<Self>, options: TaskOptions) -> Self {
        Self { request, options }
    }
    fn request(&self) -> &Request<Self> {
        &self.request
    }
    fn options(&self) -> &TaskOptions {
        &self.options
    }
    async fn run(&self, _: ()) -> TaskResult<()> {
        Err(crate::error::TaskError::Retry(None))
    }
}

#[tokio::test]
async fn shutdown_owns_early_ack_late_ack_and_retry_finalization() {
    use super::DeliveryTasks;
    use std::{convert::TryFrom, sync::Arc};
    use tokio::time::{timeout, Duration};

    for (acks_late, phase) in [
        (false, FinalizationPhase::Ack),
        (true, FinalizationPhase::Ack),
        (true, FinalizationPhase::Retry),
    ] {
        let started = Arc::new(tokio::sync::Notify::new());
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let mut broker = RecoveryTestBroker::new();
        broker.gate = Some(FinalizationGate {
            phase,
            started: started.clone(),
            release: tokio::sync::Mutex::new(Some(release_rx)),
        });
        let app = Arc::new(
            CeleryBuilder::new("shutdown", "mock://shutdown")
                .with_broker(Box::new(broker))
                .acks_late(acks_late)
                .task_max_retries(1)
                .build()
                .await
                .unwrap(),
        );
        app.register_task::<AddTask>().await.unwrap();
        app.register_task::<RetryTask>().await.unwrap();
        let message = if phase == FinalizationPhase::Retry {
            crate::protocol::Message::try_from(Signature::<RetryTask>::new(()))
        } else {
            crate::protocol::Message::try_from(AddTask::new(1, 2))
        }
        .unwrap();
        let mut tasks = DeliveryTasks::new();
        tasks
            .handles
            .spawn(app.handle_delivery(Box::new(TestDelivery(message))));
        timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("real delivery handler must reach the gated broker operation");
        let (_signal_tx, signal_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut signals = TestSignals(signal_rx);
        let drain = tasks.drain(&mut signals);
        tokio::pin!(drain);
        assert!(timeout(Duration::from_millis(10), &mut drain)
            .await
            .is_err());
        release_tx.send(()).unwrap();
        timeout(Duration::from_secs(1), drain)
            .await
            .expect("shutdown completes after the delivery handler finalizes")
            .unwrap();
    }
}

#[tokio::test]
async fn acquired_eta_delivery_remains_owned_during_shutdown() {
    use super::{DeliveryTasks, SigType};
    use std::{convert::TryFrom, sync::Arc};
    use tokio::time::{timeout, Duration};

    let app = Arc::new(build_basic_app().await);
    let message =
        crate::protocol::Message::try_from(AddTask::new(1, 2).with_countdown(60)).unwrap();
    let mut tasks = DeliveryTasks::new();
    tasks
        .handles
        .spawn(app.handle_delivery(Box::new(TestDelivery(message))));
    let (signal_tx, signal_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut signals = TestSignals(signal_rx);
    let drain = tasks.drain(&mut signals);
    tokio::pin!(drain);
    assert!(timeout(Duration::from_millis(10), &mut drain)
        .await
        .is_err());
    signal_tx.send(SigType::Interrupt).unwrap();
    assert!(matches!(
        timeout(Duration::from_secs(1), drain).await.unwrap(),
        Err(crate::error::CeleryError::ForcedShutdown)
    ));
}
