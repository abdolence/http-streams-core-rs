//! Accounting and observability for one streamed body, in either direction.
//!
//! This is the merge of two implementations that had independently converged on the same
//! design (`axum-streams`' `progress.rs` and `reqwest-streams`' `observability.rs`), and it
//! takes the union of their behaviour. Both dependents drive it; neither owns a copy.
//!
//! Unlike an earlier design, the tracing callsites live **here** rather than in the binding
//! crates. `tracing` bakes a span's name and target into a `static Metadata`, so they cannot be
//! passed in at runtime; keeping them here means one target, `http_streams_core`, with the
//! direction carried as a span field instead.

use crate::error::StreamError;
use bytes::Bytes;
use futures::stream::{Stream, TryStreamExt};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// Reported about once a second unless overridden.
pub const DEFAULT_PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

/// Which HTTP message the body belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Direction {
    /// A request body: uploaded by a client, received by a server.
    Request,
    /// A response body: produced by a server, read by a client.
    #[default]
    Response,
}

impl Direction {
    /// A short, stable name, reported as the `direction` tracing field.
    pub fn as_str(&self) -> &'static str {
        match self {
            Direction::Request => "request",
            Direction::Response => "response",
        }
    }
}

/// Which end of the connection this code is running on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Side {
    /// Running in an HTTP client.
    #[default]
    Client,
    /// Running in an HTTP server.
    Server,
}

impl Side {
    /// A short, stable name, reported as the `side` tracing field.
    pub fn as_str(&self) -> &'static str {
        match self {
            Side::Client => "client",
            Side::Server => "server",
        }
    }
}

/// How a stream ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamOutcome {
    /// Still running; reported by interim progress only.
    InProgress,
    /// Reached the end of the body with no errors.
    Completed,
    /// Ended early because the other side of the stream went away.
    Aborted,
    /// Reached its end, but at least one error was reported along the way.
    Failed,
}

impl StreamOutcome {
    /// A short, stable name, reported as the `outcome` tracing field.
    pub fn as_str(&self) -> &'static str {
        match self {
            StreamOutcome::InProgress => "in_progress",
            StreamOutcome::Completed => "completed",
            StreamOutcome::Aborted => "aborted",
            StreamOutcome::Failed => "failed",
        }
    }
}

/// A snapshot of one stream's accounting.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct StreamProgress {
    /// Items encoded or decoded so far.
    pub items: u64,
    /// Body bytes transferred so far.
    pub bytes: u64,
    /// Errors reported so far. Not every error is terminal.
    pub errors: u64,
    /// Time since the stream was created.
    pub elapsed: Duration,
    /// How the stream ended, or [`InProgress`] if it has not.
    ///
    /// [`InProgress`]: StreamOutcome::InProgress
    pub outcome: StreamOutcome,
}

/// Called for every error reported by a stream.
pub type StreamErrorHandler = Arc<dyn Fn(&StreamError) + Send + Sync + 'static>;

/// Called for every progress report, interim and terminal.
pub type StreamProgressHandler = Arc<dyn Fn(&StreamProgress) + Send + Sync + 'static>;

/// The direction-neutral subset of the dependents' options structs.
///
/// Both `StreamBodyAsOptions` and `ReqwestStreamOptions` keep their own definitions, because they
/// carry direction-specific fields, and neither could add inherent builder methods to a type
/// defined here (E0116). Each builds one of these internally instead.
#[derive(Clone)]
#[non_exhaustive]
pub struct ProgressOptions {
    /// Invoked for every error reported by the stream.
    pub on_error: Option<StreamErrorHandler>,
    /// Invoked for every progress report.
    pub on_progress: Option<StreamProgressHandler>,
    /// How often to report interim progress. `None` disables the time trigger.
    pub progress_interval: Option<Duration>,
    /// Report interim progress every N items. `None` disables the item trigger.
    pub progress_items: Option<u64>,
}

impl ProgressOptions {
    /// Default options: report about once a second, no item trigger, no callbacks.
    pub fn new() -> Self {
        Self {
            on_error: None,
            on_progress: None,
            progress_interval: Some(DEFAULT_PROGRESS_INTERVAL),
            progress_items: None,
        }
    }

    /// Set the error callback.
    pub fn on_error(mut self, handler: StreamErrorHandler) -> Self {
        self.on_error = Some(handler);
        self
    }

    /// Set the progress callback.
    pub fn on_progress(mut self, handler: StreamProgressHandler) -> Self {
        self.on_progress = Some(handler);
        self
    }

    /// Set the interim reporting interval.
    pub fn progress_interval(mut self, interval: Duration) -> Self {
        self.progress_interval = Some(interval);
        self
    }

    /// Report interim progress every `items` items.
    pub fn progress_items(mut self, items: u64) -> Self {
        self.progress_items = Some(items);
        self
    }
}

impl Default for ProgressOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// What a stream is about, for the span covering it.
///
/// Fields the caller cannot know are simply left `None` and are omitted from the span rather
/// than reported as a placeholder.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct StreamContext {
    /// The format's [`format_name`](crate::StreamFormat::format_name).
    ///
    /// `Cow` rather than `&'static str` because `axum-streams`' long-standing public
    /// `StreamingFormat::format_name` returns a borrowed `&str`, and that signature cannot
    /// change without breaking the third-party implementations it was made public for.
    /// Core's own formats all return `&'static str` and so allocate nothing.
    pub format: std::borrow::Cow<'static, str>,
    /// Request or response body.
    pub direction: Direction,
    /// Client or server.
    pub side: Side,
    /// Response status, where one is known.
    pub status: Option<u16>,
    /// Declared body length, where one is known.
    pub content_length: Option<u64>,
    /// The negotiated content type, where one is known.
    pub content_type: Option<String>,
    /// The per-object limit in force, where one applies.
    pub max_obj_len: Option<usize>,
    /// The read-buffer size in force, where one applies.
    pub buf_capacity: Option<usize>,
}

impl StreamContext {
    /// A context for `format`, in the given direction and on the given side.
    pub fn new(
        format: impl Into<std::borrow::Cow<'static, str>>,
        direction: Direction,
        side: Side,
    ) -> Self {
        Self {
            format: format.into(),
            direction,
            side,
            ..Default::default()
        }
    }

    /// Record the response status.
    pub fn status(mut self, status: u16) -> Self {
        self.status = Some(status);
        self
    }

    /// Record the declared body length.
    pub fn content_length(mut self, len: Option<u64>) -> Self {
        self.content_length = len;
        self
    }

    /// Record the content type.
    pub fn content_type(mut self, ct: impl Into<String>) -> Self {
        self.content_type = Some(ct.into());
        self
    }

    /// Record the per-object limit.
    pub fn max_obj_len(mut self, len: usize) -> Self {
        self.max_obj_len = Some(len);
        self
    }

    /// Record the read-buffer size.
    pub fn buf_capacity(mut self, cap: usize) -> Self {
        self.buf_capacity = Some(cap);
        self
    }
}

/// What the accounting needs to know about an error passing through a stream.
///
/// Deliberately not `&StreamError`: a binding crate's pipeline carries that crate's own error
/// type, `axum::Error` say, and it could not implement a trait from here for it anyway,
/// since both the trait and the type would be foreign to it (orphan rule). Everything the
/// accounting actually needs is a `Display` and, when available, the typed error.
pub struct ErrorInfo<'a> {
    display: &'a dyn std::fmt::Display,
    stream_error: Option<&'a StreamError>,
}

impl<'a> ErrorInfo<'a> {
    /// The error's `Display`, for the `error` tracing field.
    pub fn display(&self) -> &dyn std::fmt::Display {
        self.display
    }

    /// The typed error, when this stream carries [`StreamError`]s.
    pub fn stream_error(&self) -> Option<&'a StreamError> {
        self.stream_error
    }

    /// A short, stable kind name for the `error_kind` tracing field.
    pub fn kind_str(&self) -> &'static str {
        self.stream_error.map_or("unknown", |e| e.kind().as_str())
    }
}

/// Lets [`instrument`] classify items without being generic over the item type.
///
/// A `T` that appeared only in a `where` clause would be an unconstrained type parameter
/// (E0207), and a `PhantomData<T>` would drag `T`'s auto traits into the stream's type, which
/// would break any binding whose `T` carries no `Send + 'b` bound even though the method it
/// backs promises a `Send` stream. `reqwest-streams`' CSV reader is one such.
pub trait ProgressItem {
    /// The error this item carries, if it is one.
    fn progress_error(&self) -> Option<ErrorInfo<'_>>;
}

/// Covers every `Result` whose error is a standard error, so binding crates get this for their
/// own error types without implementing anything.
impl<T, E> ProgressItem for Result<T, E>
where
    E: std::error::Error + 'static,
{
    fn progress_error(&self) -> Option<ErrorInfo<'_>> {
        self.as_ref().err().map(|err| ErrorInfo {
            display: err,
            // Resolves at compile time for any concrete `E`, so this costs nothing when the
            // stream does not carry `StreamError`s.
            stream_error: (err as &dyn std::any::Any).downcast_ref::<StreamError>(),
        })
    }
}

/// Checked at `ERROR`, the least verbose level the accounting can produce: a failed stream
/// reports there, so gating any higher would mean `RUST_LOG=http_streams_core=error` silently
/// loses the totals of the very streams it asked about. Every more verbose filter enables
/// `ERROR` too, so this can never suppress wanted output.
#[cfg(feature = "tracing")]
fn tracing_enabled() -> bool {
    tracing::enabled!(target: "http_streams_core", tracing::Level::ERROR)
}

#[cfg(not(feature = "tracing"))]
fn tracing_enabled() -> bool {
    false
}

/// Shared accounting for one streamed body.
///
/// Bytes are counted on the byte stream and items on the item stream, so the two counters live
/// in different combinators and share this state. The ordering is `Relaxed` throughout: these
/// are counters, not synchronisation.
struct ProgressState {
    items: AtomicU64,
    bytes: AtomicU64,
    errors: AtomicU64,
    last_emit_micros: AtomicU64,
    next_item_step: AtomicU64,
    polled: AtomicBool,
    finalized: AtomicBool,
    start: Instant,
    interval_micros: Option<u64>,
    item_step: Option<u64>,
    on_error: Option<StreamErrorHandler>,
    on_progress: Option<StreamProgressHandler>,
    #[cfg(feature = "tracing")]
    span: tracing::Span,
}

impl ProgressState {
    /// Returns `None` when nobody is listening, in which case every accounting call below
    /// short-circuits on a single `Option` check.
    #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
    fn maybe_new(context: &StreamContext, options: &ProgressOptions) -> Option<Arc<Self>> {
        if options.on_progress.is_none() && options.on_error.is_none() && !tracing_enabled() {
            return None;
        }

        // A step of zero would never advance, so treat it as "disabled" rather than looping.
        let item_step = options.progress_items.filter(|step| *step > 0);

        Some(Arc::new(Self {
            items: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            last_emit_micros: AtomicU64::new(0),
            next_item_step: AtomicU64::new(item_step.unwrap_or(u64::MAX)),
            polled: AtomicBool::new(false),
            finalized: AtomicBool::new(false),
            start: Instant::now(),
            interval_micros: options
                .progress_interval
                .map(|interval| interval.as_micros() as u64),
            item_step,
            on_error: options.on_error.clone(),
            on_progress: options.on_progress.clone(),
            #[cfg(feature = "tracing")]
            span: Self::new_span(context),
        }))
    }

    /// The span covering the whole stream, created by the caller while its own span is still
    /// current, so collectors nest it under their request rather than orphaning it. The stream
    /// itself is polled later, potentially from an entirely different task.
    ///
    /// Every counter is declared up front as an empty field so it can be filled in later with
    /// [`tracing::Span::record`]: collectors that read span attributes (OpenTelemetry and
    /// friends) then see `items`/`bytes`/`outcome` as structured values on a span whose
    /// duration is the streaming duration, instead of having to parse log messages.
    ///
    /// No URL is recorded, deliberately: it carries query strings and userinfo, which
    /// routinely means presigned-URL signatures and `?api_key=`.
    #[cfg(feature = "tracing")]
    fn new_span(context: &StreamContext) -> tracing::Span {
        let span = tracing::info_span!(
            target: "http_streams_core",
            "http_streams_core::stream",
            format = context.format.as_ref(),
            direction = context.direction.as_str(),
            side = context.side.as_str(),
            // `Option` is a `Value` that simply skips the field when it is empty.
            status = context.status,
            content_length = context.content_length,
            content_type = context.content_type.as_deref(),
            max_obj_len = tracing::field::Empty,
            buf_capacity = tracing::field::Empty,
            items = tracing::field::Empty,
            bytes = tracing::field::Empty,
            errors = tracing::field::Empty,
            elapsed_ms = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );

        // `usize::MAX` means "no limit", which is noise rather than information.
        if let Some(max) = context.max_obj_len.filter(|m| *m != usize::MAX) {
            span.record("max_obj_len", max as u64);
        }
        if let Some(cap) = context.buf_capacity {
            span.record("buf_capacity", cap as u64);
        }

        span
    }

    fn record_bytes(&self, len: u64) {
        let bytes = self.bytes.fetch_add(len, Ordering::Relaxed) + len;
        let items = self.items.load(Ordering::Relaxed);

        #[cfg(feature = "tracing")]
        tracing::trace!(
            target: "http_streams_core",
            parent: &self.span,
            chunk_bytes = len,
            items,
            bytes,
            "Transferred an HTTP body chunk"
        );

        // Progress is driven from transferred bytes as well as from items, because a single
        // item can take a long time: one large Arrow batch, or a JSON array streamed slowly,
        // would otherwise report nothing at all until it completed. Emitting resets the
        // interval, so a chunk and an item cannot both report for the same tick.
        if !self.finalized.load(Ordering::Relaxed) && self.should_emit_elapsed() {
            self.emit(
                StreamOutcome::InProgress,
                items,
                bytes,
                self.errors.load(Ordering::Relaxed),
            );
        }
    }

    fn record_item(&self) {
        let items = self.items.fetch_add(1, Ordering::Relaxed) + 1;

        // Nothing may be reported after the summary, or the final snapshot would no longer be
        // final. A consumer is free to keep polling a stream past its end.
        if !self.finalized.load(Ordering::Relaxed) && self.should_emit_items(items) {
            self.emit(
                StreamOutcome::InProgress,
                items,
                self.bytes.load(Ordering::Relaxed),
                self.errors.load(Ordering::Relaxed),
            );
        }
    }

    /// Errors are reported as they happen but are deliberately **not** terminal.
    ///
    /// Only some of them are: `FramedRead` latches its own error state and ends the stream,
    /// but the JSON Lines and CSV formats produce their decoding errors from a successfully
    /// framed line, and the stream carries on to the next one. Finalising here would stop
    /// counting the remaining items of a stream that is still perfectly healthy, so the
    /// terminal outcome is decided at the end instead, from this counter.
    fn record_error(&self, info: &ErrorInfo<'_>) {
        self.errors.fetch_add(1, Ordering::Relaxed);

        #[cfg(feature = "tracing")]
        tracing::error!(
            target: "http_streams_core",
            parent: &self.span,
            error = %info.display(),
            error_kind = info.kind_str(),
            "An error occurred while streaming an HTTP body"
        );

        // Only fires for streams that carry `StreamError`s. A binding whose pipeline uses its
        // own error type reports through its own typed callback instead, so that the closure
        // its users already wrote keeps compiling.
        if let (Some(handler), Some(err)) = (&self.on_error, info.stream_error()) {
            handler(err);
        }
    }

    /// The time trigger, checked when bytes move.
    ///
    /// The two triggers are deliberately driven from *different* signals rather than both
    /// being checked everywhere. Bytes are what keeps flowing regardless of how the payload
    /// divides into items (a single large item, or a slow one, still reports), so elapsed time
    /// is checked here. Items drive the item-step trigger below.
    ///
    /// Checking both from both places double-reports: every pipeline counts the same payload
    /// twice, once as items and once as bytes, so a zero interval would emit two events per
    /// unit rather than one.
    fn should_emit_elapsed(&self) -> bool {
        let Some(interval) = self.interval_micros else {
            return false;
        };

        let elapsed = self.start.elapsed().as_micros() as u64;
        let since_last = elapsed.saturating_sub(self.last_emit_micros.load(Ordering::Relaxed));
        if since_last >= interval {
            self.last_emit_micros.store(elapsed, Ordering::Relaxed);
            return true;
        }

        false
    }

    /// The item-step trigger, checked when items are counted.
    fn should_emit_items(&self, items: u64) -> bool {
        let Some(step) = self.item_step else {
            return false;
        };

        if items < self.next_item_step.load(Ordering::Relaxed) {
            return false;
        }

        // Skip past every step the current count already crossed, so a single poll carrying
        // many items cannot queue up a burst of events.
        self.next_item_step
            .store(items - (items % step) + step, Ordering::Relaxed);

        // Emitting resets the time trigger as well, so a step and a tick that fall together
        // produce one event rather than two.
        self.last_emit_micros
            .store(self.start.elapsed().as_micros() as u64, Ordering::Relaxed);

        true
    }

    fn mark_polled(&self) {
        self.polled.store(true, Ordering::Relaxed);
    }

    /// Emits the terminal snapshot, exactly once per stream.
    ///
    /// A stream that was never polled reports nothing at all. Building one and dropping it
    /// unconsumed is routine (a `?` short-circuits, a handler returns early), and reporting
    /// those as aborted would bury the real ones in `items=0 bytes=0` noise.
    fn finalize(&self, aborted: bool) {
        if !self.polled.load(Ordering::Relaxed) || self.finalized.swap(true, Ordering::Relaxed) {
            return;
        }

        let items = self.items.load(Ordering::Relaxed);
        let bytes = self.bytes.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);

        let outcome = if errors > 0 {
            StreamOutcome::Failed
        } else if aborted {
            StreamOutcome::Aborted
        } else {
            StreamOutcome::Completed
        };

        // Recorded once, here rather than on every progress report: subscribers are free to
        // treat `record` as append-only (`tracing-subscriber`'s formatter does), so writing a
        // field repeatedly makes the rendered span grow with every tick. Once per span also
        // means the values a collector reads are the final ones.
        #[cfg(feature = "tracing")]
        {
            self.span.record("items", items);
            self.span.record("bytes", bytes);
            self.span.record("errors", errors);
            self.span
                .record("elapsed_ms", self.start.elapsed().as_millis() as u64);
            self.span.record("outcome", outcome.as_str());
        }

        self.emit(outcome, items, bytes, errors);
    }

    fn emit(&self, outcome: StreamOutcome, items: u64, bytes: u64, errors: u64) {
        let progress = StreamProgress {
            items,
            bytes,
            errors,
            elapsed: self.start.elapsed(),
            outcome,
        };

        #[cfg(feature = "tracing")]
        {
            let elapsed_ms = progress.elapsed.as_millis() as u64;

            match outcome {
                // Interim progress is chatter; the summary is the line worth keeping, and a
                // truncated stream is worth an operator's attention.
                StreamOutcome::InProgress => tracing::debug!(
                    target: "http_streams_core",
                    parent: &self.span,
                    items,
                    bytes,
                    elapsed_ms,
                    "Streaming an HTTP body"
                ),
                StreamOutcome::Failed => tracing::error!(
                    target: "http_streams_core",
                    parent: &self.span,
                    items,
                    bytes,
                    errors,
                    elapsed_ms,
                    outcome = outcome.as_str(),
                    "Failed streaming an HTTP body"
                ),
                // Completed, and aborted: an end that stops early is ordinary.
                _ => tracing::info!(
                    target: "http_streams_core",
                    parent: &self.span,
                    items,
                    bytes,
                    errors,
                    elapsed_ms,
                    outcome = outcome.as_str(),
                    "Finished streaming an HTTP body"
                ),
            }
        }

        if let Some(handler) = &self.on_progress {
            handler(&progress);
        }
    }
}

/// The accounting handle threaded through one stream's pipeline.
///
/// `None` inside means nobody is listening and every method is a no-op. Cheap to clone.
#[derive(Clone)]
pub struct Progress(Option<Arc<ProgressState>>);

impl Progress {
    /// Build a handle for one stream.
    ///
    /// Call this while the caller's own tracing span is still current, and before the body is
    /// consumed, so the span nests correctly and the context fields are still available.
    pub fn new(context: &StreamContext, options: &ProgressOptions) -> Self {
        Progress(ProgressState::maybe_new(context, options))
    }

    /// A handle that reports nothing.
    pub fn disabled() -> Self {
        Progress(None)
    }

    /// Whether anything is listening. Useful to skip work that only feeds reporting.
    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    /// Count one item.
    ///
    /// A no-op when nobody is listening. Provided for bindings that cannot use
    /// [`count_items`] because their pipeline's item type carries no `'static`-ish bound:
    /// applying a combinator would force one onto their public signature.
    pub fn record_item(&self) {
        if let Some(state) = &self.0 {
            state.record_item();
        }
    }

    /// Count `len` transferred bytes.
    ///
    /// A no-op when nobody is listening. The combinator form is [`count_bytes`].
    pub fn record_bytes(&self, len: u64) {
        if let Some(state) = &self.0 {
            state.record_bytes(len);
        }
    }
}

/// Whether [`instrument`] should count the items it sees.
///
/// The outermost stream of an encode pipeline yields `Bytes` chunks, not items, so counting
/// its `Ok`s would report a JSON array's `[` and `]` as items. In that case items are counted
/// upstream by [`count_items`] and this is set to [`Bytes`].
///
/// [`Bytes`]: Counting::Bytes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Counting {
    /// Each `Ok` is one item.
    Items,
    /// Each `Ok` is a byte chunk; items are counted elsewhere.
    Bytes,
}

/// Counts the bytes flowing through a byte stream.
///
/// Applied to the byte stream rather than the item stream so that `bytes` is what actually
/// crossed the wire, independently of how many objects that turned into.
pub fn count_bytes<'b, S, E>(
    stream: S,
    progress: &Progress,
) -> impl Stream<Item = Result<Bytes, E>> + Send + 'b
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'b,
    E: 'b,
{
    let progress = progress.clone();
    stream.inspect_ok(move |chunk| {
        if let Some(state) = &progress.0 {
            state.record_bytes(chunk.len() as u64);
        }
    })
}

/// Counts items flowing through an item stream, without touching errors or the outcome.
///
/// Used on the encode side, where items exist only upstream of the encoder. This is the one
/// place they are still items rather than bytes.
pub fn count_items<'b, S, T, E>(
    stream: S,
    progress: &Progress,
) -> impl Stream<Item = Result<T, E>> + Send + 'b
where
    S: Stream<Item = Result<T, E>> + Send + 'b,
    // Deliberately no `T: Send`. The *stream* must be `Send`; its item type need not be, and
    // requiring it would break callers whose `T` carries no such bound. Same reasoning as
    // [`ProgressItem`].
    T: 'b,
    E: 'b,
{
    let progress = progress.clone();
    stream.inspect_ok(move |_| {
        if let Some(state) = &progress.0 {
            state.record_item();
        }
    })
}

/// Reports errors, owns the outcome state machine, and optionally counts items.
///
/// Wrap the **outermost** stream with this: every error passes through there, and its `Drop`
/// is the only way to notice an end that stopped early.
pub fn instrument<'b, S>(
    stream: S,
    progress: Progress,
    counting: Counting,
) -> impl Stream<Item = S::Item> + Send + 'b
where
    S: Stream + Unpin + Send + 'b,
    S::Item: ProgressItem,
{
    ProgressStream {
        inner: stream,
        progress,
        counting,
    }
}

struct ProgressStream<S> {
    inner: S,
    progress: Progress,
    counting: Counting,
}

impl<S> Stream for ProgressStream<S>
where
    S: Stream + Unpin,
    S::Item: ProgressItem,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Safe without any projection: `Self: Unpin` whenever `S: Unpin`, which is exactly the
        // bound above. That keeps `#![forbid(unsafe_code)]` intact.
        let this = self.get_mut();

        // Borrowed, not cloned: `progress` and `inner` are disjoint fields, so this avoids an
        // atomic refcount bump on every single poll.
        let Some(state) = this.progress.0.as_ref() else {
            return Pin::new(&mut this.inner).poll_next(cx);
        };

        // Polling here drives the whole pipeline synchronously, so entering the span gives
        // everything it touches the stream's context. `poll_next` is synchronous, so this
        // guard is never held across an await.
        #[cfg(feature = "tracing")]
        let _entered = state.span.enter();

        state.mark_polled();

        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(item)) => {
                match item.progress_error() {
                    Some(info) => state.record_error(&info),
                    None => {
                        if this.counting == Counting::Items {
                            state.record_item();
                        }
                    }
                }
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                state.finalize(false);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> Drop for ProgressStream<S> {
    fn drop(&mut self) {
        if let Some(state) = &self.progress.0 {
            // A no-op when the stream already ran to completion, or was never polled.
            state.finalize(true);
        }
    }
}
