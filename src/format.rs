//! The symmetric format abstraction: one trait for identity, one per direction.
//!
//! Deliberately **item-level** rather than stream-level. A stream-level trait would have to
//! name a concrete stream error type in its signature, which is exactly what ties
//! `axum_streams::StreamingFormat` to `axum::Error` and makes it unmovable. Encoding an item
//! into a buffer names nothing but [`StreamError`].
//!
//! [`ItemEncoder::prologue`] and [`ItemEncoder::epilogue`] exist because framing is not purely
//! per-item: a JSON array opens with `[` and closes with `]`, and Arrow IPC ends with an
//! eight-byte end-of-stream marker. This is precisely the hook that
//! [`tokio_util::codec::Encoder`] lacks (`FramedWrite` never calls anything at end of stream),
//! and the reason this crate does not implement that trait.

use crate::content_type::ContentType;
use crate::error::StreamError;
use bytes::BytesMut;

/// Identity and content-type negotiation, shared by both directions.
pub trait StreamFormat {
    /// A short, stable name, reported as the `format` tracing field.
    fn format_name(&self) -> &'static str;

    /// The `Content-Type` this format emits when encoding.
    fn default_content_type(&self) -> &'static str;

    /// Whether an incoming `Content-Type` should be accepted as this format.
    ///
    /// Implementations should be lenient about parameters, which [`ContentType`] has already
    /// stripped, and should accept the well-known aliases for their wire format.
    fn accepts_content_type(&self, ct: &ContentType<'_>) -> bool {
        ct.matches(self.default_content_type())
    }
}

/// Encodes items of type `T` into bytes.
pub trait StreamFormatEncode<T>: StreamFormat {
    /// The per-stream encoder. A fresh one is built for every body, because framing state
    /// (item index, Arrow's dictionary tracker, CSV's header flag) is per-stream.
    type Encoder: ItemEncoder<T>;

    /// Build an encoder for one body.
    fn encoder(&self) -> Self::Encoder;
}

/// Per-stream encoding state.
///
/// `index` is passed to [`encode`] rather than tracked internally because several formats key
/// their framing off it: the JSON array writes a separator before every item but the first,
/// CSV writes its header row only at index 0, and Arrow emits the schema message only at
/// index 0. Implementations that do not care simply ignore it.
///
/// [`encode`]: ItemEncoder::encode
pub trait ItemEncoder<T> {
    /// Bytes emitted before the first item, if any.
    fn prologue(&mut self, buf: &mut BytesMut) -> Result<(), StreamError> {
        let _ = buf;
        Ok(())
    }

    /// Encode one item.
    fn encode(&mut self, item: &T, index: u64, buf: &mut BytesMut) -> Result<(), StreamError>;

    /// Bytes emitted after the last item, if any.
    ///
    /// Called on normal end of stream only, never after an error, and never if the stream is
    /// dropped, since in both cases the body is already truncated and a well-formed terminator
    /// would misrepresent it as complete.
    fn epilogue(&mut self, buf: &mut BytesMut) -> Result<(), StreamError> {
        let _ = buf;
        Ok(())
    }
}

/// Turns one framed record into an item.
///
/// Separate from the framer so that the framer need not be generic over `T`. That is not
/// tidiness: a decoder that structurally mentions `T` forces a `T: 'b` bound onto every
/// caller's public signature, whereas a parser mentioning `T` only in its return type does not.
pub trait FrameParser<F, T> {
    /// Deserialise one framed record.
    ///
    /// An error here is **not** terminal: the record framed correctly, so the decoder knows
    /// exactly where the next one starts and the stream continues.
    fn parse(&self, frame: F) -> Result<T, StreamError>;
}

/// Yields frames unchanged, for formats whose framer already produces items.
///
/// Used by every format whose framing cannot be separated from deserialisation. A JSON array
/// element is not self-delimiting, so finding its end and parsing it are one operation.
#[derive(Debug, Clone, Copy, Default)]
pub struct IdentityParser;

impl<T> FrameParser<Result<T, StreamError>, T> for IdentityParser {
    fn parse(&self, frame: Result<T, StreamError>) -> Result<T, StreamError> {
        frame
    }
}

/// Decodes bytes into items of type `T`.
pub trait StreamFormatDecode<T>: StreamFormat {
    /// One framed record, before deserialisation.
    ///
    /// Formats that can separate the two use a type independent of `T`, such as `csv::ByteRecord`,
    /// say. Formats that cannot use `Result<T, StreamError>` and an [`IdentityParser`].
    type Frame;

    /// Splits the body into records.
    ///
    /// Its error type is the *framing* error, and it is terminal: `FramedRead` latches its
    /// error state and ends the stream. Correct, because a framer that has lost track of where
    /// records begin cannot resynchronise. Per-record deserialisation failures are not
    /// terminal and are reported by [`Self::Parser`] instead.
    type Framer: tokio_util::codec::Decoder<Item = Self::Frame, Error = StreamError> + Send;

    /// Deserialises framed records.
    type Parser: FrameParser<Self::Frame, T> + Send;

    /// Build a framer for one body.
    fn framer(&self, options: &DecodeOptions) -> Self::Framer;

    /// Build a parser for one body.
    fn parser(&self) -> Self::Parser;
}

/// A format that can be constructed without configuration.
///
/// Needed by server-side extractors, which are built by the framework rather than by the user
/// and so have nowhere to receive constructor arguments. A method rather than a `Default`
/// supertrait on [`StreamFormat`], so that a format which genuinely cannot be defaulted is not
/// locked out of the rest of the abstraction.
pub trait DefaultFormat: Sized {
    /// The configuration to use when the caller supplied none.
    fn default_format() -> Self;
}

/// Limits applied while decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct DecodeOptions {
    /// Maximum length of a single decoded object.
    pub max_obj_len: usize,
    /// Initial capacity of the read buffer.
    pub buf_capacity: usize,
}

/// 8 KiB, matching the existing `reqwest-streams` default.
pub const DEFAULT_BUF_CAPACITY: usize = 8 * 1024;

impl DecodeOptions {
    /// Options with no per-object limit.
    ///
    /// Appropriate for a client reading a server it chose. A server reading untrusted input
    /// should set [`max_obj_len`] instead.
    ///
    /// [`max_obj_len`]: DecodeOptions::max_obj_len
    pub fn new() -> Self {
        Self {
            max_obj_len: usize::MAX,
            buf_capacity: DEFAULT_BUF_CAPACITY,
        }
    }

    /// Set the maximum length of a single decoded object.
    pub fn max_obj_len(mut self, value: usize) -> Self {
        self.max_obj_len = value;
        self
    }

    /// Set the initial capacity of the read buffer.
    pub fn buf_capacity(mut self, value: usize) -> Self {
        self.buf_capacity = value;
        self
    }
}

impl Default for DecodeOptions {
    fn default() -> Self {
        Self::new()
    }
}
