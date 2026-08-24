//! The error type shared by every streaming format, in both directions.

use std::fmt;

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

/// The error that may occur while encoding or decoding a streamed HTTP body.
pub struct StreamError {
    kind: StreamErrorKind,
    source: Option<BoxedError>,
    message: Option<String>,
}

impl StreamError {
    /// Create a new instance of an error.
    ///
    /// Public so that formats implemented outside this crate can report failures the same way
    /// the built-in ones do.
    pub fn new(kind: StreamErrorKind, source: Option<BoxedError>, message: Option<String>) -> Self {
        Self {
            kind,
            source,
            message,
        }
    }

    /// The kind of error that occurred.
    pub fn kind(&self) -> StreamErrorKind {
        self.kind
    }

    /// The actual error that occurred.
    pub fn source(&self) -> Option<&BoxedError> {
        self.source.as_ref()
    }

    /// The message associated with the error.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Takes the error apart, giving up ownership of its cause.
    ///
    /// Exists so a binding crate can wrap its own error type to pass it through this crate's
    /// pipeline and then recover the *original* on the way out, by downcasting the returned
    /// source. Without that, a round trip would leave the caller's error nested inside a
    /// `StreamError` inside their own error type, changing what their callbacks see.
    pub fn into_parts(self) -> (StreamErrorKind, Option<BoxedError>, Option<String>) {
        (self.kind, self.source, self.message)
    }

    /// A codec error carrying `source` as its cause.
    pub fn codec(source: impl Into<BoxedError>) -> Self {
        Self::new(StreamErrorKind::CodecError, Some(source.into()), None)
    }

    /// An I/O error carrying `source` as its cause.
    pub fn io(source: impl Into<BoxedError>) -> Self {
        Self::new(StreamErrorKind::InputOutputError, Some(source.into()), None)
    }
}

/// The kind of error that occurred.
///
/// Variant names are inherited from `reqwest-streams`, whose public `StreamBodyKind` is a
/// renamed re-export of this type: renaming them would break downstream `match` arms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamErrorKind {
    /// An error occured while encoding or decoding a frame or format.
    CodecError,

    /// An error occured while reading or writing the stream.
    InputOutputError,

    /// The maximum length of a single object was exceeded.
    MaxLenReachedError,

    /// The maximum length of the whole body was exceeded.
    ///
    /// Only reachable on the receiving side, where the peer is not trusted.
    MaxBodyLenReachedError,
}

impl StreamErrorKind {
    /// A short, stable name for this kind, reported as the `error_kind` tracing field so that
    /// errors can be aggregated without parsing their [`Display`] output.
    ///
    /// [`Display`]: fmt::Display
    pub fn as_str(&self) -> &'static str {
        match self {
            StreamErrorKind::CodecError => "codec",
            StreamErrorKind::InputOutputError => "io",
            StreamErrorKind::MaxLenReachedError => "max_len",
            StreamErrorKind::MaxBodyLenReachedError => "max_body_len",
        }
    }
}

impl fmt::Debug for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut builder = f.debug_struct("StreamError");

        builder.field("kind", &self.kind);

        if let Some(ref source) = self.source {
            builder.field("source", source);
        }

        if let Some(ref message) = self.message {
            builder.field("message", message);
        }

        builder.finish()
    }
}

impl fmt::Display for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.kind {
            StreamErrorKind::CodecError => f.write_str("Frame/codec error")?,
            StreamErrorKind::InputOutputError => f.write_str("I/O error")?,
            StreamErrorKind::MaxLenReachedError => f.write_str("Max object length reached")?,
            StreamErrorKind::MaxBodyLenReachedError => f.write_str("Max body length reached")?,
        };

        if let Some(message) = &self.message {
            write!(f, ": {}", message)?;
        }

        if let Some(e) = &self.source {
            write!(f, ": {}", e)?;
        }

        Ok(())
    }
}

impl std::error::Error for StreamError {}

impl From<std::io::Error> for StreamError {
    fn from(err: std::io::Error) -> Self {
        StreamError::new(StreamErrorKind::InputOutputError, Some(Box::new(err)), None)
    }
}
