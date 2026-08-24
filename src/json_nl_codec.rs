//! Decoding JSON Lines.
//!
//! The easy direction: `serde_json` escapes newlines inside strings, so a raw `\n` can only
//! ever be a record separator and a line split is safe.

use crate::error::{StreamError, StreamErrorKind};
use bytes::BytesMut;
use serde::Deserialize;
use std::marker::PhantomData;
use tokio_util::codec::{Decoder, LinesCodec, LinesCodecError};

/// A [`Decoder`] that yields one deserialised item per line.
#[derive(Debug)]
pub struct JsonNewLineCodec<T> {
    inner: LinesCodec,
    /// `fn() -> T` rather than `T`: a bare `PhantomData<T>` would make this codec `!Send`
    /// whenever `T` is, and the crates built on this one promise `Send` streams for item
    /// types that carry no such bound. The item type is produced, never held, so this is also
    /// the honest variance.
    _ph: PhantomData<fn() -> T>,
}

impl<T> JsonNewLineCodec<T> {
    /// A codec that rejects any single line longer than `max_length` bytes.
    pub fn new_with_max_length(max_length: usize) -> Self {
        Self {
            inner: LinesCodec::new_with_max_length(max_length),
            _ph: PhantomData,
        }
    }
}

/// Framing failures are told apart from deserialisation failures, so that a line which blew the
/// length limit reports as [`MaxLenReachedError`] rather than as a generic codec error.
///
/// [`MaxLenReachedError`]: StreamErrorKind::MaxLenReachedError
fn frame_error(err: LinesCodecError) -> StreamError {
    match err {
        LinesCodecError::MaxLineLengthExceeded => StreamError::new(
            StreamErrorKind::MaxLenReachedError,
            None,
            Some("Max line length reached".into()),
        ),
        LinesCodecError::Io(err) => StreamError::from(err),
    }
}

/// A line that fails to parse is yielded as an error **item**, not as the decoder's error:
/// the line framed correctly, so the decoder knows exactly where the next one starts and the
/// stream carries on. Returning the decoder's `Error` here would make one bad line silently
/// truncate the rest of the body, because `FramedRead` latches its error state.
fn parse<T>(line: &str) -> Option<Result<T, StreamError>>
where
    T: for<'de> Deserialize<'de>,
{
    Some(
        serde_json::from_str(line)
            .map_err(|err| StreamError::new(StreamErrorKind::CodecError, Some(Box::new(err)), None)),
    )
}

impl<T> Decoder for JsonNewLineCodec<T>
where
    T: for<'de> Deserialize<'de>,
{
    type Item = Result<T, StreamError>;
    type Error = StreamError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, StreamError> {
        match self.inner.decode(buf).map_err(frame_error)? {
            Some(line) => Ok(parse(&line)),
            None => Ok(None),
        }
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, StreamError> {
        match self.inner.decode_eof(buf).map_err(frame_error)? {
            Some(line) => Ok(parse(&line)),
            None => Ok(None),
        }
    }
}
