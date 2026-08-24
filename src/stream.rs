//! Driving an [`ItemEncoder`] or a [`Decoder`] over a stream.
//!
//! These two functions are the API boundary that keeps `tokio-util` an implementation detail:
//! a dependent crate hands in a byte stream and gets items back (or vice versa) without ever
//! naming `FramedRead`, `StreamReader` or [`Decoder`].
//!
//! [`Decoder`]: tokio_util::codec::Decoder

use crate::error::StreamError;
use crate::format::{DecodeOptions, FrameParser, ItemEncoder};
use bytes::{Bytes, BytesMut};
use futures::stream::{Stream, StreamExt};
use tokio_util::codec::{Decoder, FramedRead};
use tokio_util::io::StreamReader;

/// Where an encode run has got to.
///
/// The epilogue is reachable only from a clean end of the item stream. After an error the run
/// jumps straight to `Done`, because a body that stopped early must not be closed with a
/// well-formed terminator: a truncated JSON array ending in `]` would parse as complete and
/// silently lose items.
enum Phase {
    Prologue,
    Items,
    Epilogue,
    Done,
}

struct EncodeState<S, ENC> {
    inner: S,
    encoder: ENC,
    index: u64,
    phase: Phase,
}

/// Encode a stream of items into a stream of body chunks.
///
/// Empty chunks are swallowed rather than yielded, so a format with no prologue or epilogue
/// does not emit zero-length frames.
pub fn encode_stream<'b, S, T, ENC>(
    stream: S,
    encoder: ENC,
) -> impl Stream<Item = Result<Bytes, StreamError>> + Send + 'b
where
    S: Stream<Item = Result<T, StreamError>> + Send + Unpin + 'b,
    T: Send + 'b,
    ENC: ItemEncoder<T> + Send + 'b,
{
    let state = EncodeState {
        inner: stream,
        encoder,
        index: 0,
        phase: Phase::Prologue,
    };

    futures::stream::unfold(state, |mut st| async move {
        loop {
            match st.phase {
                Phase::Prologue => {
                    let mut buf = BytesMut::new();
                    match st.encoder.prologue(&mut buf) {
                        Err(e) => {
                            st.phase = Phase::Done;
                            return Some((Err(e), st));
                        }
                        Ok(()) => {
                            st.phase = Phase::Items;
                            if !buf.is_empty() {
                                return Some((Ok(buf.freeze()), st));
                            }
                        }
                    }
                }
                Phase::Items => match st.inner.next().await {
                    Some(Ok(item)) => {
                        let mut buf = BytesMut::new();
                        let index = st.index;
                        match st.encoder.encode(&item, index, &mut buf) {
                            Err(e) => {
                                st.phase = Phase::Done;
                                return Some((Err(e), st));
                            }
                            Ok(()) => {
                                st.index += 1;
                                if !buf.is_empty() {
                                    return Some((Ok(buf.freeze()), st));
                                }
                            }
                        }
                    }
                    // The source's own error is forwarded verbatim, so that every error in the
                    // pipeline surfaces at exactly one place downstream.
                    Some(Err(e)) => {
                        st.phase = Phase::Done;
                        return Some((Err(e), st));
                    }
                    None => st.phase = Phase::Epilogue,
                },
                Phase::Epilogue => {
                    let mut buf = BytesMut::new();
                    let result = st.encoder.epilogue(&mut buf);
                    st.phase = Phase::Done;
                    match result {
                        Err(e) => return Some((Err(e), st)),
                        Ok(()) => {
                            if !buf.is_empty() {
                                return Some((Ok(buf.freeze()), st));
                            }
                        }
                    }
                }
                Phase::Done => return None,
            }
        }
    })
}

/// Decode a stream of body chunks into a stream of items.
///
/// The byte stream's error type is [`std::io::Error`] because that is what both hyper and
/// reqwest body streams already produce, and what [`StreamReader`] requires.
///
/// Framing errors and per-record deserialisation errors are flattened into one stream here,
/// but the difference stays observable: after a framing error the stream ends, after a
/// deserialisation error it continues with the next record.
///
/// Note what is *not* required: `T: 'b`. Neither the framer nor the parser mentions `T` in its
/// own type for formats that can separate the two, so callers are not forced to add an
/// outlives bound to their public signatures.
pub fn decode_stream<'b, S, F, D, P, T>(
    stream: S,
    framer: D,
    parser: P,
    options: &DecodeOptions,
) -> impl Stream<Item = Result<T, StreamError>> + Send + 'b
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Send + 'b,
    D: Decoder<Item = F, Error = StreamError> + Send + 'b,
    P: FrameParser<F, T> + Send + 'b,
    F: 'b,
{
    let reader = StreamReader::new(Box::pin(stream));
    FramedRead::with_capacity(reader, framer, options.buf_capacity).map(
        move |framed| match framed {
            Ok(frame) => parser.parse(frame),
            Err(err) => Err(err),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::StreamErrorKind;

    /// A format that brackets its items, so prologue/epilogue/index are all exercised.
    struct Bracketed {
        fail_at: Option<u64>,
    }

    impl ItemEncoder<u32> for Bracketed {
        fn prologue(&mut self, buf: &mut BytesMut) -> Result<(), StreamError> {
            buf.extend_from_slice(b"[");
            Ok(())
        }

        fn encode(
            &mut self,
            item: &u32,
            index: u64,
            buf: &mut BytesMut,
        ) -> Result<(), StreamError> {
            if self.fail_at == Some(index) {
                return Err(StreamError::new(
                    StreamErrorKind::CodecError,
                    None,
                    Some("boom".into()),
                ));
            }
            if index != 0 {
                buf.extend_from_slice(b",");
            }
            buf.extend_from_slice(item.to_string().as_bytes());
            Ok(())
        }

        fn epilogue(&mut self, buf: &mut BytesMut) -> Result<(), StreamError> {
            buf.extend_from_slice(b"]");
            Ok(())
        }
    }

    async fn collect(s: impl Stream<Item = Result<Bytes, StreamError>>) -> (Vec<u8>, usize) {
        let items: Vec<_> = Box::pin(s).collect().await;
        let errors = items.iter().filter(|i| i.is_err()).count();
        let mut out = Vec::new();
        for i in items.into_iter().flatten() {
            out.extend_from_slice(&i);
        }
        (out, errors)
    }

    #[tokio::test]
    async fn encodes_prologue_items_and_epilogue() {
        let source = futures::stream::iter(vec![Ok(1u32), Ok(2), Ok(3)]);
        let (bytes, errors) = collect(encode_stream(source, Bracketed { fail_at: None })).await;
        assert_eq!(String::from_utf8(bytes).unwrap(), "[1,2,3]");
        assert_eq!(errors, 0);
    }

    #[tokio::test]
    async fn empty_source_still_brackets() {
        let source = futures::stream::iter(Vec::<Result<u32, StreamError>>::new());
        let (bytes, _) = collect(encode_stream(source, Bracketed { fail_at: None })).await;
        assert_eq!(String::from_utf8(bytes).unwrap(), "[]");
    }

    /// The closing bracket must NOT appear: a truncated body that looks complete is worse than
    /// one that is visibly truncated.
    #[tokio::test]
    async fn encoder_error_suppresses_the_epilogue() {
        let source = futures::stream::iter(vec![Ok(1u32), Ok(2), Ok(3)]);
        let (bytes, errors) = collect(encode_stream(source, Bracketed { fail_at: Some(1) })).await;
        assert_eq!(String::from_utf8(bytes).unwrap(), "[1");
        assert_eq!(errors, 1);
    }

    #[tokio::test]
    async fn source_error_is_forwarded_and_suppresses_the_epilogue() {
        let source = futures::stream::iter(vec![
            Ok(1u32),
            Err(StreamError::new(
                StreamErrorKind::InputOutputError,
                None,
                None,
            )),
        ]);
        let (bytes, errors) = collect(encode_stream(source, Bracketed { fail_at: None })).await;
        assert_eq!(String::from_utf8(bytes).unwrap(), "[1");
        assert_eq!(errors, 1);
    }
}
