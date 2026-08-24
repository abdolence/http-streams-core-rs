//! Coalescing small encoded chunks into larger ones.
//!
//! Without this, a format whose items are small (JSON Lines of short objects, say) emits one
//! HTTP frame per item, which is dreadful wire efficiency. It matters on the sending side in
//! both directions: a client uploading a stream has exactly the same problem as a server
//! returning one.
//!
//! Core yields [`Bytes`]; wrapping them in whatever frame type the HTTP layer wants is the
//! binding crate's job.

use bytes::{Bytes, BytesMut};
use futures::stream::{Stream, StreamExt};

/// Coalesce every `count` items that are ready at the same time into one chunk.
///
/// Uses readiness rather than a fixed count, so a slow producer is never held back waiting for
/// a batch to fill.
pub fn buffer_ready_items<'b, S, E>(
    stream: S,
    count: usize,
) -> impl Stream<Item = Result<Bytes, E>> + Send + 'b
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'b,
    E: 'b,
{
    stream.ready_chunks(count).map(|chunks| {
        let mut buf = BytesMut::new();
        for chunk in chunks {
            buf.extend_from_slice(&chunk?);
        }
        Ok(buf.freeze())
    })
}

/// Coalesce output into chunks of at least `size` bytes.
///
/// The final chunk is whatever is left over, and may be shorter.
pub fn buffer_bytes<'b, S, E>(
    stream: S,
    size: usize,
) -> impl Stream<Item = Result<Bytes, E>> + Send + 'b
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'b,
    // Unlike the combinators that only observe, this one holds errors in its scan state, so
    // they must be `Send` for the resulting stream to be.
    E: Send + 'b,
{
    // A zero size would make the drain loop below spin forever, since `split_to(0)` never
    // shrinks the buffer. This is a public function, so the guard belongs here rather than in
    // every caller.
    let size = size.max(1);

    // An empty chunk appended to the end is the signal to flush whatever is still buffered.
    // `Bytes::new()` cannot occur otherwise, because empty encoder output is dropped upstream.
    let stream = stream.chain(futures::stream::once(futures::future::ready(Ok(
        Bytes::new(),
    ))));

    stream
        .scan(
            (BytesMut::with_capacity(size), false),
            move |(current_buffer, errored), maybe_bytes| {
                futures::future::ready(if *errored {
                    None
                } else {
                    match maybe_bytes {
                        // The flush marker. Emitting unconditionally would append an empty
                        // chunk whenever the input divided evenly into `size`, or was empty.
                        Ok(bytes) if bytes.is_empty() => {
                            if current_buffer.is_empty() {
                                Some(Vec::new())
                            } else {
                                Some(vec![Ok(current_buffer.split().freeze())])
                            }
                        }
                        Ok(bytes) => {
                            let mut chunks = Vec::new();
                            current_buffer.extend_from_slice(&bytes);
                            while current_buffer.len() >= size {
                                chunks.push(Ok(current_buffer.split_to(size).freeze()));
                            }
                            Some(chunks)
                        }
                        // Propagate the error instead of ending the stream: returning `None`
                        // here would make a failure indistinguishable from a clean EOF, and
                        // the receiver would silently accept a truncated body. Buffered bytes
                        // are dropped and the stream stops, so no data can follow the error
                        // via the trailing flush marker above.
                        Err(e) => {
                            *errored = true;
                            current_buffer.clear();
                            Some(vec![Err(e)])
                        }
                    }
                })
            },
        )
        .flat_map(futures::stream::iter)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{StreamError, StreamErrorKind};

    async fn collect(s: impl Stream<Item = Result<Bytes, StreamError>>) -> (Vec<Vec<u8>>, usize) {
        let items: Vec<_> = Box::pin(s).collect().await;
        let errors = items.iter().filter(|i| i.is_err()).count();
        let chunks = items.into_iter().flatten().map(|b| b.to_vec()).collect();
        (chunks, errors)
    }

    fn source(parts: Vec<&'static str>) -> impl Stream<Item = Result<Bytes, StreamError>> {
        futures::stream::iter(
            parts
                .into_iter()
                .map(|p| Ok(Bytes::from_static(p.as_bytes()))),
        )
    }

    #[tokio::test]
    async fn buffers_to_the_requested_size() {
        let (chunks, errors) = collect(buffer_bytes(source(vec!["ab", "cd", "ef", "g"]), 3)).await;
        assert_eq!(errors, 0);
        assert_eq!(
            chunks,
            vec![b"abc".to_vec(), b"def".to_vec(), b"g".to_vec()]
        );
    }

    #[tokio::test]
    async fn flushes_a_short_tail() {
        let (chunks, _) = collect(buffer_bytes(source(vec!["ab"]), 100)).await;
        assert_eq!(chunks, vec![b"ab".to_vec()]);
    }

    /// The regression: an error must not look like a clean end of body.
    #[tokio::test]
    async fn an_error_stops_the_stream_and_is_visible() {
        let parts = vec![
            Ok(Bytes::from_static(b"ab")),
            Err(StreamError::new(StreamErrorKind::CodecError, None, None)),
            Ok(Bytes::from_static(b"cd")),
        ];
        let (chunks, errors) = collect(buffer_bytes(futures::stream::iter(parts), 100)).await;

        assert_eq!(errors, 1, "the error must be yielded, not swallowed");
        assert!(
            chunks.is_empty(),
            "buffered bytes are dropped, so no data can follow the error"
        );
    }

    #[tokio::test]
    async fn ready_items_coalesce() {
        let (chunks, errors) =
            collect(buffer_ready_items(source(vec!["a", "b", "c", "d", "e"]), 2)).await;
        assert_eq!(errors, 0);
        assert_eq!(
            chunks.concat(),
            b"abcde".to_vec(),
            "coalescing must not change the bytes"
        );
        assert!(chunks.len() < 5, "chunks must actually be coalesced");
    }

    /// The flush marker used to be emitted unconditionally, appending a zero-length chunk
    /// whenever the input happened to divide evenly into `size`.
    #[tokio::test]
    async fn an_exact_multiple_emits_no_empty_tail() {
        let (chunks, errors) = collect(buffer_bytes(source(vec!["abc", "def"]), 3)).await;
        assert_eq!(errors, 0);
        assert_eq!(chunks, vec![b"abc".to_vec(), b"def".to_vec()]);
        assert!(
            chunks.iter().all(|c| !c.is_empty()),
            "no zero-length chunk may be emitted"
        );
    }

    #[tokio::test]
    async fn an_empty_source_emits_nothing() {
        let empty = futures::stream::iter(Vec::<Result<Bytes, StreamError>>::new());
        let (chunks, errors) = collect(buffer_bytes(empty, 8)).await;
        assert_eq!(errors, 0);
        assert!(chunks.is_empty(), "an empty body must produce no chunks");
    }

    /// A zero size would spin forever: `split_to(0)` never shrinks the buffer. These are public
    /// functions, so a caller can pass one.
    #[tokio::test]
    async fn a_zero_size_does_not_hang() {
        let buffered = buffer_bytes(source(vec!["ab", "cd"]), 0);
        let (chunks, errors) =
            tokio::time::timeout(std::time::Duration::from_secs(5), collect(buffered))
                .await
                .expect("a zero size must not loop forever");

        assert_eq!(errors, 0);
        assert_eq!(chunks.concat(), b"abcd".to_vec());
    }
}
