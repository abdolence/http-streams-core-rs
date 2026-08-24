//! Framing and round-trip tests for the binary formats, plus the text encoder.

use bytes::Bytes;
use futures::StreamExt;

use http_streams_core::format::{DecodeOptions, StreamFormatDecode, StreamFormatEncode};
use http_streams_core::{decode_stream, encode_stream, StreamError};

async fn encode<T, F>(format: &F, items: Vec<T>) -> Vec<u8>
where
    F: StreamFormatEncode<T>,
    F::Encoder: Send + 'static,
    T: Send + 'static,
{
    let source = futures::stream::iter(items.into_iter().map(Ok::<_, StreamError>));
    let mut out = Vec::new();
    let mut stream = Box::pin(encode_stream(source, format.encoder()));
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk.expect("encoding must not fail"));
    }
    out
}

async fn decode_chunked<T, F>(format: &F, bytes: &[u8], chunk_size: usize) -> Vec<T>
where
    F: StreamFormatDecode<T>,
    F::Framer: 'static,
    F::Parser: 'static,
    T: Send + 'static,
{
    let chunks: Vec<Result<Bytes, std::io::Error>> = bytes
        .chunks(chunk_size)
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .collect();
    let options = DecodeOptions::new();
    let framer = format.framer(&options);
    let parser = format.parser();
    let results: Vec<Result<T, StreamError>> = Box::pin(decode_stream(
        futures::stream::iter(chunks),
        framer,
        parser,
        &options,
    ))
    .collect()
    .await;
    results
        .into_iter()
        .map(|r| r.expect("decoding must not fail"))
        .collect()
}

#[cfg(feature = "protobuf")]
mod protobuf {
    use super::*;
    use http_streams_core::ProtobufStreamFormat;

    #[derive(Clone, PartialEq, prost::Message)]
    struct Record {
        #[prost(string, tag = "1")]
        name: String,
        #[prost(uint32, tag = "2")]
        id: u32,
    }

    fn records() -> Vec<Record> {
        vec![
            Record {
                name: "one".into(),
                id: 1,
            },
            Record {
                name: "two".into(),
                id: 2,
            },
        ]
    }

    /// Each frame is a LEB128 length followed by exactly that many bytes.
    #[tokio::test]
    async fn framing_is_length_prefixed() {
        let bytes = encode(&ProtobufStreamFormat::new(), records()).await;

        let mut cursor = 0usize;
        let mut frames = 0;
        while cursor < bytes.len() {
            let len = bytes[cursor] as usize;
            assert!(
                len < 0x80,
                "test records are small enough for a 1-byte varint"
            );
            cursor += 1 + len;
            frames += 1;
        }
        assert_eq!(cursor, bytes.len(), "frames must tile the body exactly");
        assert_eq!(frames, 2);
    }

    /// The interesting failure is a varint or message split across two reads.
    #[tokio::test]
    async fn round_trips_at_every_chunk_boundary() {
        let format = ProtobufStreamFormat::new();
        let bytes = encode(&format, records()).await;

        for chunk_size in 1..=bytes.len() {
            let decoded: Vec<Record> = decode_chunked(&format, &bytes, chunk_size).await;
            assert_eq!(decoded, records(), "failed at chunk size {chunk_size}");
        }
    }

    /// A message long enough to need a multi-byte varint length, so the varint itself gets
    /// split across chunk boundaries.
    #[tokio::test]
    async fn round_trips_a_multibyte_varint_length() {
        let format = ProtobufStreamFormat::new();
        let big = vec![Record {
            name: "x".repeat(500),
            id: 7,
        }];
        let bytes = encode(&format, big.clone()).await;
        assert!(
            bytes[0] >= 0x80,
            "length must need more than one varint byte"
        );

        for chunk_size in 1..=8 {
            let decoded: Vec<Record> = decode_chunked(&format, &bytes, chunk_size).await;
            assert_eq!(decoded, big, "failed at chunk size {chunk_size}");
        }
    }

    /// A message whose fields all hold their default values encodes to *zero bytes*, so its
    /// frame is a lone `0x00`. A codec using `0` to mean "no length read yet" would try to
    /// parse the next frame's length prefix as this message's body.
    #[tokio::test]
    async fn round_trips_zero_length_messages() {
        let format = ProtobufStreamFormat::new();
        let empty = Record {
            name: String::new(),
            id: 0,
        };
        let items = vec![
            empty.clone(),
            Record {
                name: "after".into(),
                id: 9,
            },
            empty.clone(),
        ];

        let bytes = encode(&format, items.clone()).await;
        assert_eq!(
            bytes[0], 0,
            "an all-default message must encode to an empty frame"
        );

        for chunk_size in 1..=bytes.len() {
            let decoded: Vec<Record> = decode_chunked(&format, &bytes, chunk_size).await;
            assert_eq!(decoded, items, "failed at chunk size {chunk_size}");
        }
    }

    /// The message-loss regression: two or more whole frames buffered when the body ends.
    #[tokio::test]
    async fn does_not_drop_frames_buffered_at_eof() {
        let format = ProtobufStreamFormat::new();
        let items: Vec<Record> = (0..10)
            .map(|i| Record {
                name: format!("item-{i}"),
                id: i,
            })
            .collect();

        let bytes = encode(&format, items.clone()).await;
        // One chunk, so every frame is buffered before EOF is seen.
        let decoded: Vec<Record> = decode_chunked(&format, &bytes, bytes.len()).await;

        assert_eq!(
            decoded, items,
            "no frame may be lost when the body arrives at once"
        );
    }

    #[tokio::test]
    async fn content_type_negotiation() {
        use http_streams_core::{ContentType, StreamFormat};

        let format = ProtobufStreamFormat::new();
        assert_eq!(
            format.default_content_type(),
            "application/x-protobuf-stream"
        );
        assert!(format.accepts_content_type(&ContentType::parse("application/x-protobuf-stream")));
        assert!(format.accepts_content_type(&ContentType::parse("application/protobuf")));
        // Accepting this would mean decoding any unlabelled binary body as protobuf frames.
        assert!(!format.accepts_content_type(&ContentType::parse("application/octet-stream")));
    }
}

#[cfg(feature = "arrow")]
mod arrow_ipc {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use http_streams_core::ArrowRecordBatchIpcStreamFormat;
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn batch(ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
        let id: ArrayRef = Arc::new(Int32Array::from(ids));
        let name: ArrayRef = Arc::new(StringArray::from(names));
        RecordBatch::try_new(schema(), vec![id, name]).unwrap()
    }

    fn batches() -> Vec<RecordBatch> {
        vec![
            batch(vec![1, 2], vec!["one", "two"]),
            batch(vec![3], vec!["three"]),
        ]
    }

    /// The stream must end with the continuation marker and a zero length.
    #[tokio::test]
    async fn ends_with_the_continuation_marker() {
        let format = ArrowRecordBatchIpcStreamFormat::new(schema());
        let bytes = encode(&format, batches()).await;

        assert_eq!(
            &bytes[bytes.len() - 8..],
            &[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0],
            "an Arrow IPC stream must be terminated"
        );
    }

    #[tokio::test]
    async fn round_trips_whole() {
        let format = ArrowRecordBatchIpcStreamFormat::new(schema());
        let bytes = encode(&format, batches()).await;
        let decoded: Vec<RecordBatch> = decode_chunked(&format, &bytes, bytes.len()).await;
        assert_eq!(decoded, batches());
    }

    /// Small chunk sizes only: Arrow bodies are large, and splitting at every one of several
    /// thousand offsets would dominate the suite's runtime without testing anything new.
    #[tokio::test]
    async fn round_trips_across_chunk_boundaries() {
        let format = ArrowRecordBatchIpcStreamFormat::new(schema());
        let bytes = encode(&format, batches()).await;

        for chunk_size in [1, 2, 3, 5, 7, 8, 13, 64, 127, 256] {
            if chunk_size > bytes.len() {
                continue;
            }
            let decoded: Vec<RecordBatch> = decode_chunked(&format, &bytes, chunk_size).await;
            assert_eq!(decoded, batches(), "failed at chunk size {chunk_size}");
        }
    }

    #[tokio::test]
    async fn content_type_negotiation() {
        use http_streams_core::{ContentType, StreamFormat};

        let format = ArrowRecordBatchIpcStreamFormat::new(schema());
        assert_eq!(
            format.default_content_type(),
            "application/vnd.apache.arrow.stream"
        );
        assert!(
            format.accepts_content_type(&ContentType::parse("application/vnd.apache.arrow.stream"))
        );
        assert!(!format.accepts_content_type(&ContentType::parse("application/json")));
    }
}

#[cfg(feature = "text")]
mod text {
    use super::*;
    use http_streams_core::TextStreamFormat;

    /// The framing that makes decoding impossible: no delimiter, so these two inputs are
    /// indistinguishable on the wire. This test documents that, rather than working around it.
    #[tokio::test]
    async fn writes_raw_bytes_with_no_delimiter() {
        let a = encode(
            &TextStreamFormat::new(),
            vec!["ab".to_string(), "c".to_string()],
        )
        .await;
        let b = encode(
            &TextStreamFormat::new(),
            vec!["a".to_string(), "bc".to_string()],
        )
        .await;

        assert_eq!(String::from_utf8(a.clone()).unwrap(), "abc");
        assert_eq!(
            a, b,
            "different item boundaries produce identical bytes, which is why there is no decoder"
        );
    }
}
