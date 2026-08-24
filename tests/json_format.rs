#![cfg(feature = "json")]

//! Byte-level framing and round-trip tests for the JSON formats.
//!
//! The expected byte strings here are the contract with `axum-streams`, whose published
//! encoder produced exactly these sequences before the extraction. They are written as
//! literals rather than compared against that crate because a dev-dependency on it would
//! become a dependency cycle once it depends on this crate. The direct comparison against the
//! published `axum-streams` output lives in `reqwest-streams`, which already dev-depends on it.

use bytes::Bytes;
use futures::StreamExt;
use http_streams_core::format::{DecodeOptions, StreamFormatDecode, StreamFormatEncode};
use http_streams_core::{
    decode_stream, encode_stream, JsonArrayStreamFormat, JsonNewLineStreamFormat, StreamError,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Item {
    id: u32,
    name: String,
}

fn items() -> Vec<Item> {
    vec![
        Item {
            id: 1,
            name: "one".into(),
        },
        Item {
            id: 2,
            name: "two".into(),
        },
    ]
}

#[derive(Serialize)]
struct Meta {
    total: u32,
}

#[derive(Serialize)]
struct Empty {}

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

/// Decode `bytes` fed in chunks of exactly `chunk_size`, so that framing is exercised across
/// buffer boundaries rather than only on whole-message reads.
async fn decode_chunked<T, F>(
    format: &F,
    bytes: &[u8],
    chunk_size: usize,
) -> Vec<Result<T, StreamError>>
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
    let source = futures::stream::iter(chunks);
    let framer = format.framer(&DecodeOptions::new());
    let parser = format.parser();
    Box::pin(decode_stream(source, framer, parser, &DecodeOptions::new()))
        .collect()
        .await
}

fn unwrap_all<T>(results: Vec<Result<T, StreamError>>) -> Vec<T> {
    results
        .into_iter()
        .map(|r| r.expect("decoding must not fail"))
        .collect()
}

#[tokio::test]
async fn json_array_framing_is_exact() {
    let bytes = encode(&JsonArrayStreamFormat::new(), items()).await;
    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        r#"[{"id":1,"name":"one"},{"id":2,"name":"two"}]"#
    );
}

#[tokio::test]
async fn json_array_of_nothing_is_still_an_array() {
    let bytes = encode(&JsonArrayStreamFormat::new(), Vec::<Item>::new()).await;
    assert_eq!(String::from_utf8(bytes).unwrap(), "[]");
}

#[tokio::test]
async fn json_array_envelope_framing_is_exact() {
    let format = JsonArrayStreamFormat::with_envelope(Meta { total: 2 }, "items");
    let bytes = encode(&format, items()).await;
    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        r#"{"total":2,"items":[{"id":1,"name":"one"},{"id":2,"name":"two"}]}"#
    );
}

/// An envelope with no fields of its own must not gain a leading comma.
#[tokio::test]
async fn json_array_empty_envelope_omits_the_separator() {
    let format = JsonArrayStreamFormat::with_envelope(Empty {}, "items");
    let bytes = encode(&format, items()).await;
    assert!(
        String::from_utf8(bytes)
            .unwrap()
            .starts_with(r#"{"items":["#),
        "an empty envelope must not emit a separator before the array field"
    );
}

#[tokio::test]
async fn json_nl_framing_is_exact() {
    let bytes = encode(&JsonNewLineStreamFormat::new(), items()).await;
    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        "{\"id\":1,\"name\":\"one\"}\n{\"id\":2,\"name\":\"two\"}\n"
    );
}

/// The test that actually catches decoder bugs: every possible split point.
#[tokio::test]
async fn json_array_round_trips_at_every_chunk_boundary() {
    let format = JsonArrayStreamFormat::new();
    let bytes = encode(&format, items()).await;

    for chunk_size in 1..=bytes.len() {
        let decoded: Vec<Item> = unwrap_all(decode_chunked(&format, &bytes, chunk_size).await);
        assert_eq!(decoded, items(), "failed at chunk size {chunk_size}");
    }
}

#[tokio::test]
async fn json_nl_round_trips_at_every_chunk_boundary() {
    let format = JsonNewLineStreamFormat::new();
    let bytes = encode(&format, items()).await;

    for chunk_size in 1..=bytes.len() {
        let decoded: Vec<Item> = unwrap_all(decode_chunked(&format, &bytes, chunk_size).await);
        assert_eq!(decoded, items(), "failed at chunk size {chunk_size}");
    }
}

/// Multi-byte UTF-8 and escapes are where a byte-level scanner goes wrong.
#[tokio::test]
async fn json_array_handles_escapes_and_multibyte_at_every_boundary() {
    let tricky = vec![
        Item {
            id: 1,
            name: "quote\" and \\ backslash".into(),
        },
        Item {
            id: 2,
            name: "日本語とemoji🎉".into(),
        },
        Item {
            id: 3,
            name: "newline\nand\ttab".into(),
        },
    ];
    let format = JsonArrayStreamFormat::new();
    let bytes = encode(&format, tricky.clone()).await;

    for chunk_size in 1..=bytes.len() {
        let decoded: Vec<Item> = unwrap_all(decode_chunked(&format, &bytes, chunk_size).await);
        assert_eq!(decoded, tricky, "failed at chunk size {chunk_size}");
    }
}

#[tokio::test]
async fn json_array_decodes_primitives_and_nested_arrays() {
    let format = JsonArrayStreamFormat::new();

    let numbers = vec![1u32, 2, 3];
    let bytes = encode(&format, numbers.clone()).await;
    assert_eq!(String::from_utf8(bytes.clone()).unwrap(), "[1,2,3]");
    for chunk_size in 1..=bytes.len() {
        let decoded: Vec<u32> = unwrap_all(decode_chunked(&format, &bytes, chunk_size).await);
        assert_eq!(decoded, numbers, "failed at chunk size {chunk_size}");
    }

    let nested = vec![vec![1u32, 2], vec![3]];
    let bytes = encode(&format, nested.clone()).await;
    for chunk_size in 1..=bytes.len() {
        let decoded: Vec<Vec<u32>> = unwrap_all(decode_chunked(&format, &bytes, chunk_size).await);
        assert_eq!(decoded, nested, "failed at chunk size {chunk_size}");
    }
}

/// JSON-NL keeps going after a bad line; the array format cannot, because `FramedRead` latches
/// its error state. Both behaviours are load-bearing and documented.
#[tokio::test]
async fn json_nl_survives_a_malformed_line() {
    let format = JsonNewLineStreamFormat::new();
    let body = b"{\"id\":1,\"name\":\"one\"}\nnot json\n{\"id\":2,\"name\":\"two\"}\n";
    let results: Vec<Result<Item, StreamError>> = decode_chunked(&format, body, body.len()).await;

    assert_eq!(results.len(), 3);
    assert!(results[0].is_ok());
    assert!(results[1].is_err());
    assert!(results[2].is_ok(), "a bad line must not end the stream");
}

#[tokio::test]
async fn content_type_negotiation() {
    use http_streams_core::{ContentType, StreamFormat};

    let array = JsonArrayStreamFormat::new();
    assert_eq!(array.default_content_type(), "application/json");
    assert!(array.accepts_content_type(&ContentType::parse("application/json")));
    assert!(array.accepts_content_type(&ContentType::parse("application/json; charset=utf-8")));
    assert!(array.accepts_content_type(&ContentType::parse("APPLICATION/JSON")));
    assert!(array.accepts_content_type(&ContentType::parse("application/cloudevents+json")));
    assert!(!array.accepts_content_type(&ContentType::parse("text/json")));

    let nl = JsonNewLineStreamFormat::new();
    assert_eq!(nl.default_content_type(), "application/jsonstream");
    assert!(nl.accepts_content_type(&ContentType::parse("application/jsonstream")));
    assert!(nl.accepts_content_type(&ContentType::parse("application/x-ndjson")));
    // RFC 7464 uses 0x1E separators, not newlines: accepting it would mis-decode.
    assert!(!nl.accepts_content_type(&ContentType::parse("application/json-seq")));
}

/// The mirror of `json_nl_survives_a_malformed_line`: for a JSON array a bad element IS
/// terminal, because the cursor tracks nesting across elements and cannot resynchronise.
#[tokio::test]
async fn json_array_stops_at_a_malformed_element() {
    let format = JsonArrayStreamFormat::new();
    let body = br#"[{"id":1,"name":"one"},{"id":"not a number"},{"id":3,"name":"three"}]"#;
    let results: Vec<Result<Item, StreamError>> = decode_chunked(&format, body, body.len()).await;

    assert!(results[0].is_ok());
    assert!(results[1].is_err());
    assert_eq!(
        results.len(),
        2,
        "a JSON array cannot resynchronise, so the stream must end at the first bad element"
    );
}

/// A stray `}` at the top level used to underflow the nesting counter: a panic in debug, and
/// in release a wrap to `usize::MAX` that silently corrupted framing for the rest of the body.
/// Malformed input is untrusted input, so this has to be an error rather than either.
#[tokio::test]
async fn a_stray_closing_brace_is_an_error_not_a_panic() {
    let format = JsonArrayStreamFormat::new();
    let results: Vec<Result<u32, StreamError>> = decode_chunked(&format, b"[1, }, 2]", 9).await;

    assert!(
        results.iter().any(|r| r.is_err()),
        "a stray closing brace must be reported: {results:?}"
    );
}

/// The same byte arriving on its own, so the guard is exercised at a chunk boundary too.
#[tokio::test]
async fn a_stray_closing_brace_is_an_error_at_every_boundary() {
    let format = JsonArrayStreamFormat::new();
    let body = b"[1, }, 2]";

    for chunk_size in 1..=body.len() {
        let results: Vec<Result<u32, StreamError>> =
            decode_chunked(&format, body, chunk_size).await;
        assert!(
            results.iter().any(|r| r.is_err()),
            "failed at chunk size {chunk_size}"
        );
    }
}
