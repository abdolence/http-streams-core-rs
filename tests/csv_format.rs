#![cfg(feature = "csv")]

//! Framing and round-trip tests for CSV.

use bytes::Bytes;
use futures::StreamExt;
use http_streams_core::format::{DecodeOptions, StreamFormatDecode, StreamFormatEncode};
use http_streams_core::{decode_stream, encode_stream, CsvStreamFormat, StreamError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Row {
    id: u32,
    name: String,
}

fn rows() -> Vec<Row> {
    vec![
        Row {
            id: 1,
            name: "one".into(),
        },
        Row {
            id: 2,
            name: "two".into(),
        },
    ]
}

async fn encode(format: &CsvStreamFormat, rows: Vec<Row>) -> Vec<u8> {
    let source = futures::stream::iter(rows.into_iter().map(Ok::<_, StreamError>));
    let mut out = Vec::new();
    let mut stream = Box::pin(encode_stream(
        source,
        StreamFormatEncode::<Row>::encoder(format),
    ));
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk.expect("encoding must not fail"));
    }
    out
}

async fn decode_chunked(
    format: &CsvStreamFormat,
    bytes: &[u8],
    chunk_size: usize,
) -> Vec<Result<Row, StreamError>> {
    let chunks: Vec<Result<Bytes, std::io::Error>> = bytes
        .chunks(chunk_size)
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .collect();
    let framer = StreamFormatDecode::<Row>::framer(format, &DecodeOptions::new());
    let parser = StreamFormatDecode::<Row>::parser(format);
    Box::pin(decode_stream(
        futures::stream::iter(chunks),
        framer,
        parser,
        &DecodeOptions::new(),
    ))
    .collect()
    .await
}

#[tokio::test]
async fn header_is_written_once_then_rows() {
    let bytes = encode(&CsvStreamFormat::new(true, b','), rows()).await;
    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        "id,name\n1,one\n2,two\n",
        "the header must appear exactly once, before the first row"
    );
}

#[tokio::test]
async fn headerless_writes_only_rows() {
    let bytes = encode(&CsvStreamFormat::new(false, b','), rows()).await;
    assert_eq!(String::from_utf8(bytes).unwrap(), "1,one\n2,two\n");
}

#[tokio::test]
async fn custom_delimiter_is_honoured() {
    let bytes = encode(&CsvStreamFormat::new(true, b';'), rows()).await;
    assert_eq!(String::from_utf8(bytes).unwrap(), "id;name\n1;one\n2;two\n");
}

#[tokio::test]
async fn round_trips_with_header_at_every_chunk_boundary() {
    let format = CsvStreamFormat::new(true, b',');
    let bytes = encode(&format, rows()).await;

    for chunk_size in 1..=bytes.len() {
        let decoded: Vec<Row> = decode_chunked(&format, &bytes, chunk_size)
            .await
            .into_iter()
            .map(|r| r.expect("decoding must not fail"))
            .collect();
        assert_eq!(decoded, rows(), "failed at chunk size {chunk_size}");
    }
}

#[tokio::test]
async fn round_trips_without_header_at_every_chunk_boundary() {
    let format = CsvStreamFormat::new(false, b',');
    let bytes = encode(&format, rows()).await;

    for chunk_size in 1..=bytes.len() {
        let decoded: Vec<Row> = decode_chunked(&format, &bytes, chunk_size)
            .await
            .into_iter()
            .map(|r| r.expect("decoding must not fail"))
            .collect();
        assert_eq!(decoded, rows(), "failed at chunk size {chunk_size}");
    }
}

#[tokio::test]
async fn round_trips_with_a_custom_delimiter() {
    let format = CsvStreamFormat::new(true, b';');
    let bytes = encode(&format, rows()).await;
    let decoded: Vec<Row> = decode_chunked(&format, &bytes, bytes.len())
        .await
        .into_iter()
        .map(|r| r.expect("decoding must not fail"))
        .collect();
    assert_eq!(decoded, rows());
}

/// A bad row is not terminal: rows are framed by line, so the decoder knows exactly where the
/// next one starts.
#[tokio::test]
async fn a_malformed_row_does_not_end_the_stream() {
    let format = CsvStreamFormat::new(true, b',');
    let body = b"id,name\n1,one\nnot-a-number,two\n3,three\n";
    let results = decode_chunked(&format, body, body.len()).await;

    assert_eq!(results.len(), 3);
    assert!(results[0].is_ok());
    assert!(results[1].is_err());
    assert!(results[2].is_ok(), "a bad row must not end the stream");
}

/// The regression this guards is subtle: skipping the first frame unconditionally would
/// swallow a header line that failed to frame, and the stream would report itself as having
/// completed cleanly while having produced nothing.
#[tokio::test]
async fn a_header_that_fails_to_frame_is_reported() {
    let format = CsvStreamFormat::new(true, b',');
    let long_header = "a".repeat(64);
    let body = format!("{long_header}\n1,one\n");

    let chunks: Vec<Result<Bytes, std::io::Error>> =
        vec![Ok(Bytes::copy_from_slice(body.as_bytes()))];
    let options = DecodeOptions::new().max_obj_len(8);
    let framer = StreamFormatDecode::<Row>::framer(&format, &options);
    let parser = StreamFormatDecode::<Row>::parser(&format);
    let results: Vec<Result<Row, StreamError>> = Box::pin(decode_stream(
        futures::stream::iter(chunks),
        framer,
        parser,
        &options,
    ))
    .collect()
    .await;

    assert!(
        results.iter().any(|r| r.is_err()),
        "the header's framing error must be reported, not skipped"
    );
}

#[tokio::test]
async fn content_type_negotiation() {
    use http_streams_core::{ContentType, StreamFormat};

    let format = CsvStreamFormat::default();
    assert_eq!(format.default_content_type(), "text/csv");
    assert!(format.accepts_content_type(&ContentType::parse("text/csv")));
    assert!(format.accepts_content_type(&ContentType::parse("text/csv; charset=utf-8")));
    assert!(format.accepts_content_type(&ContentType::parse("application/csv")));
    assert!(!format.accepts_content_type(&ContentType::parse("application/json")));
}

/// A CSV field may contain a newline when quoted, and this crate's own encoder emits exactly
/// that. Line-based framing used to truncate such a field and yield the remainder as a bogus
/// record — and the truncated row reported *no error at all*, so the corruption was silent.
#[tokio::test]
async fn a_quoted_newline_survives_the_round_trip() {
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Note {
        id: u32,
        note: String,
    }

    let notes = vec![
        Note {
            id: 1,
            note: "line one\nline two".into(),
        },
        Note {
            id: 2,
            note: "plain".into(),
        },
    ];
    let format = CsvStreamFormat::new(true, b',');

    let source = futures::stream::iter(notes.clone().into_iter().map(Ok::<_, StreamError>));
    let mut encoded = Vec::new();
    let mut stream = Box::pin(encode_stream(
        source,
        StreamFormatEncode::<Note>::encoder(&format),
    ));
    while let Some(chunk) = stream.next().await {
        encoded.extend_from_slice(&chunk.expect("encoding must not fail"));
    }

    let options = DecodeOptions::new();
    let framer = StreamFormatDecode::<Note>::framer(&format, &options);
    let parser = StreamFormatDecode::<Note>::parser(&format);
    let decoded: Vec<Result<Note, StreamError>> = Box::pin(decode_stream(
        futures::stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(encoded))]),
        framer,
        parser,
        &options,
    ))
    .collect()
    .await;

    let items: Vec<Note> = decoded
        .into_iter()
        .map(|r| r.expect("no record may fail"))
        .collect();
    assert_eq!(items, notes);
}

/// The same hazard one level down: the embedded newline falls on a chunk boundary.
#[tokio::test]
async fn a_quoted_newline_survives_every_chunk_boundary() {
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Note {
        id: u32,
        note: String,
    }

    let notes = vec![
        Note {
            id: 1,
            note: "a\nb".into(),
        },
        Note {
            id: 2,
            note: "c".into(),
        },
    ];
    let format = CsvStreamFormat::new(true, b',');

    let source = futures::stream::iter(notes.clone().into_iter().map(Ok::<_, StreamError>));
    let mut encoded = Vec::new();
    let mut stream = Box::pin(encode_stream(
        source,
        StreamFormatEncode::<Note>::encoder(&format),
    ));
    while let Some(chunk) = stream.next().await {
        encoded.extend_from_slice(&chunk.expect("encoding must not fail"));
    }

    for chunk_size in 1..=encoded.len() {
        let chunks: Vec<Result<Bytes, std::io::Error>> = encoded
            .chunks(chunk_size)
            .map(|c| Ok(Bytes::copy_from_slice(c)))
            .collect();
        let options = DecodeOptions::new();
        let framer = StreamFormatDecode::<Note>::framer(&format, &options);
        let parser = StreamFormatDecode::<Note>::parser(&format);
        let decoded: Vec<Note> = Box::pin(decode_stream(
            futures::stream::iter(chunks),
            framer,
            parser,
            &options,
        ))
        .collect::<Vec<Result<Note, StreamError>>>()
        .await
        .into_iter()
        .map(|r| r.expect("no record may fail"))
        .collect();
        assert_eq!(decoded, notes, "failed at chunk size {chunk_size}");
    }
}

/// The encoder escapes a quote by doubling it, not with a backslash, so the framer must only
/// honour the escape character when doubling is off. Enabling both — as the previous
/// line-based decoder did — makes a backslash inside a *quoted* field disappear, silently.
#[tokio::test]
async fn a_backslash_inside_a_quoted_field_survives() {
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Path {
        id: u32,
        path: String,
    }

    let paths = vec![
        Path {
            id: 1,
            path: r"C:\Users\abdulla".into(),
        },
        // Quoted, because it contains a quote — which is what exposes the escape handling.
        Path {
            id: 2,
            path: r#"quote" and \ both"#.into(),
        },
    ];
    let format = CsvStreamFormat::new(true, b',');

    let source = futures::stream::iter(paths.clone().into_iter().map(Ok::<_, StreamError>));
    let mut encoded = Vec::new();
    let mut stream = Box::pin(encode_stream(
        source,
        StreamFormatEncode::<Path>::encoder(&format),
    ));
    while let Some(chunk) = stream.next().await {
        encoded.extend_from_slice(&chunk.expect("encoding must not fail"));
    }

    let options = DecodeOptions::new();
    let decoded: Vec<Path> = Box::pin(decode_stream(
        futures::stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(encoded))]),
        StreamFormatDecode::<Path>::framer(&format, &options),
        StreamFormatDecode::<Path>::parser(&format),
        &options,
    ))
    .collect::<Vec<Result<Path, StreamError>>>()
    .await
    .into_iter()
    .map(|r| r.expect("no record may fail"))
    .collect();

    assert_eq!(decoded, paths);
}

/// `csv-core` records one end offset per field whether or not that field wrote any bytes, so a
/// record of many empty fields grows the offset vector while the field bytes stay near zero.
/// A limit that only counted bytes would never trip.
#[tokio::test]
async fn many_empty_fields_still_hit_the_length_limit() {
    // The field is never read; the point is that the record never gets far enough to
    // deserialise, because framing it should hit the limit first.
    #[derive(Debug, Deserialize)]
    struct Anything(#[allow(dead_code)] Vec<String>);

    let body = format!("{}\n", ",".repeat(50_000));
    let format = CsvStreamFormat::new(false, b',');
    let options = DecodeOptions::new().max_obj_len(4096);

    let results: Vec<Result<Anything, StreamError>> = Box::pin(decode_stream(
        futures::stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(body))]),
        StreamFormatDecode::<Anything>::framer(&format, &options),
        StreamFormatDecode::<Anything>::parser(&format),
        &options,
    ))
    .collect()
    .await;

    assert!(
        results.iter().any(|r| r.is_err()),
        "a record of many empty fields must still hit the limit"
    );
}
