#![cfg(feature = "arrow")]
use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use futures::StreamExt;
use http_streams_core::format::{DecodeOptions, StreamFormatDecode, StreamFormatEncode};
use http_streams_core::{
    decode_stream, encode_stream, ArrowRecordBatchIpcStreamFormat, StreamError,
};
use std::sync::Arc;

/// The per-object limit counts a record's own bytes, not the buffer it arrived in.
///
/// The codec adds each `decode` call's input length to a running total, which is only correct
/// because `arrow`'s `StreamDecoder` drains the buffer every call. Were it to leave bytes
/// behind, the total would grow quadratically and trip far below the configured limit: this
/// body is 63 chunks, so a quadratic sum would be roughly 15 MB against a 600 KB limit.
#[tokio::test]
async fn a_large_batch_does_not_trip_a_far_larger_limit() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    // ~120 KiB of payload: well under a 1 MiB per-object limit.
    let values: Vec<i32> = (0..125_000).collect();
    let col: ArrayRef = Arc::new(Int32Array::from(values));
    let batch = RecordBatch::try_new(schema.clone(), vec![col]).unwrap();

    let format = ArrowRecordBatchIpcStreamFormat::new(schema);
    let src = futures::stream::iter(vec![Ok::<_, StreamError>(batch.clone())]);
    let mut enc = Vec::new();
    let mut s = Box::pin(encode_stream(
        src,
        StreamFormatEncode::<RecordBatch>::encoder(&format),
    ));
    while let Some(c) = s.next().await {
        enc.extend_from_slice(&c.unwrap());
    }

    // 8 KiB chunks, exactly what a real HTTP body delivers.
    let chunks: Vec<Result<Bytes, std::io::Error>> = enc
        .chunks(8 * 1024)
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .collect();

    let o = DecodeOptions::new().max_obj_len(600 * 1024);
    let got: Vec<Result<RecordBatch, StreamError>> = Box::pin(decode_stream(
        futures::stream::iter(chunks),
        StreamFormatDecode::<RecordBatch>::framer(&format, &o),
        StreamFormatDecode::<RecordBatch>::parser(&format),
        &o,
    ))
    .collect()
    .await;

    for r in &got {
        if let Err(e) = r {
            println!("ERROR: {e}");
        }
    }
    let ok: Vec<_> = got.into_iter().filter_map(|r| r.ok()).collect();
    assert_eq!(
        ok.len(),
        1,
        "the batch must decode: linear accounting stays under the limit, quadratic would not"
    );
}
