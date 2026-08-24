#![forbid(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! Framework-neutral building blocks for streaming HTTP bodies as sequences of items.
//!
//! This crate holds the parts that are the same whichever HTTP library is in use: the wire
//! formats (encoding *and* decoding), the error type, and the progress/observability state
//! machine. It knows nothing about any specific client or server.
//!
//! You are unlikely to depend on it directly. It backs:
//!
//! - [axum-streams](https://github.com/abdolence/axum-streams-rs): server side
//! - [reqwest-streams](https://github.com/abdolence/reqwest-streams-rs): client side
//!
//! Both re-export the types they expose, so downstream code names them through those crates
//! rather than here.
//!
//! # Why this crate exists
//!
//! It was extracted from those two, which had grown up as a pair: one encoding response bodies
//! on the server, the other decoding them on the client. Adding streaming *request* bodies
//! meant each needed what the other already had, and the two had by then also grown
//! near-identical progress and error handling independently. The shared parts moved here so a
//! body encoded by one is decoded by the same code in the other.
//!
//! # Features
//!
//! **Note:** the `default` features do not include any formats.
//!
//! - `json`: JSON array and JSON Lines (JSONL)
//! - `csv`: CSV
//! - `protobuf`: length-prefixed Protobuf
//! - `arrow`: Apache Arrow IPC
//! - `text`: raw UTF-8 text (encode only, see below)
//! - `tracing`: report progress and errors through [tracing]
//!
//! # Directionality
//!
//! Every format can encode. All but `text` can decode: text framing writes raw bytes with no
//! delimiter, so `["ab", "c"]` and `["a", "bc"]` are byte-identical on the wire and splitting
//! them back into items is not merely unimplemented but impossible.
//!
//! [tracing]: https://docs.rs/tracing

#[macro_use]
mod macros;

pub mod buffer;
pub mod content_type;
pub mod envelope;
pub mod error;
pub mod format;
pub mod progress;
pub mod stream;

cfg_arrow! {
    pub use arrow_format::{ArrowIpcEncoder, ArrowRecordBatchIpcStreamFormat};
    pub use arrow_ipc_codec::ArrowIpcCodec;
    mod arrow_format;
    mod arrow_ipc_codec;
}

cfg_protobuf! {
    pub use protobuf_format::{ProtobufEncoder, ProtobufStreamFormat};
    pub use protobuf_len_codec::ProtobufLenPrefixCodec;
    mod protobuf_format;
    mod protobuf_len_codec;
}

cfg_text! {
    pub use text_format::{TextEncoder, TextStreamFormat};
    mod text_format;
}

cfg_csv! {
    pub use csv_format::{CsvEncoder, CsvParser, CsvStreamFormat};
    pub use csv_record_codec::{CsvFrameConfig, CsvRecordCodec};
    /// Re-exported so callers can configure [`CsvStreamFormat`] without depending on `csv`.
    pub use csv::{QuoteStyle, Terminator};
    mod csv_format;
    mod csv_record_codec;
}

cfg_json! {
    pub use json_formats::{
        JsonArrayEncoder, JsonArrayStreamFormat, JsonNewLineEncoder, JsonNewLineStreamFormat,
    };
    pub use json_array_codec::JsonArrayCodec;
    pub use json_nl_codec::JsonNewLineCodec;
    mod json_array_codec;
    mod json_formats;
    mod json_nl_codec;
}

pub use buffer::{buffer_bytes, buffer_ready_items};
pub use content_type::ContentType;
pub use envelope::StreamFormatEnvelope;
pub use error::{StreamError, StreamErrorKind};
pub use format::{
    DecodeOptions, DefaultFormat, FrameParser, IdentityParser, ItemEncoder, StreamFormat,
    StreamFormatDecode, StreamFormatEncode, DEFAULT_BUF_CAPACITY,
};
pub use progress::{
    count_bytes, count_items, instrument, Counting, Direction, ErrorInfo, Progress, ProgressItem,
    ProgressOptions, Side, StreamContext, StreamErrorHandler, StreamOutcome, StreamProgress,
    StreamProgressHandler, DEFAULT_PROGRESS_INTERVAL,
};
pub use stream::{decode_stream, encode_stream};

/// Alias for the [`Result`] type produced by streaming a body in either direction.
pub type StreamResult<T> = std::result::Result<T, StreamError>;
