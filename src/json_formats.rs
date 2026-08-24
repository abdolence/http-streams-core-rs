//! JSON array and JSON Lines, both directions.

use crate::content_type::ContentType;
use crate::envelope::StreamFormatEnvelope;
use crate::error::{StreamError, StreamErrorKind};
use crate::format::{
    DecodeOptions, DefaultFormat, IdentityParser, ItemEncoder, StreamFormat, StreamFormatDecode,
    StreamFormatEncode,
};
use crate::json_array_codec::JsonArrayCodec;
use crate::json_nl_codec::JsonNewLineCodec;
use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::io::Write;

const JSON_ARRAY_BEGIN: &[u8] = b"[";
const JSON_ARRAY_END: &[u8] = b"]";
const JSON_ARRAY_ENVELOPE_END: &[u8] = b"]}";
const JSON_SEP: &[u8] = b",";
const JSON_NL_SEP: &[u8] = b"\n";

/// `application/json`, plus anything using the `+json` structured suffix.
const JSON_ARRAY_CONTENT_TYPE: &str = "application/json";

/// What this crate emits, plus the de-facto names the ecosystem uses for the same framing.
///
/// Deliberately **not** `application/json-seq`: RFC 7464 separates records with 0x1E rather
/// than a newline, so accepting it here would mis-decode.
const JSON_NL_CONTENT_TYPE: &str = "application/jsonstream";
const JSON_NL_ALIASES: &[&str] = &[
    "application/jsonstream",
    "application/x-ndjson",
    "application/ndjson",
    "application/jsonl",
    "application/x-jsonl",
];

/// A JSON array: `[item, item, …]`, optionally wrapped in an envelope object.
#[derive(Debug, Clone)]
pub struct JsonArrayStreamFormat<E = ()>
where
    E: Serialize,
{
    envelope: Option<StreamFormatEnvelope<E>>,
}

impl JsonArrayStreamFormat {
    /// A bare array.
    pub fn new() -> JsonArrayStreamFormat<()> {
        JsonArrayStreamFormat { envelope: None }
    }

    /// An array emitted as `array_field` of `envelope`.
    ///
    /// Encode only: the decoder expects a bare array and will not unwrap an envelope.
    pub fn with_envelope<E>(envelope: E, array_field: &str) -> JsonArrayStreamFormat<E>
    where
        E: Serialize,
    {
        JsonArrayStreamFormat {
            envelope: Some(StreamFormatEnvelope {
                object: envelope,
                array_field: array_field.to_string(),
            }),
        }
    }
}

impl Default for JsonArrayStreamFormat<()> {
    fn default() -> Self {
        JsonArrayStreamFormat { envelope: None }
    }
}

impl DefaultFormat for JsonArrayStreamFormat<()> {
    fn default_format() -> Self {
        Self::default()
    }
}

impl<E> StreamFormat for JsonArrayStreamFormat<E>
where
    E: Serialize,
{
    fn format_name(&self) -> &'static str {
        "json_array"
    }

    fn default_content_type(&self) -> &'static str {
        JSON_ARRAY_CONTENT_TYPE
    }

    fn accepts_content_type(&self, ct: &ContentType<'_>) -> bool {
        ct.matches(JSON_ARRAY_CONTENT_TYPE) || ct.has_suffix("json")
    }
}

/// Per-stream state for [`JsonArrayStreamFormat`].
///
/// The prologue is computed up front, in [`encoder`], because serialising the envelope can
/// fail and the failure must be reported through the stream rather than panicking.
///
/// [`encoder`]: StreamFormatEncode::encoder
pub struct JsonArrayEncoder {
    prologue: Result<Vec<u8>, Option<StreamError>>,
    epilogue: &'static [u8],
}

impl JsonArrayEncoder {
    fn bare() -> Self {
        Self {
            prologue: Ok(JSON_ARRAY_BEGIN.to_vec()),
            epilogue: JSON_ARRAY_END,
        }
    }

    fn enveloped<E: Serialize>(envelope: &StreamFormatEnvelope<E>) -> Self {
        // The envelope object is serialised whole, then its trailing `}` is removed so the
        // array can be appended as one more field. Anything shorter than `{}` is not an object
        // and there is nothing sensible to append to.
        let prologue = match serde_json::to_vec(&envelope.object) {
            Ok(bytes) if bytes.len() > 1 => {
                let mut buf = Vec::with_capacity(bytes.len() + envelope.array_field.len() + 4);
                buf.extend_from_slice(&bytes[0..bytes.len() - 1]);
                // `{}` serialises to two bytes and needs no separator; anything longer already
                // has at least one field, so the array must be separated from it.
                if bytes.len() > 2 {
                    buf.extend_from_slice(JSON_SEP);
                }
                buf.extend_from_slice(format!("\"{}\":", envelope.array_field).as_bytes());
                buf.extend_from_slice(JSON_ARRAY_BEGIN);
                Ok(buf)
            }
            Ok(bytes) => Err(Some(StreamError::new(
                StreamErrorKind::CodecError,
                None,
                Some(format!("Too short envelope: {bytes:?}")),
            ))),
            Err(err) => Err(Some(StreamError::new(
                StreamErrorKind::CodecError,
                Some(Box::new(err)),
                None,
            ))),
        };

        Self {
            prologue,
            epilogue: JSON_ARRAY_ENVELOPE_END,
        }
    }
}

impl<T> ItemEncoder<T> for JsonArrayEncoder
where
    T: Serialize,
{
    fn prologue(&mut self, buf: &mut BytesMut) -> Result<(), StreamError> {
        match &mut self.prologue {
            Ok(bytes) => {
                buf.extend_from_slice(bytes);
                Ok(())
            }
            // Taken rather than cloned: `StreamError` is not `Clone`, and the encode run stops
            // at the first error anyway, so it can only be reported once.
            Err(err) => Err(err.take().unwrap_or_else(|| {
                StreamError::new(StreamErrorKind::CodecError, None, Some("Bad envelope".into()))
            })),
        }
    }

    fn encode(&mut self, item: &T, index: u64, buf: &mut BytesMut) -> Result<(), StreamError> {
        let mut writer = buf.writer();
        if index != 0 {
            writer
                .write_all(JSON_SEP)
                .map_err(|err| StreamError::new(StreamErrorKind::CodecError, Some(Box::new(err)), None))?;
        }
        serde_json::to_writer(&mut writer, item)
            .map_err(|err| StreamError::new(StreamErrorKind::CodecError, Some(Box::new(err)), None))
    }

    fn epilogue(&mut self, buf: &mut BytesMut) -> Result<(), StreamError> {
        buf.extend_from_slice(self.epilogue);
        Ok(())
    }
}

impl<T, E> StreamFormatEncode<T> for JsonArrayStreamFormat<E>
where
    T: Serialize,
    E: Serialize,
{
    type Encoder = JsonArrayEncoder;

    fn encoder(&self) -> Self::Encoder {
        match &self.envelope {
            Some(envelope) => JsonArrayEncoder::enveloped(envelope),
            None => JsonArrayEncoder::bare(),
        }
    }
}

impl<T, E> StreamFormatDecode<T> for JsonArrayStreamFormat<E>
where
    T: for<'de> Deserialize<'de>,
    E: Serialize,
{
    type Frame = Result<T, StreamError>;
    type Framer = JsonArrayCodec<T>;
    type Parser = IdentityParser;

    fn framer(&self, options: &DecodeOptions) -> Self::Framer {
        JsonArrayCodec::new_with_max_length(options.max_obj_len)
    }

    fn parser(&self) -> Self::Parser {
        IdentityParser
    }
}

/// JSON Lines: one JSON value per line, newline-terminated.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonNewLineStreamFormat;

impl JsonNewLineStreamFormat {
    /// A JSON Lines format.
    pub fn new() -> Self {
        Self
    }
}

impl DefaultFormat for JsonNewLineStreamFormat {
    fn default_format() -> Self {
        Self
    }
}

impl StreamFormat for JsonNewLineStreamFormat {
    fn format_name(&self) -> &'static str {
        "json_nl"
    }

    fn default_content_type(&self) -> &'static str {
        JSON_NL_CONTENT_TYPE
    }

    fn accepts_content_type(&self, ct: &ContentType<'_>) -> bool {
        ct.matches_any(JSON_NL_ALIASES)
    }
}

/// Per-stream state for [`JsonNewLineStreamFormat`]. There is none: the framing is per-item.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonNewLineEncoder;

impl<T> ItemEncoder<T> for JsonNewLineEncoder
where
    T: Serialize,
{
    fn encode(&mut self, item: &T, _index: u64, buf: &mut BytesMut) -> Result<(), StreamError> {
        let mut writer = buf.writer();
        serde_json::to_writer(&mut writer, item)
            .map_err(|err| StreamError::new(StreamErrorKind::CodecError, Some(Box::new(err)), None))?;
        writer
            .write_all(JSON_NL_SEP)
            .map_err(|err| StreamError::new(StreamErrorKind::CodecError, Some(Box::new(err)), None))
    }
}

impl<T> StreamFormatEncode<T> for JsonNewLineStreamFormat
where
    T: Serialize,
{
    type Encoder = JsonNewLineEncoder;

    fn encoder(&self) -> Self::Encoder {
        JsonNewLineEncoder
    }
}

impl<T> StreamFormatDecode<T> for JsonNewLineStreamFormat
where
    T: for<'de> Deserialize<'de>,
{
    type Frame = Result<T, StreamError>;
    type Framer = JsonNewLineCodec<T>;
    type Parser = IdentityParser;

    fn framer(&self, options: &DecodeOptions) -> Self::Framer {
        JsonNewLineCodec::new_with_max_length(options.max_obj_len)
    }

    fn parser(&self) -> Self::Parser {
        IdentityParser
    }
}
