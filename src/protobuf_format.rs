//! Length-prefixed protobuf, both directions.

use crate::content_type::ContentType;
use crate::error::StreamError;
use crate::format::{
    DecodeOptions, DefaultFormat, IdentityParser, ItemEncoder, StreamFormat, StreamFormatDecode,
    StreamFormatEncode,
};
use crate::protobuf_len_codec::ProtobufLenPrefixCodec;
use bytes::BytesMut;

const PROTOBUF_CONTENT_TYPE: &str = "application/x-protobuf-stream";
/// The stream type this crate emits, plus the two names used for a bare protobuf payload.
///
/// Deliberately **not** `application/octet-stream`: accepting it would mean decoding any
/// unlabelled binary body as protobuf frames.
const PROTOBUF_ALIASES: &[&str] = &[
    "application/x-protobuf-stream",
    "application/x-protobuf",
    "application/protobuf",
];

/// Protobuf messages, each preceded by its length as a LEB128 varint.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProtobufStreamFormat;

impl ProtobufStreamFormat {
    /// A length-prefixed protobuf format.
    pub fn new() -> Self {
        Self
    }
}

impl DefaultFormat for ProtobufStreamFormat {
    fn default_format() -> Self {
        Self
    }
}

impl StreamFormat for ProtobufStreamFormat {
    fn format_name(&self) -> &'static str {
        "protobuf"
    }

    fn default_content_type(&self) -> &'static str {
        PROTOBUF_CONTENT_TYPE
    }

    fn accepts_content_type(&self, ct: &ContentType<'_>) -> bool {
        ct.matches_any(PROTOBUF_ALIASES)
    }
}

/// Per-stream state for [`ProtobufStreamFormat`]. There is none: every frame is self-describing.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProtobufEncoder;

impl<T> ItemEncoder<T> for ProtobufEncoder
where
    T: prost::Message,
{
    fn encode(&mut self, item: &T, _index: u64, buf: &mut BytesMut) -> Result<(), StreamError> {
        let encoded = item.encode_to_vec();
        let mut frame = Vec::with_capacity(encoded.len() + 10);
        prost::encoding::encode_varint(encoded.len() as u64, &mut frame);
        frame.extend(encoded);
        buf.extend_from_slice(&frame);
        Ok(())
    }
}

impl<T> StreamFormatEncode<T> for ProtobufStreamFormat
where
    T: prost::Message,
{
    type Encoder = ProtobufEncoder;

    fn encoder(&self) -> Self::Encoder {
        ProtobufEncoder
    }
}

impl<T> StreamFormatDecode<T> for ProtobufStreamFormat
where
    T: prost::Message + Default,
{
    type Frame = Result<T, StreamError>;
    type Framer = ProtobufLenPrefixCodec<T>;
    type Parser = IdentityParser;

    fn framer(&self, options: &DecodeOptions) -> Self::Framer {
        ProtobufLenPrefixCodec::new_with_max_length(options.max_obj_len)
    }

    fn parser(&self) -> Self::Parser {
        IdentityParser
    }
}
