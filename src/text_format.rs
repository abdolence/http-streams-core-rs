//! Raw UTF-8 text. Encode only.
//!
//! There is no decoder and there cannot be one. The framing writes each string's bytes with no
//! delimiter at all, so `["ab", "c"]` and `["a", "bc"]` produce identical bytes and splitting
//! them back into the original items is not merely unimplemented but impossible. Offering a
//! decoder that handed back HTTP chunk boundaries as though they were items would make `text`
//! the one format whose round trip silently lies.
//!
//! If a raw byte stream is what is wanted, take the body as bytes directly rather than through
//! this format.

use crate::content_type::ContentType;
use crate::error::StreamError;
use crate::format::{DefaultFormat, ItemEncoder, StreamFormat, StreamFormatEncode};
use bytes::BytesMut;

const TEXT_CONTENT_TYPE: &str = "text/plain; charset=utf-8";

/// Raw UTF-8 text, undelimited.
#[derive(Debug, Clone, Copy, Default)]
pub struct TextStreamFormat;

impl TextStreamFormat {
    /// A raw text format.
    pub fn new() -> Self {
        Self
    }
}

impl DefaultFormat for TextStreamFormat {
    fn default_format() -> Self {
        Self
    }
}

impl StreamFormat for TextStreamFormat {
    fn format_name(&self) -> &'static str {
        "text"
    }

    fn default_content_type(&self) -> &'static str {
        TEXT_CONTENT_TYPE
    }

    fn accepts_content_type(&self, ct: &ContentType<'_>) -> bool {
        ct.matches("text/plain")
    }
}

/// Per-stream state for [`TextStreamFormat`]. There is none.
#[derive(Debug, Clone, Copy, Default)]
pub struct TextEncoder;

impl ItemEncoder<String> for TextEncoder {
    fn encode(&mut self, item: &String, _index: u64, buf: &mut BytesMut) -> Result<(), StreamError> {
        buf.extend_from_slice(item.as_bytes());
        Ok(())
    }
}

impl StreamFormatEncode<String> for TextStreamFormat {
    type Encoder = TextEncoder;

    fn encoder(&self) -> Self::Encoder {
        TextEncoder
    }
}
