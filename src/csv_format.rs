//! CSV, both directions.

use crate::content_type::ContentType;
use crate::csv_record_codec::{CsvFrameConfig, CsvRecordCodec};
use crate::error::{StreamError, StreamErrorKind};
use crate::format::{
    DecodeOptions, DefaultFormat, FrameParser, ItemEncoder, StreamFormat, StreamFormatDecode,
    StreamFormatEncode,
};
use bytes::BytesMut;
use serde::{Deserialize, Serialize};

const CSV_CONTENT_TYPE: &str = "text/csv";
const CSV_ALIASES: &[&str] = &["text/csv", "application/csv"];

/// CSV rows, with an optional header row.
#[derive(Debug, Clone)]
pub struct CsvStreamFormat {
    has_headers: bool,
    delimiter: u8,
    flexible: bool,
    quote_style: csv::QuoteStyle,
    quote: u8,
    double_quote: bool,
    escape: u8,
    terminator: csv::Terminator,
}

impl Default for CsvStreamFormat {
    fn default() -> Self {
        Self {
            has_headers: true,
            delimiter: b',',
            flexible: false,
            quote_style: csv::QuoteStyle::Necessary,
            quote: b'"',
            double_quote: true,
            escape: b'\\',
            terminator: csv::Terminator::Any(b'\n'),
        }
    }
}

impl DefaultFormat for CsvStreamFormat {
    fn default_format() -> Self {
        Self::default()
    }
}

impl CsvStreamFormat {
    /// CSV with the given header behaviour and field delimiter, everything else default.
    pub fn new(has_headers: bool, delimiter: u8) -> Self {
        Self {
            has_headers,
            delimiter,
            ..Default::default()
        }
    }

    /// Sets whether to use flexible serialize.
    ///
    /// Encode only. On the way back in, records are framed and deserialised one at a time, so
    /// there is no "first record" to compare a field count against. That was already true of
    /// the previous decoder, where a fresh reader was built per row.
    pub fn with_flexible(mut self, flexible: bool) -> Self {
        self.flexible = flexible;
        self
    }

    /// Sets the quote style to use.
    pub fn with_quote_style(mut self, quote_style: csv::QuoteStyle) -> Self {
        self.quote_style = quote_style;
        self
    }

    /// Sets the quote character to use.
    pub fn with_quote(mut self, quote: u8) -> Self {
        self.quote = quote;
        self
    }

    /// Sets whether to double quote.
    pub fn with_double_quote(mut self, double_quote: bool) -> Self {
        self.double_quote = double_quote;
        self
    }

    /// Sets the escape character to use.
    pub fn with_escape(mut self, escape: u8) -> Self {
        self.escape = escape;
        self
    }

    /// Sets the record terminator to use.
    ///
    /// Honoured in both directions: the framer is configured from the same value.
    pub fn with_terminator(mut self, terminator: csv::Terminator) -> Self {
        self.terminator = terminator;
        self
    }

    /// Set the field delimiter to use.
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Set whether to write headers.
    pub fn with_has_headers(mut self, has_headers: bool) -> Self {
        self.has_headers = has_headers;
        self
    }

    fn writer_builder(&self, write_headers: bool) -> csv::WriterBuilder {
        let mut builder = csv::WriterBuilder::new();
        builder
            .has_headers(write_headers)
            .delimiter(self.delimiter)
            .flexible(self.flexible)
            .quote_style(self.quote_style)
            .quote(self.quote)
            .double_quote(self.double_quote)
            .escape(self.escape)
            .terminator(self.terminator);
        builder
    }

    fn frame_config(&self) -> CsvFrameConfig {
        CsvFrameConfig {
            delimiter: self.delimiter,
            quote: self.quote,
            double_quote: self.double_quote,
            // `csv`'s writer escapes by doubling unless `double_quote` is off, so the escape
            // character only applies in the other case.
            escape: if self.double_quote {
                None
            } else {
                Some(self.escape)
            },
            terminator: match self.terminator {
                csv::Terminator::CRLF => csv_core::Terminator::CRLF,
                csv::Terminator::Any(b) => csv_core::Terminator::Any(b),
                _ => csv_core::Terminator::Any(b'\n'),
            },
        }
    }
}

impl StreamFormat for CsvStreamFormat {
    fn format_name(&self) -> &'static str {
        "csv"
    }

    fn default_content_type(&self) -> &'static str {
        CSV_CONTENT_TYPE
    }

    fn accepts_content_type(&self, ct: &ContentType<'_>) -> bool {
        ct.matches_any(CSV_ALIASES)
    }
}

/// Per-stream state for [`CsvStreamFormat`].
///
/// A fresh `csv::Writer` is built for every row, because `csv` writes its header from the
/// first serialised record and there is no way to ask an existing writer to stop. The header
/// therefore has to be produced by a writer configured for it, and every later row by one that
/// is not.
pub struct CsvEncoder {
    format: CsvStreamFormat,
}

impl<T> ItemEncoder<T> for CsvEncoder
where
    T: Serialize,
{
    fn encode(&mut self, item: &T, index: u64, buf: &mut BytesMut) -> Result<(), StreamError> {
        let write_headers = index == 0 && self.format.has_headers;
        let mut writer = self
            .format
            .writer_builder(write_headers)
            .from_writer(Vec::new());

        writer
            .serialize(item)
            .map_err(|err| StreamError::new(StreamErrorKind::CodecError, Some(Box::new(err)), None))?;
        writer
            .flush()
            .map_err(|err| StreamError::new(StreamErrorKind::CodecError, Some(Box::new(err)), None))?;

        let bytes = writer
            .into_inner()
            .map_err(|err| StreamError::new(StreamErrorKind::CodecError, Some(Box::new(err)), None))?;

        buf.extend_from_slice(&bytes);
        Ok(())
    }
}

impl<T> StreamFormatEncode<T> for CsvStreamFormat
where
    T: Serialize,
{
    type Encoder = CsvEncoder;

    fn encoder(&self) -> Self::Encoder {
        CsvEncoder {
            format: self.clone(),
        }
    }
}

/// Deserialises one framed CSV record.
///
/// Positional, not by header name: the header row is consumed by the framer and discarded,
/// which is the behaviour this pair of crates has always had.
#[derive(Debug, Clone, Copy, Default)]
pub struct CsvParser;

impl<T> FrameParser<csv::ByteRecord, T> for CsvParser
where
    T: for<'de> Deserialize<'de>,
{
    fn parse(&self, frame: csv::ByteRecord) -> Result<T, StreamError> {
        frame.deserialize(None).map_err(|err| {
            StreamError::new(StreamErrorKind::CodecError, Some(Box::new(err)), None)
        })
    }
}

impl<T> StreamFormatDecode<T> for CsvStreamFormat
where
    T: for<'de> Deserialize<'de>,
{
    type Frame = csv::ByteRecord;
    type Framer = CsvRecordCodec;
    type Parser = CsvParser;

    fn framer(&self, options: &DecodeOptions) -> Self::Framer {
        CsvRecordCodec::new(self.frame_config(), self.has_headers, options.max_obj_len)
    }

    fn parser(&self) -> Self::Parser {
        CsvParser
    }
}
