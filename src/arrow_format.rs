//! Apache Arrow IPC, both directions.

use crate::content_type::ContentType;
use crate::error::{StreamError, StreamErrorKind};
use crate::format::{
    DecodeOptions, IdentityParser, ItemEncoder, StreamFormat, StreamFormatDecode,
    StreamFormatEncode,
};
use crate::arrow_ipc_codec::ArrowIpcCodec;
use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ipc::writer::{
    write_message, DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions,
};
use bytes::{BufMut, BytesMut};
use std::io::Write;
use std::sync::Arc;

const ARROW_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";

/// The four-byte continuation marker plus a zero length, which ends an Arrow IPC stream.
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];
const TOTAL_LEN: [u8; 4] = [0; 4];

/// Arrow record batches in IPC stream framing.
///
///
/// Encoding needs the schema up front, because it is written once ahead of the first batch.
/// Decoding does not: the schema arrives in the stream.
#[derive(Debug, Clone)]
pub struct ArrowRecordBatchIpcStreamFormat {
    schema: SchemaRef,
    options: IpcWriteOptions,
}

impl ArrowRecordBatchIpcStreamFormat {
    /// A format writing batches of `schema` with default IPC options.
    pub fn new(schema: Arc<Schema>) -> Self {
        Self::with_options(schema, IpcWriteOptions::default())
    }

    /// A format writing batches of `schema` with the given IPC options.
    pub fn with_options(schema: Arc<Schema>, options: IpcWriteOptions) -> Self {
        Self { schema, options }
    }

    /// A format for **decoding** only.
    ///
    /// An Arrow IPC stream carries its own schema, so a decoder needs none. Named for what it
    /// is rather than offered as a `Default`, because encoding with it would write an empty
    /// schema and produce a useless stream.
    pub fn for_decoding() -> Self {
        Self::new(Arc::new(Schema::empty()))
    }
}

impl crate::format::DefaultFormat for ArrowRecordBatchIpcStreamFormat {
    /// Decoding needs no configuration; see [`for_decoding`](Self::for_decoding).
    fn default_format() -> Self {
        Self::for_decoding()
    }
}

impl StreamFormat for ArrowRecordBatchIpcStreamFormat {
    fn format_name(&self) -> &'static str {
        "arrow"
    }

    fn default_content_type(&self) -> &'static str {
        ARROW_CONTENT_TYPE
    }

    fn accepts_content_type(&self, ct: &ContentType<'_>) -> bool {
        ct.matches(ARROW_CONTENT_TYPE)
    }
}

fn arrow_error(err: ArrowError) -> StreamError {
    StreamError::new(StreamErrorKind::CodecError, Some(Box::new(err)), None)
}

/// Per-stream state for [`ArrowRecordBatchIpcStreamFormat`].
///
/// Genuinely stateful, unlike every other format here: the dictionary tracker spans the whole
/// stream, so batches cannot be encoded independently of one another.
pub struct ArrowIpcEncoder {
    schema: SchemaRef,
    options: IpcWriteOptions,
    data_gen: IpcDataGenerator,
    dictionary_tracker: DictionaryTracker,
    write_context: IpcWriteContext,
}

impl ItemEncoder<RecordBatch> for ArrowIpcEncoder {
    fn encode(
        &mut self,
        item: &RecordBatch,
        index: u64,
        buf: &mut BytesMut,
    ) -> Result<(), StreamError> {
        let mut writer = buf.writer();

        // The schema message goes ahead of the first batch and nowhere else.
        if index == 0 {
            let encoded = self.data_gen.schema_to_bytes_with_dictionary_tracker(
                &self.schema,
                &mut self.dictionary_tracker,
                &self.options,
            );
            write_message(&mut writer, encoded, &self.options).map_err(arrow_error)?;
        }

        let (encoded_dictionaries, encoded_message) = self
            .data_gen
            .encode(
                item,
                &mut self.dictionary_tracker,
                &self.options,
                &mut self.write_context,
            )
            .map_err(arrow_error)?;

        for encoded_dictionary in encoded_dictionaries {
            write_message(&mut writer, encoded_dictionary, &self.options).map_err(arrow_error)?;
        }

        write_message(&mut writer, encoded_message, &self.options).map_err(arrow_error)?;
        writer.flush().map_err(|err| {
            StreamError::new(StreamErrorKind::CodecError, Some(Box::new(err)), None)
        })
    }

    /// The end-of-stream marker. Emitted only on a clean end, so a truncated body stays
    /// visibly truncated rather than reading as a complete, empty-tailed stream.
    fn epilogue(&mut self, buf: &mut BytesMut) -> Result<(), StreamError> {
        buf.extend_from_slice(&CONTINUATION_MARKER);
        buf.extend_from_slice(&TOTAL_LEN);
        Ok(())
    }
}

impl StreamFormatEncode<RecordBatch> for ArrowRecordBatchIpcStreamFormat {
    type Encoder = ArrowIpcEncoder;

    fn encoder(&self) -> Self::Encoder {
        ArrowIpcEncoder {
            schema: self.schema.clone(),
            options: self.options.clone(),
            data_gen: IpcDataGenerator::default(),
            dictionary_tracker: DictionaryTracker::new(false),
            write_context: IpcWriteContext::default(),
        }
    }
}

impl StreamFormatDecode<RecordBatch> for ArrowRecordBatchIpcStreamFormat {
    type Frame = Result<RecordBatch, StreamError>;
    type Framer = ArrowIpcCodec;
    type Parser = IdentityParser;

    fn framer(&self, options: &DecodeOptions) -> Self::Framer {
        ArrowIpcCodec::new_with_max_length(options.max_obj_len)
    }

    fn parser(&self) -> Self::Parser {
        IdentityParser
    }
}
