//! Decoding an Arrow IPC stream.

use crate::error::{StreamError, StreamErrorKind};
use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamDecoder;
use bytes::{Buf, BytesMut};

/// A [`Decoder`](tokio_util::codec::Decoder) that yields one [`RecordBatch`] per IPC message.
///
/// The schema is read from the stream, so nothing needs to be configured to decode one.
#[derive(Debug)]
pub struct ArrowIpcCodec {
    max_length: usize,
    decoder: StreamDecoder,
    current_obj_len: usize,
}

impl ArrowIpcCodec {
    /// A codec that rejects any single batch longer than `max_length` bytes.
    pub fn new_with_max_length(max_length: usize) -> Self {
        ArrowIpcCodec {
            max_length,
            decoder: StreamDecoder::new(),
            current_obj_len: 0,
        }
    }
}

impl tokio_util::codec::Decoder for ArrowIpcCodec {
    /// Always `Ok(_)`: the decoder carries dictionary state across the whole stream, so a
    /// message it could not read leaves it unable to interpret the ones after it.
    type Item = Result<RecordBatch, StreamError>;
    type Error = StreamError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, StreamError> {
        let buf_len = buf.len();
        if buf_len == 0 {
            return Ok(None);
        }

        let obj_bytes = buf.as_ref();
        let obj_bytes_len = obj_bytes.len();
        let mut buffer = arrow::buffer::Buffer::from(obj_bytes);
        let maybe_record = self.decoder.decode(&mut buffer).map_err(|e| {
            StreamError::new(
                StreamErrorKind::CodecError,
                Some(Box::new(e)),
                Some("Decode arrow IPC record error".into()),
            )
        })?;

        if maybe_record.is_none() {
            self.current_obj_len += obj_bytes_len;
        } else {
            self.current_obj_len = 0;
        }

        if self.current_obj_len > self.max_length {
            return Err(StreamError::new(
                StreamErrorKind::MaxLenReachedError,
                None,
                Some("Object length exceeds the maximum length".into()),
            ));
        }

        buf.advance(obj_bytes_len - buffer.len());
        Ok(maybe_record.map(Ok))
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, StreamError> {
        self.decode(buf)
    }
}
