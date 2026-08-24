//! Decoding a JSON array into its elements, incrementally.
//!
//! Elements of a JSON array are not self-delimiting: there is no marker that says "an element
//! ends here" without tracking nesting, quoting and escaping. So this is a byte-level state
//! machine rather than a delimiter search, and it handles objects, arrays, strings and bare
//! primitives as elements.

use crate::error::{StreamError, StreamErrorKind};
use bytes::{Buf, BytesMut};
use serde::Deserialize;
use std::marker::PhantomData;

/// A [`Decoder`](tokio_util::codec::Decoder) that yields the elements of a JSON array.
#[derive(Clone, Debug)]
pub struct JsonArrayCodec<T> {
    max_length: usize,
    json_cursor: JsonCursor,
    /// `fn() -> T` rather than `T`: a bare `PhantomData<T>` would make this codec `!Send`
    /// whenever `T` is, and the crates built on this one promise `Send` streams for item
    /// types that carry no such bound. The item type is produced, never held, so this is also
    /// the honest variance.
    _ph: PhantomData<fn() -> T>,
}

#[derive(Clone, Debug)]
struct JsonCursor {
    current_offset: usize,
    array_is_opened: bool,
    delimiter_expected: bool,
    quote_opened: bool,
    escaped: bool,
    opened_brackets: usize,
    current_obj_pos: usize,
    /// When `Some(pos)`, a primitive value (number/bool/null/string) is being accumulated from
    /// `pos` in the buffer. A quoted string also uses this.
    current_primitive_start: Option<usize>,
}

impl<T> JsonArrayCodec<T> {
    /// A codec that rejects any single element longer than `max_length` bytes.
    pub fn new_with_max_length(max_length: usize) -> Self {
        let initial_cursor = JsonCursor {
            current_offset: 0,
            array_is_opened: false,
            delimiter_expected: false,
            quote_opened: false,
            escaped: false,
            opened_brackets: 0,
            current_obj_pos: 0,
            current_primitive_start: None,
        };

        JsonArrayCodec {
            max_length,
            json_cursor: initial_cursor,
            _ph: PhantomData,
        }
    }
}

fn codec_error(err: serde_json::Error) -> StreamError {
    StreamError::new(StreamErrorKind::CodecError, Some(Box::new(err)), None)
}

impl<T> tokio_util::codec::Decoder for JsonArrayCodec<T>
where
    T: for<'de> Deserialize<'de>,
{
    /// Always `Ok(_)`: every failure this format can have is terminal, because the cursor
    /// tracks nesting across records and cannot resynchronise after a bad one.
    type Item = Result<T, StreamError>;
    type Error = StreamError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, StreamError> {
        if buf.is_empty() {
            return Ok(None);
        }

        for (position, current_ch) in buf[self.json_cursor.current_offset..buf.len()]
            .iter()
            .enumerate()
        {
            let abs_pos = self.json_cursor.current_offset + position;

            if abs_pos >= self.max_length {
                return Err(StreamError::new(
                    StreamErrorKind::MaxLenReachedError,
                    None,
                    Some("Max object length reached".into()),
                ));
            }

            match *current_ch {
                b'[' if !self.json_cursor.quote_opened && self.json_cursor.opened_brackets == 0 => {
                    if self.json_cursor.array_is_opened {
                        // A nested array element, treated like an object open.
                        self.json_cursor.current_obj_pos = abs_pos;
                        self.json_cursor.opened_brackets += 1;
                        self.json_cursor.current_primitive_start = None;
                    } else {
                        self.json_cursor.array_is_opened = true;
                    }
                }
                b'[' if !self.json_cursor.quote_opened && self.json_cursor.opened_brackets > 0 => {
                    self.json_cursor.opened_brackets += 1;
                    self.json_cursor.escaped = false;
                }
                b']' if !self.json_cursor.quote_opened && self.json_cursor.opened_brackets == 0 => {
                    // End of the top-level array. Emit any pending primitive.
                    if let Some(prim_start) = self.json_cursor.current_primitive_start.take() {
                        let obj_slice = trim_ascii(&buf[prim_start..abs_pos]);
                        if !obj_slice.is_empty() {
                            let result = serde_json::from_slice(obj_slice).map_err(codec_error);
                            buf.advance(abs_pos + 1);
                            self.json_cursor.current_offset = 0;
                            self.json_cursor.delimiter_expected = false;
                            return result.map(|item| Some(Ok(item)));
                        }
                    }
                }
                b']' if !self.json_cursor.quote_opened && self.json_cursor.opened_brackets > 0 => {
                    self.json_cursor.opened_brackets -= 1;
                    self.json_cursor.escaped = false;
                    if self.json_cursor.opened_brackets == 0 {
                        // Closed a nested array/object element.
                        self.json_cursor.delimiter_expected = true;
                        let obj_slice = &buf[self.json_cursor.current_obj_pos..abs_pos + 1];
                        let result = serde_json::from_slice(obj_slice).map_err(codec_error);
                        self.json_cursor.current_obj_pos = 0;
                        buf.advance(abs_pos + 1);
                        self.json_cursor.current_offset = 0;
                        return result.map(|item| Some(Ok(item)));
                    }
                }
                b'"' if !self.json_cursor.escaped && self.json_cursor.opened_brackets == 0 => {
                    if self.json_cursor.quote_opened {
                        // Closing quote of a top-level string element.
                        self.json_cursor.quote_opened = false;
                        if let Some(prim_start) = self.json_cursor.current_primitive_start.take() {
                            self.json_cursor.delimiter_expected = true;
                            let obj_slice = &buf[prim_start..abs_pos + 1];
                            let result = serde_json::from_slice(obj_slice).map_err(codec_error);
                            buf.advance(abs_pos + 1);
                            self.json_cursor.current_offset = 0;
                            return result.map(|item| Some(Ok(item)));
                        }
                    } else {
                        // Opening quote of a top-level string element.
                        self.json_cursor.quote_opened = true;
                        if self.json_cursor.current_primitive_start.is_none() {
                            self.json_cursor.current_primitive_start = Some(abs_pos);
                        }
                    }
                }
                b'"' if !self.json_cursor.escaped => {
                    // Inside a nested object/array.
                    self.json_cursor.quote_opened = !self.json_cursor.quote_opened;
                }
                b'\\' if self.json_cursor.quote_opened => {
                    self.json_cursor.escaped = !self.json_cursor.escaped;
                }
                b'{' if !self.json_cursor.quote_opened => {
                    if self.json_cursor.opened_brackets == 0 {
                        self.json_cursor.current_obj_pos = abs_pos;
                        self.json_cursor.current_primitive_start = None;
                    }
                    self.json_cursor.opened_brackets += 1;
                    self.json_cursor.escaped = false;
                }
                // Guarded like the `]` arm above. Without this, a stray `}` at the top level
                // underflows the counter: a panic in debug, and in release a wrap to
                // `usize::MAX` that silently corrupts framing for the rest of the stream.
                b'}' if !self.json_cursor.quote_opened && self.json_cursor.opened_brackets == 0 => {
                    return Err(StreamError::new(
                        StreamErrorKind::CodecError,
                        None,
                        Some("Unexpected `}` outside any object".into()),
                    ));
                }
                b'}' if !self.json_cursor.quote_opened => {
                    self.json_cursor.opened_brackets -= 1;
                    self.json_cursor.escaped = false;
                    if self.json_cursor.opened_brackets == 0 {
                        self.json_cursor.delimiter_expected = true;
                        let obj_slice = &buf[self.json_cursor.current_obj_pos..abs_pos + 1];
                        let result = serde_json::from_slice(obj_slice).map_err(codec_error);
                        self.json_cursor.current_obj_pos = 0;
                        buf.advance(abs_pos + 1);
                        self.json_cursor.current_offset = 0;
                        return result.map(|item| Some(Ok(item)));
                    }
                }
                b',' if !self.json_cursor.quote_opened && self.json_cursor.opened_brackets == 0 => {
                    if let Some(prim_start) = self.json_cursor.current_primitive_start.take() {
                        let obj_slice = trim_ascii(&buf[prim_start..abs_pos]);
                        if !obj_slice.is_empty() {
                            let result = serde_json::from_slice(obj_slice).map_err(codec_error);
                            buf.advance(abs_pos + 1);
                            self.json_cursor.current_offset = 0;
                            self.json_cursor.delimiter_expected = false;
                            return result.map(|item| Some(Ok(item)));
                        }
                    } else if !self.json_cursor.delimiter_expected {
                        return Err(StreamError::new(
                            StreamErrorKind::CodecError,
                            None,
                            Some("Unexpected delimiter found".into()),
                        ));
                    }
                    self.json_cursor.delimiter_expected = false;
                }
                _ if !self.json_cursor.quote_opened
                    && self.json_cursor.opened_brackets == 0
                    && self.json_cursor.array_is_opened
                    && !current_ch.is_ascii_whitespace() =>
                {
                    // Non-whitespace at top level inside the array: the start of a primitive.
                    if self.json_cursor.current_primitive_start.is_none() {
                        self.json_cursor.current_primitive_start = Some(abs_pos);
                    }
                    self.json_cursor.escaped = false;
                }
                _ => {
                    self.json_cursor.escaped = false;
                }
            }
        }
        self.json_cursor.current_offset = buf.len();

        Ok(None)
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, StreamError> {
        self.decode(buf)
    }
}

fn trim_ascii(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map(|i| i + 1)
        .unwrap_or(0);
    if start >= end {
        &[]
    } else {
        &bytes[start..end]
    }
}
