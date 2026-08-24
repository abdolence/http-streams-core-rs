//! Decoding LEB128 length-prefixed protobuf messages.

use crate::error::{StreamError, StreamErrorKind};
use bytes::{Buf, BytesMut};
use std::marker::PhantomData;

/// A [`Decoder`](tokio_util::codec::Decoder) that yields one message per length-prefixed frame.
#[derive(Clone, Debug)]
pub struct ProtobufLenPrefixCodec<T> {
    max_length: usize,
    cursor: ProtobufCursor,
    /// `fn() -> T` rather than `T`: a bare `PhantomData<T>` would make this codec `!Send`
    /// whenever `T` is, and the crates built on this one promise `Send` streams for item
    /// types that carry no such bound. The item type is produced, never held, so this is also
    /// the honest variance.
    _ph: PhantomData<fn() -> T>,
}

#[derive(Clone, Debug)]
struct ProtobufCursor {
    /// The length of the message currently being accumulated.
    ///
    /// `Option`, not a bare `usize` with zero meaning "none": a protobuf message all of whose
    /// fields hold their default values encodes to *zero bytes*, so `0` is a perfectly ordinary
    /// frame length and conflating it with "no length read yet" would make the codec try to
    /// parse the following frame's length prefix as that message's body.
    expected_len: Option<usize>,
}

impl<T> ProtobufLenPrefixCodec<T> {
    /// A codec that rejects any single message longer than `max_length` bytes.
    pub fn new_with_max_length(max_length: usize) -> Self {
        let initial_cursor = ProtobufCursor { expected_len: None };

        ProtobufLenPrefixCodec {
            max_length,
            cursor: initial_cursor,
            _ph: PhantomData,
        }
    }
}

impl<T> tokio_util::codec::Decoder for ProtobufLenPrefixCodec<T>
where
    T: prost::Message + Default,
{
    /// Always `Ok(_)`: a message that fails to decode is reported as terminal, matching the
    /// behaviour this codec has always had.
    type Item = Result<T, StreamError>;
    type Error = StreamError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, StreamError> {
        // Loops rather than returning `Ok(None)` after reading a length prefix.
        //
        // `Ok(None)` means "I need more bytes", and `FramedRead` acts on it: during `decode_eof`
        // it ends the stream outright. A codec that consumed a length prefix and then said
        // "need more bytes" while a complete message sat in the buffer would silently drop that
        // message and every one after it. That happens whenever two or more whole frames are
        // buffered when the body ends. So `Ok(None)` is returned only when the buffer genuinely
        // cannot yield anything further.
        loop {
            // The length is checked before the buffer-empty guard, deliberately: a zero-length
            // frame is complete with an empty buffer, and returning "need more bytes" for one
            // would drop it.
            let Some(expected_len) = self.cursor.expected_len else {
                if buf.is_empty() {
                    return Ok(None);
                }
                match read_varint(buf)? {
                    // Made progress: go round again, the body may already be buffered.
                    Some(len) => {
                        self.cursor.expected_len = Some(len as usize);
                        continue;
                    }
                    // A partial varint. This one really does need more bytes.
                    None => return Ok(None),
                }
            };

            if expected_len > self.max_length {
                return Err(StreamError::new(
                    StreamErrorKind::MaxLenReachedError,
                    None,
                    Some("Max object length reached".into()),
                ));
            }

            if buf.len() < expected_len {
                return Ok(None);
            }

            let obj_bytes = buf.copy_to_bytes(expected_len);
            self.cursor.expected_len = None;
            return prost::Message::decode(obj_bytes)
                .map(|item| Some(Ok(item)))
                .map_err(|err| {
                    StreamError::new(StreamErrorKind::CodecError, Some(Box::new(err)), None)
                });
        }
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, StreamError> {
        self.decode(buf)
    }
}

/// Reads a LEB128 varint from the front of `buf`, consuming it.
///
/// Returns `Ok(None)` when the buffer holds only part of one, leaving `buf` untouched so the
/// next call can retry with more bytes.
fn read_varint(buf: &mut BytesMut) -> Result<Option<u64>, StreamError> {
    let bytes = buf.chunk();
    if bytes.is_empty() {
        return Ok(None);
    }

    // The single-byte case is both the overwhelmingly common one and the only one that needs
    // no lookahead at all.
    if bytes[0] < 0x80 {
        let value = u64::from(bytes[0]);
        buf.advance(1);
        return Ok(Some(value));
    }

    // `decode_varint_slice` requires either a full 10 bytes or a visible terminator; without
    // one of those the varint is genuinely incomplete.
    if bytes.len() > 10 || bytes[bytes.len() - 1] < 0x80 {
        let (value, advance) = decode_varint_slice(bytes)?;
        buf.advance(advance);
        return Ok(Some(value));
    }

    Ok(None)
}

/// This function is copied from Prost, since it is not available as public API yet optimized for performance.
///
/// Decodes a LEB128-encoded variable length integer from the slice, returning the value and the
/// number of bytes read.
///
/// Based loosely on [`ReadVarint64FromArray`][1] with a varint overflow check from
/// [`ConsumeVarint`][2].
///
/// ## Safety
///
/// The caller must ensure that `bytes` is non-empty and either `bytes.len() >= 10` or the last
/// element in bytes is < `0x80`.
///
/// [1]: https://github.com/google/protobuf/blob/3.3.x/src/google/protobuf/io/coded_stream.cc#L365-L406
/// [2]: https://github.com/protocolbuffers/protobuf-go/blob/v1.27.1/encoding/protowire/wire.go#L358
#[inline]
fn decode_varint_slice(bytes: &[u8]) -> Result<(u64, usize), StreamError> {
    // Fully unrolled varint decoding loop. Splitting into 32-bit pieces gives better performance.

    // Use assertions to ensure memory safety, but it should always be optimized after inline.
    assert!(!bytes.is_empty());
    assert!(bytes.len() > 10 || bytes[bytes.len() - 1] < 0x80);

    let mut b: u8 = bytes[0];
    let mut part0: u32 = u32::from(b);
    if b < 0x80 {
        return Ok((u64::from(part0), 1));
    };
    part0 -= 0x80;
    b = bytes[1];
    part0 += u32::from(b) << 7;
    if b < 0x80 {
        return Ok((u64::from(part0), 2));
    };
    part0 -= 0x80 << 7;
    b = bytes[2];
    part0 += u32::from(b) << 14;
    if b < 0x80 {
        return Ok((u64::from(part0), 3));
    };
    part0 -= 0x80 << 14;
    b = bytes[3];
    part0 += u32::from(b) << 21;
    if b < 0x80 {
        return Ok((u64::from(part0), 4));
    };
    part0 -= 0x80 << 21;
    let value = u64::from(part0);

    b = bytes[4];
    let mut part1: u32 = u32::from(b);
    if b < 0x80 {
        return Ok((value + (u64::from(part1) << 28), 5));
    };
    part1 -= 0x80;
    b = bytes[5];
    part1 += u32::from(b) << 7;
    if b < 0x80 {
        return Ok((value + (u64::from(part1) << 28), 6));
    };
    part1 -= 0x80 << 7;
    b = bytes[6];
    part1 += u32::from(b) << 14;
    if b < 0x80 {
        return Ok((value + (u64::from(part1) << 28), 7));
    };
    part1 -= 0x80 << 14;
    b = bytes[7];
    part1 += u32::from(b) << 21;
    if b < 0x80 {
        return Ok((value + (u64::from(part1) << 28), 8));
    };
    part1 -= 0x80 << 21;
    let value = value + ((u64::from(part1)) << 28);

    b = bytes[8];
    let mut part2: u32 = u32::from(b);
    if b < 0x80 {
        return Ok((value + (u64::from(part2) << 56), 9));
    };
    part2 -= 0x80;
    b = bytes[9];
    part2 += u32::from(b) << 7;
    // Check for u64::MAX overflow. See [`ConsumeVarint`][1] for details.
    // [1]: https://github.com/protocolbuffers/protobuf-go/blob/v1.27.1/encoding/protowire/wire.go#L358
    if b < 0x02 {
        return Ok((value + (u64::from(part2) << 56), 10));
    };

    // We have overrun the maximum size of a varint (10 bytes) or the final byte caused an overflow.
    // Assume the data is corrupt.
    Err(StreamError::new(
        StreamErrorKind::CodecError,
        None,
        Some("invalid varint".into()),
    ))
}
