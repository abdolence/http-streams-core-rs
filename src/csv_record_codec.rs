//! Framing CSV records.
//!
//! Built directly on `csv-core` rather than on line splitting, for three reasons:
//!
//! 1. **Correctness.** A CSV field may contain a newline when quoted, and this crate's own
//!    encoder emits exactly that. Splitting on `\n` truncates such a field and yields the
//!    remainder as a bogus record: silent data loss on the row that *did* decode.
//! 2. **Allocation.** `csv::ReaderBuilder::from_reader` allocates an 8 KiB buffer, so parsing
//!    one record at a time through it allocates 8 KiB per row.
//! 3. **Types.** This framer is not generic over the item type. A decoder that structurally
//!    mentions `T` forces a `T: 'b` bound onto every caller's public signature; deserialisation
//!    therefore happens in a separate step, where `T` appears only as a return type.

use crate::error::{StreamError, StreamErrorKind};
use bytes::{Buf, BytesMut};
use csv_core::{ReadRecordResult, Reader as CoreReader};
use tokio_util::codec::Decoder;

/// Initial size of the field and offset buffers; both grow on demand.
const INITIAL_FIELDS: usize = 512;
const INITIAL_ENDS: usize = 16;

/// How to frame CSV records.
#[derive(Debug, Clone, Copy)]
pub struct CsvFrameConfig {
    /// Field delimiter.
    pub delimiter: u8,
    /// Quote character.
    pub quote: u8,
    /// Whether a doubled quote character is an escaped quote.
    pub double_quote: bool,
    /// Escape character, if escaping is by prefix rather than by doubling.
    pub escape: Option<u8>,
    /// Record terminator.
    pub terminator: csv_core::Terminator,
}

impl CsvFrameConfig {
    fn build(&self) -> CoreReader {
        csv_core::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .quote(self.quote)
            .double_quote(self.double_quote)
            .escape(self.escape)
            .terminator(self.terminator)
            .build()
    }
}

/// A [`Decoder`] that yields one [`csv::ByteRecord`] per CSV record.
///
/// Not generic over the item type: see the module docs.
#[derive(Debug)]
pub struct CsvRecordCodec {
    core: CoreReader,
    output: Vec<u8>,
    ends: Vec<usize>,
    outlen: usize,
    endlen: usize,
    header_pending: bool,
    max_len: usize,
}

impl CsvRecordCodec {
    /// A framer reading records per `config`, skipping a leading header row if `has_headers`.
    pub fn new(config: CsvFrameConfig, has_headers: bool, max_len: usize) -> Self {
        Self {
            core: config.build(),
            output: vec![0; INITIAL_FIELDS],
            ends: vec![0; INITIAL_ENDS],
            outlen: 0,
            endlen: 0,
            header_pending: has_headers,
            max_len,
        }
    }

    /// Builds the finished record and resets the accumulators for the next one.
    fn take_record(&mut self) -> csv::ByteRecord {
        let mut fields: Vec<&[u8]> = Vec::with_capacity(self.endlen);
        let mut start = 0;
        for &end in &self.ends[..self.endlen] {
            fields.push(&self.output[start..end]);
            start = end;
        }
        let record = csv::ByteRecord::from(fields);
        self.outlen = 0;
        self.endlen = 0;
        record
    }

    /// One pass of the framing loop, mirroring `csv`'s own reader.
    ///
    /// `at_eof` says whether the caller may signal end of input, which `csv-core` recognises as
    /// an empty input slice and which is the only way to flush a final record that has no
    /// trailing terminator.
    fn next_record(
        &mut self,
        buf: &mut BytesMut,
        at_eof: bool,
    ) -> Result<Option<csv::ByteRecord>, StreamError> {
        loop {
            let input_was_empty = buf.is_empty();
            if input_was_empty && !at_eof {
                return Ok(None);
            }

            let (res, nin, nout, nend) = self.core.read_record(
                &buf[..],
                &mut self.output[self.outlen..],
                &mut self.ends[self.endlen..],
            );

            buf.advance(nin);
            self.outlen += nout;
            self.endlen += nend;

            // Counts the offset vector as well as the field bytes. `csv-core` records one end
            // offset per field whether or not that field wrote any bytes, so a record like
            // `a,,,,,,,...` keeps `outlen` near zero while `ends` grows without ever tripping a
            // bytes-only check.
            let record_bytes = self
                .outlen
                .saturating_add(self.endlen.saturating_mul(std::mem::size_of::<usize>()));
            if record_bytes > self.max_len {
                return Err(StreamError::new(
                    StreamErrorKind::MaxLenReachedError,
                    None,
                    Some("Max record length reached".into()),
                ));
            }

            match res {
                // Not a whole record yet. When the body has ended, going round again passes an
                // empty slice, which is how `csv-core` is told there will be no more input.
                ReadRecordResult::InputEmpty => {
                    if input_was_empty {
                        return Ok(None);
                    }
                    continue;
                }
                ReadRecordResult::OutputFull => {
                    self.output.resize(self.output.len().saturating_mul(2).max(1), 0);
                    continue;
                }
                ReadRecordResult::OutputEndsFull => {
                    self.ends.resize(self.ends.len().saturating_mul(2).max(1), 0);
                    continue;
                }
                ReadRecordResult::Record => {
                    let record = self.take_record();
                    // The header slot is consumed here rather than by a `.skip(1)` downstream:
                    // skipping the first *yielded* item would swallow a header that failed to
                    // frame, and the stream would report itself as cleanly completed.
                    if self.header_pending {
                        self.header_pending = false;
                        continue;
                    }
                    return Ok(Some(record));
                }
                ReadRecordResult::End => return Ok(None),
            }
        }
    }
}

impl Decoder for CsvRecordCodec {
    /// A framed record, not yet deserialised. Errors here are framing errors and are terminal.
    type Item = csv::ByteRecord;
    type Error = StreamError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, StreamError> {
        self.next_record(buf, false)
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, StreamError> {
        self.next_record(buf, true)
    }
}
