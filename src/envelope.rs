//! Wrapping a streamed array in a surrounding JSON object.

/// An object to wrap a streamed JSON array in.
///
/// The array is emitted as `array_field` of `object`, so
/// `StreamFormatEnvelope { object: Meta { total: 3 }, array_field: "items" }` produces
/// `{"total":3,"items":[…]}`.
///
/// Only one level of nesting is supported: the array is always a direct field of the envelope
/// object. This is a consequence of how the framing works: the envelope is serialised once,
/// its trailing `}` is stripped, and the array is appended. It is not something a deeper path
/// could be threaded through.
#[derive(Debug, Clone)]
pub struct StreamFormatEnvelope<E> {
    /// The object to wrap the array in.
    pub object: E,
    /// The field of `object` that the array is emitted as.
    pub array_field: String,
}
