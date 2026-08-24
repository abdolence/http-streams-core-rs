[![Cargo](https://img.shields.io/crates/v/http-streams-core.svg)](https://crates.io/crates/http-streams-core)
![tests and formatting](https://github.com/abdolence/http-streams-core-rs/workflows/tests%20&amp;%20formatting/badge.svg)
![security audit](https://github.com/abdolence/http-streams-core-rs/workflows/security%20audit/badge.svg)

# HTTP streams core for Rust

Library provides framework-neutral building blocks for streaming HTTP bodies as sequences of
items:
- JSON array stream format
- JSON lines stream format
- CSV stream
- Protobuf len-prefixed stream format
- Arrow IPC stream format
- Raw text stream format

It holds the parts that are the same whichever HTTP library is in use: the wire formats
(encoding and decoding), the error type, and the progress accounting. It knows nothing about
any specific client or server.

You are unlikely to depend on this crate directly. It backs:
- [axum-streams](https://github.com/abdolence/axum-streams-rs) for server support
- [reqwest-streams](https://github.com/abdolence/reqwest-streams-rs) for client support

Both re-export the types they expose, so downstream code names them through those crates rather
than here.

## Why this crate exists

It was extracted from [axum-streams](https://github.com/abdolence/axum-streams-rs) and
[reqwest-streams](https://github.com/abdolence/reqwest-streams-rs), which had grown up as a
pair: one encoding response bodies on the server, the other decoding them on the client. Adding
streaming *request* bodies meant each crate needed what the other already had, an encoder on
the client and a decoder on the server, and the two had by then also grown near-identical
progress and error handling independently of one another.

Rather than duplicate either half, the shared parts moved here. Both crates became thin
bindings over it, and a body encoded by one is now decoded by the same code in the other.

## Quick start

Cargo.toml:
```toml
[dependencies]
http-streams-core = { version = "0.1", features=["json", "csv", "protobuf", "arrow", "text"] }
```

Example code:
```rust
use futures::stream;
use http_streams_core::*;
use http_streams_core::format::StreamFormatEncode;
use serde::Serialize;

#[derive(Serialize)]
struct MyTestStructure {
    some_test_field: String
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let format = JsonArrayStreamFormat::new();

    let items = stream::iter(vec![
        Ok(MyTestStructure { some_test_field: "test".to_string() })
    ]);

    // The encoder is generic over the item type, so name it here.
    let encoder = StreamFormatEncode::<MyTestStructure>::encoder(&format);
    let _bytes = encode_stream(items, encoder);

    Ok(())
}
```

All the examples are available in both dependent crates, which is where the ergonomic API
lives.

## Formats

Every format can encode. All but `text` can decode: text framing writes each string's bytes
with no delimiter at all, so `["ab", "c"]` and `["a", "bc"]` produce identical bytes and
splitting them back into items is not merely unimplemented but impossible.

| Format                     | Encode | Decode | Content-Type                          |
|----------------------------|--------|--------|---------------------------------------|
| JSON array                 | yes    | yes    | `application/json`                    |
| JSON lines                 | yes    | yes    | `application/jsonstream`              |
| CSV                        | yes    | yes    | `text/csv`                            |
| Protobuf (len-prefixed)    | yes    | yes    | `application/x-protobuf-stream`       |
| Apache Arrow IPC           | yes    | yes    | `application/vnd.apache.arrow.stream` |
| Text                       | yes    | no     | `text/plain; charset=utf-8`           |

The `default` features do not include any formats, so enable the ones you need.

## Framing and parsing

Decoding is split in two, and the split is load-bearing rather than cosmetic.

A *framer* finds record boundaries. Its errors are terminal: a framer that has lost track of
where records begin cannot resynchronise, and `FramedRead` ends the stream.

A *parser* turns one framed record into an item. Its errors are not terminal, and the stream
continues with the next record.

So a JSON lines body with one unparseable line reports that line and carries on, while a JSON
array with one unparseable element stops, because its elements are not independently framed.

Keeping the framer free of the item type also matters for the API of the crates built on this
one: a decoder that structurally mentions `T` forces a `T: 'b` bound onto every caller's public
signature, whereas a parser mentioning `T` only in its return type does not.

## Observing errors and progress

With the `tracing` feature every stream reports on an `http_streams_core::stream` span, on the
`http_streams_core` target. The `direction` and `side` span fields tell apart the four cases of
client or server, request or response body.

At `INFO` a stream reports its totals once, when it ends:

```text
INFO http_streams_core::stream{format="json_array" direction="response" side="client" status=200 items=1000 bytes=28001 errors=0 elapsed_ms=11239 outcome="completed"}: Finished streaming an HTTP body
```

The `outcome` tells apart the three ways a stream can end: `completed`, `aborted` (the other
end went away early, which is otherwise invisible), and `failed`, which reports at `ERROR`
instead. A stream that was built but never polled reports nothing at all.

Raise the filter to `http_streams_core=debug` for periodic progress, and to `trace` for an
event per chunk. The same accounting is available without tracing, for metrics, through
`ProgressOptions`.

The counters are only maintained when someone is listening, so a stream with no callbacks and
no interested subscriber pays nothing for this.

## Licence
Apache Software License (ASL)

## Author
Abdulla Abdurakhmanov
