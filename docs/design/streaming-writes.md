# Bounded streaming writes

Decision status: **provisional -- no elapsed incubation**

## Top story

`Parquet.write_rows` and `Parquet.write_columns` must accept a finite Ruby
enumeration whose total size is not known in advance, preserve its logical row
sequence exactly, and write a valid Parquet file while keeping native live
memory independent of the total number of rows. A path becomes observable only
after the complete stream and Parquet footer have been written successfully.

This decision replaces total-input materialization and total-column
accumulation with pull-based enumeration into one private, bounded writer.

## Problem and closure

Facts in the previous implementation:

- both public entry points called `to_a`, retaining the entire input;
- the column path retained a second converted copy of every value;
- `flush_threshold` observed encoded memory only after an admitted row batch;
- column lengths were validated after batches had been concatenated, allowing
  mismatches in one batch to cancel mismatches in another;
- a destination path was truncated before enumeration and encoding succeeded.

The closure is the path from Ruby enumeration through schema inference,
validation, Ruby-to-native conversion, bounded row admission, Arrow/Parquet
encoding, destination publication, and regression/scale evidence for both
public write entry points. Readers, repacking, a public paging API, new
platform-specific publication behavior, and unrelated cleanup are non-goals.

## Meaning and laws

Let `Rows` be a finite ordered sequence of schema-valid logical rows. A row
write denotes `Rows`. A column batch with equally sized columns denotes the
rows produced by zipping those columns in schema order; a column stream denotes
the concatenation of its batch denotations.

Successful observation is a Parquet file whose decoded rows equal `Rows` in
order. Row batches, byte quanta, Arrow batches, and Parquet row groups are
implementation structure and are not observable semantics.

The implementation must satisfy:

- **chunking:** encoding `a ++ b` is observationally equivalent to encoding
  `a` followed by `b` in the same writer;
- **order:** no internal flush reorders rows or fields;
- **column-batch validity:** every yielded batch is rejected unless all columns
  have the schema width and equal row count;
- **empty input:** an explicit schema may encode an empty file; schema inference
  from empty input fails;
- **singleton progress:** a row larger than the byte quantum is written alone,
  rather than rejected or retained with later rows;
- **commit:** for path output, failure before publication preserves an existing
  destination and leaves an absent destination absent.
- **property refinement:** internal one-row-group segments may remove only the
  row-group row and byte limits from caller properties. Any per-column value
  derived from those limits is resolved and frozen first; rebuilding segment
  properties must not silently change compression, encoding, statistics, or
  bloom-filter sizing.

On Unix, path preparation happens before the first input pull. Existing
symlinks continue to name their resolved target rather than being replaced as
directory entries. Existing targets must be writable and must have one hard
link; a multiply-linked
inode is rejected before input is consumed because atomic replacement cannot
preserve all of its aliases. The staging inode receives the existing target's
uid, gid, and mode before input is consumed; failure to reproduce them fails
preflight. They are reapplied and verified after encoding closes the stage and
before publication, because writes may clear special mode bits. Publication is
one same-directory atomic rename. Existing target
names therefore use the filesystem's standard last-committer-wins semantics and
require a stable namespace for callers that need exclusion; portable POSIX has
no compare-by-inode rename. An absent target is published with no-clobber
semantics. Atomic replacement creates a new inode, so extended attributes and
ACLs are outside this contract. These rules preserve ordinary Unix ownership,
write authorization, and the symlink boundary while keeping failure publication
atomic. They do not add a new non-Unix support contract.

The API is partial for malformed/infinite enumerations and values outside the
declared schema. Ruby exceptions from enumeration remain Ruby exceptions.
Generic Ruby IO cannot promise rollback if the IO itself fails during the final
copy; enumeration and encoding still finish in a temporary file before that
copy starts.

## Resource model

The core writer owns the native row buffer and the Arrow writer. Its controls
are:

| Resource | Owner | Unit and bound | Progress invariant |
| --- | --- | --- | --- |
| Ruby input | adapter | one yielded row or one yielded column batch | each `next` is consumed once |
| converted rows | core writer | at most `batch_size` rows and normally at most `flush_threshold` conservatively charged retained bytes; one oversized row is allowed alone | admission either appends one row or flushes a nonempty buffer |
| per-column slots | core writer | normally at most 1,000,000 slots across the configured row quantum; one complete row for a wider schema | a flush clears all live values |
| size samples | core writer | at most `sample_size`, maximum 10,000 `usize` values | reservoir replaces an existing slot |
| string cache | adapter converter | caller-selected entries, additionally capped by existing entry/value-byte budgets | misses do not create an unbounded side table |
| encoded row group | core writer | Ruby builder: `max(flush_threshold, 8 MiB)` target bytes; lower-level custom properties: the exact caller target; both have one Arrow batch/oversized value of slack | crossing the target closes one independently staged row group |
| footer metadata | core writer | one row group's metadata in memory; completed thrift metadata is serialized immediately to a disk spool | each completed row group releases its heap metadata; close streams the spool once |
| staged files | filesystem | output-sized destination stage, footer-metadata spool, and one bounded row-group stage | successful close advances to one atomic path publication or bounded IO copy |

`flush_threshold` is the converted-value memory quantum, not a semantic file-size
limit. The locked parquet-rs 58.3.0 writer and reader assign `i16` row-group
ordinals and reject a 32,769th group. Parquet row groups therefore have an
independent 8 MiB minimum target: equating a one-byte caller quantum with a
one-row group would exhaust that backend envelope after only 32,768 rows. The
next group is rejected before it is opened if a lower-level caller explicitly
forces the backend maximum. Peak writer-owned native live memory is a constant
multiple of `max(flush_threshold, 8 MiB)` plus schema state, the bounded string
cache, bounded sample/slot tables, Arrow conversion scratch, and one oversized
logical row. Completed footer metadata lives on disk rather than accumulating
on the heap, so heap use does not grow with the number of row groups or total
row count. Caller-owned memory already retained inside one yielded Ruby object,
a caller-provided core output sink, or an in-memory Ruby output object is
outside native ownership, but the Ruby adapter always encodes to a disk staging
file and never retains earlier yielded objects.

Transformation schedule:

1. Pull one input item; retain it only until it is validated and converted.
2. Validate row/batch shape before converting any contained values.
3. Admit converted rows to the single core buffer; flush before an admission
   that would cross the byte quantum when the buffer is nonempty.
4. Convert one bounded buffer to Arrow arrays, encode it, and clear converted
   values. At the independent row-group target, close the group into a bounded
   disk stage, copy its data forward, and serialize its adjusted thrift metadata
   to a disk spool before releasing all group heap state. Page-location offsets
   inside each offset index are adjusted and streamed alongside the copied
   payload.
5. Stream the metadata spool into the footer, then publish the staged path or
   stream-copy the staged file to the requested IO with fixed-size
   standard-library buffers.

Disk transport failures while serializing the metadata spool or footer remain
operating I/O errors. Thrift protocol/application failures remain internal
encoding errors. Neither class publishes a staged path.

## Alternatives and decision

| Approach | Memory | Semantic surface | Failure/lifecycle complexity | Decision |
| --- | --- | --- | --- | --- |
| Materialize with `to_a` | proportional to total input | existing | low, but incorrect | reject |
| Pull through Ruby `Enumerator#next` | independent of total input | private adapter change | small and explicit | select |
| Push through a captured Ruby block into native state | independent of total input | callback/lifetime state | higher GVL, exception, and ownership coupling | defer |
| Expose a public incremental native writer handle | independent of total input | new stateful public protocol | cancellation/close misuse becomes public | reject for this closure |
| Spool Ruby values before conversion | independent of RAM | new serialization format and extra IO | type fidelity and recovery machinery | reject |

The pull design is the smallest whole solution. Its credible performance risk is
one Ruby `next` boundary per yielded row or column batch. The acceptance evidence
therefore includes representative elapsed-time and peak-RSS measurements; a
future push-loop optimization is justified only if those measurements show the
boundary is material and it preserves this denotation.

The Parquet assembly decision is separate:

| Approach | Completed-group heap | Data movement | Knowledge burden | Decision |
| --- | --- | --- | --- | --- |
| Standard `ArrowWriter`, current quanta | proportional to row-group count | one output write | low, but not bounded | reject |
| Standard `ArrowWriter`, only enlarge row groups | still proportional to row-group count | one output write | low, but delays rather than removes growth | reject |
| One temporary row group plus disk-spooled metadata | one row group | one bounded staging copy per group | custom offset/footer refinement | select |
| Direct page-level Parquet encoder | controllable | one output write | duplicates substantially more parquet-rs machinery | reject |

The selected representation keeps one private owner for the converted columns,
open row group, metadata spool, and output position. Its extra disk copy is the
cost of retaining Arrow's tested encoding while removing its file-lifetime
metadata retention. It makes `thrift` 0.17 a direct dependency because the
locked parquet crate does not expose a transport that can interleave the
disk-spooled row-group bytes with the remaining footer fields. That exact crate
and version were already present transitively, so this adds no package or
transitive dependency.
`tempfile` was already a direct development dependency and moves to runtime to
own the row-group and metadata stages with automatic cleanup on every exit.

## Adversarial review and acceptance

The strongest objections are a single huge row, a huge caller-created column
batch, nested variable-sized values, a late enumerator exception, a malformed
middle column batch, Arrow retaining encoded state, file-level metadata growing
per row group, and publication failure. The design answers these with
singleton-row progress, row-at-a-time conversion inside each batch, complete
nested-value byte accounting, per-batch validation, bounded row-group staging,
disk-spooled footer metadata, and staged publication.

Acceptance requires:

- enumerables whose `to_a` raises write correctly for rows and columns;
- malformed column batches fail at that batch and do not publish output;
- late enumeration/conversion/footer failure preserves the destination;
- crossing row and byte quanta preserves decoded rows exactly;
- every finite chunk partition in the model produces the same complete decoded
  row sequence through row and column entry points;
- tiny converted-value quanta do not create one Parquet row group per row;
- rebuilding internal segment properties preserves resolved per-column bloom
  sizing;
- multiple disk-spooled row groups preserve data and page-index offsets;
- metadata-spool and footer write failures remain I/O failures and do not
  advance the observable output;
- Unix path preflight preserves symlink identity, uid/gid/mode, and write
  authorization, rejects hard-linked targets before pulling input, and uses
  no-clobber publication for an initially absent destination;
- representative small/large streaming runs show peak RSS reaches a plateau
  rather than scaling with total rows;
- focused Rust and Ruby tests, formatting, linting, and the repository test
  suite pass, or each omitted gate has a concrete reason.

## Verification evidence

On arm64 macOS with parquet-rs 58.3.0, the write-only many-row-group test used
13,680,640 bytes maximum RSS for 2,000 groups, 15,056,896 bytes for 16,000
groups, and 17,907,712 bytes at the backend maximum of 32,768 groups. These are
whole-process measurements from `/usr/bin/time -l`; the 16-fold increase in
completed groups added 4,227,072 bytes rather than retaining every group's heap
metadata.

The slow Ruby regression wrote both row and column streams from 20,000 through
80,000 1 KiB values and kept post-GC RSS growth below 32 MiB. A four-row-group
file produced through `Parquet.write_rows` was also read by DuckDB 1.5.2 with
the exact 24-row count, ID range, payload-length bounds, and group count. The
five-row finite model exhaustively covered all 16 chunk partitions at 1, 32,
and 8,192-byte quanta through both core write forms. Injected metadata-spool and
footer transport failures remained I/O errors without advancing output. The
warnings-as-errors Rust workspace suite and the full Ruby suite passed; the
Ruby suite covered 227 tests and 233,994 assertions. The opt-in RSS suite also
passed both 80,000-row plateau checks on the integrated repair, and the
write-only backend-boundary regression completed all 32,768 row groups. The
documented public write contract was smoke-tested across every accepted
compression spelling, row and column schema inference, nil return values, and
staged `StringIO` output.
