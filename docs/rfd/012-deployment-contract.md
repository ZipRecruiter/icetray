# Deployment contract

> Updated 2026-10-04: disk residency without a local directory is
> refused with the configuration, and the doctor runs as a pod of its own
> from the serving image rather than inside a serving pod.

> Updated 2026-10-04: request counts carry the status code each call
> answered, the gRPC layer's own answers included, the calls in flight
> show beside their bound, and the series an alert keys on are exposed
> from the start.

> Updated 2026-10-20: `scan` joins the subcommands, reading one key's
> rows of an ordered table as `get` reads keys of a key-value table, and
> a located table can name its order column. See RFD 025.

> Updated 2026-10-30: the memory limit is set first and the cache
> budget, by default a share of it, follows. The sizing rule counts the
> largest table's resident bytes once more in place of doubling each
> table a refresh can rewrite, and icetray checks it against the limit
> once the tables load, warning of a shortfall and holding the bytes
> missing in a gauge exposed from the start. See RFD 028.

## Problem

icetray is open source and meant to be run by organizations other than
the one that wrote it. Each has its own way of handing a process its
configuration, injecting credentials, shipping logs, scraping metrics, and
deciding when a replica may take traffic. If any of those conventions is
written into the source, a second deployer has to edit it to run icetray,
and the project keeps one deployer for good.

An operational surface can also be wrong without failing, and the
prototype showed two ways. A credential bridge that resolved credentials on
every GET put a helper process on the request path
([prototype findings, finding 11](../analysis/2026-09-10-prototype-findings.md#11-without-caller-side-caching-the-sdk-credential-chain-spawns-the-credential-helper-on-every-get)).
A snapshot id exported as a floating-point gauge lost its low digits
([prototype findings, finding 14](../analysis/2026-09-10-prototype-findings.md#14-metrics)).

## Decision

icetray will be operable through a twelve-factor surface alone, as one
binary whose subcommands are the server and the operator's tools, and will
carry no deployer-specific code. The surface is a configuration file path
from a flag or environment variable, credentials from the platform's
default chain or mounted files, JSON logs with a configurable field-name
map and static fields, Prometheus metrics with latency histograms,
plaintext gRPC behind the platform's transport security, and gRPC health
with a readiness endpoint.

Every proposed addition is tested by asking whether a second organization
could deploy the result without editing source. Upstream accepts shapes,
such as a path, a chain, or a field-name map. A deployer supplies the
values in its own wrapper: a container image, built from the published one
or from source, that carries its configuration and log field names.

Configuration is a file, located by a flag or an environment variable, in
either syntax RFD 011 (Configuration) accepts, so a deployer whose pipeline
emits JSON hands the file over as is. A running server follows a change to
the file's tables without a restart (RFD 018 (Configuration reload)).

`icetray serve` runs the server; `doctor` (RFD 016 (Doctor)), `get`,
`scan`, and `info` are subcommands of the same binary with the same flags
for where tables come from. The image a deployer runs therefore carries the tool that
explains a refusal. An operator runs it as a pod of its own from that
image, with the daemon's configuration and credentials, rather than inside
a serving pod, where a full load would compete with the server for CPU and
memory and share its local directory.

Credentials come from the platform's default chain or from mounted files,
and the storage layer of RFD 003 (One storage stack) resolves and caches
them, so a deployer never has to supply a secret format of its own.

The listener speaks plaintext gRPC. icetray expects to run on a private
network, or behind a service mesh or sidecar that encrypts traffic and
authenticates callers, and it leaves TLS termination, certificates,
credential stores, and access policy to that layer.

A deployer may mount a local directory; what lives in it, and why it needs
no backup, is RFD 017 (Residency tiers). A replica without one can serve
only the in-memory tiers. A configuration that sets a table to disk
residency without one is invalid under RFD 011 (Configuration): `serve`
refuses it before anything is opened, and a reload refuses it whole
(RFD 018 (Configuration reload)).

Logs are JSON events. A field-name map in configuration renames the
standard fields (time, level, message, target) to the deployer's schema,
and static fields from configuration are added to every event.

Metrics are Prometheus, and they cover:

- request counts by call and by the gRPC status code each call answered,
  so errors show on the server's side. The count is taken around the gRPC
  service, so a call the gRPC layer answers before any handler runs, such
  as one whose body does not decode, is counted too, once; a call dropped
  before it answers, as at its client's deadline, counts as cancelled;
- the calls in flight beside `server.max_in_flight`, the bound past which
  a call is refused, both as gauges, so saturation shows before the
  refusals do;
- GET counts, local read counts, and latency histograms by kind (load,
  lookup), read from the request counter of RFD 003 (One storage stack);
- lookup latency by table and by result (cold hit, warm hit, miss);
- hedge counts;
- the resident index's live bytes per table, beside process memory,
  because a residency tier that sheds bytes does not show up in process
  memory on every platform;
- per table, the serving snapshot's sequence number and when it was
  swapped in.

A series an alert keys on is exposed from the start, at zero for a
counter, so the alert has data before the first event rather than only
after it: request counts of every call under every status code, lookups
of every table and result, swaps of every table, reloads of every result,
the page cache's fetches, hits, and bytes, the cache budget's shortfall
(RFD 028 (Cache budget)), and the in-flight gauges. A table a reload adds
gets its series when it is added. Refresh errors are the
exception: their reason has a value per error kind and refusal, too many to
expose per table, so a refresh error's series appears with the first
failure, and the table's degraded state, exposed from the start, is the
series to alert on.

The snapshot id itself goes in logs and the table-info reply and stays out
of metrics. As a gauge value it loses digits
([prototype findings, finding 14](../analysis/2026-09-10-prototype-findings.md#14-metrics)),
and as a label it adds a series with every swap.

The gRPC listener and its health service come up before any table has
loaded. Readiness turns true only once every configured table serves its
first generation, so an orchestrator can tell a replica that is still
starting from one that is broken, and route traffic to neither.

The build carries its own protobuf compiler, so building from source needs
none on the build host.

A conformance run in CI starts the binary the way an orchestrator would
and checks each element of the surface: logs parse with the mapped names,
metrics scrape, health answers before load completes, and readiness flips
after.

## Alternatives Considered

Deployer-specific hooks upstream, such as a secret envelope format or a
house log schema. Rejected by the test above, since each hook is one
organization's convention carried by every other deployer.

A fork per deployer. Rejected: a fork stops tracking upstream, and then it
dies.

Plain-text logs. Rejected because every platform ingests JSON, while a text
format needs a parser per deployer.

TLS and authentication inside the server. Rejected. Every platform already
has a mesh or a sidecar that does both, with its own certificate rotation
and identity, and a second implementation inside the process is one more
thing to configure and get wrong.

Separate daemon and tool binaries. Rejected. Two binaries bring two flag
vocabularies and two configuration paths that drift apart, and a serving
image that lacks the tool that diagnoses it. The saving is a few megabytes
of linked code in a tool that prints JSON.

## Consequences

A deployment without a mesh needs a sidecar or a private network; the port
is never safe to expose directly.

A replica runs under a memory limit its deployer sets first, and the
cache budget, by default a share of the limit, follows it (RFD 028
(Cache budget)). The limit holds the doctor's predicted resident bytes
for each table's tier, the largest table's once more since a refresh
that rewrites it wholesale loads its next generation beside the serving
one, plus the cache budget, plus a margin of a third of that total for
the process, requests in flight, a load's transient chunks, and the
allocator's fragmentation and bookkeeping. Once the tables load, icetray
checks that sum against the limit and, when it does not fit, logs a
warning and holds the bytes missing in a gauge. A quarter was the first
margin and proved too small: with the page cache full, the process held
about a third more than the cache and index together, most of it
allocator overhead on the cache's small pages.

The limit backs up the load checks without replacing them, catching the
allocations they do not cover. RFD 004 (Resident key-chunk index)
refuses a key page header at load, and a value page header at lookup,
that declares more than its page can hold or more than a ceiling that
does not rest on the footer, but the lengths inside a delta-encoded page
still size buffers from the file. A file that trips the limit takes down
every replica that loads it, because all of them load the same snapshot
and restart into the same file. A limit alone would turn one bad file
into a fleet-wide crash loop, which is why the cases the checks know are
refused instead and, on a refresh, the last good generation keeps
serving. A panic inside parquet's decoding of a file would crash-loop
the fleet the same way, so the index catches it and refuses the file as
a fault too.

Conveniences a given platform expects, such as its metric naming, its
secret injection, or any log structure beyond renamed fields, are the
wrapper's job, and the wrapper lives outside this repository. That makes
the surface a public contract: a metric or log field renamed upstream
breaks every wrapper.

Readiness covers the window while tables load. A table that fails its first
load makes the process exit with a nonzero status, per the start-or-fail
rule of RFD 005 (Generations and refresh), after the reason has been logged
and exposed on the health service long enough to be scraped. The
orchestrator therefore sees a restart with a logged refusal, rather than a
replica that is alive and never ready. RFD 005 leaves open whether
per-table lifecycle states with a degraded state should replace
start-or-fail, and readiness inherits the question. For a degraded state:
a replica serving nine tables of ten could take traffic for the nine.
Against: a ready replica that cannot answer for one table moves the
failure from the orchestrator, which can hold traffic, to clients, which
see errors.
