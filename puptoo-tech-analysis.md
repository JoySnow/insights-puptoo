# Puptoo Tech Stack & Performance Analysis

## 1. Tech Stack

| Category | Technology |
|---|---|
| **Language** | Python 3.11 |
| **Message Queue** | Kafka via `confluent-kafka` (C-based client with Python bindings) |
| **Core Library** | [`insights-core`](https://github.com/RedHatInsights/insights-core) — the fact extraction framework that defines all parsers, combiners, and the `@rule` decorator system |
| **Build System** | Poetry |
| **Container Base** | UBI9 (Red Hat Universal Base Image 9), with multi-stage production Dockerfiles |
| **Deployment** | OpenShift via Clowder Operator (`app-common-python` for config injection) |
| **Object Storage** | S3-compatible (MinIO for dev, S3 for prod) |
| **In-memory** | Redis (retry tracking, max 3 attempts with 1hr TTL) |
| **Monitoring** | Prometheus client (standalone HTTP server on configurable port) |
| **Logging** | Logstash JSON formatter + CloudWatch via watchtower |
| **Testing** | pytest + freezegun + insights-core's `InputData`/`run_test` utilities |
| **Linting** | flake8 |
| **CI/CD** | GitHub Actions + Konflux Hermetic Builds (prefetch RPM/Python deps with SBOM generation) |

---

## 2. CPU-bound or I/O-bound?

**I/O-bound, with moderate CPU during archive decompression.**

The per-message pipeline and its bottleneck profile:

| Step | Bottleneck | Weight |
|---|---|---|
| Kafka poll (wait) | Network I/O | ★★ (waiting) |
| HTTP download archive from S3 | Network I/O | ★★★ (largest cost) |
| gzip decompression + tar extraction | CPU + Disk I/O | ★★☆ |
| Walk extracted dir for size check | Disk I/O | ☆☆☆ (trivial) |
| insights-core parser execution | CPU (regex/string) + Disk I/O (read files) | ★☆☆ |
| Postprocess facts (dict ops, MAC regex) | CPU | ☆☆☆ (trivial) |
| Upload yum_updates to S3 | Network I/O | ★☆☆ |
| Kafka produce | Network I/O | ★☆☆ |

Key observations:

- The **archive download** is the single most expensive step — full tar.gz fetched over HTTP before anything starts
- **gzip decompression** is the main CPU cost, but it releases the GIL (runs in C), so it doesn't block Python
- The parsers themselves are lightweight string processing — no heavy computation
- The service processes one message at a time; the next message waits until the previous one fully completes

---

## 3. Would asyncio / multithreading / multiprocessing help?

**Bottom line: Not meaningfully for this workload on OpenShift.** The current sequential single-threaded design with horizontal pod scaling is the right approach.

### Why each pattern doesn't add much value

**asyncio:**
- The pipeline is strongly sequential — you can't start extracting until the download finishes, and you can't parse until extraction finishes. One message = one linear chain.
- `confluent-kafka` Python client doesn't support async natively (its I/O runs in internal C threads already).
- insights-core's `extract()` and `run()` are synchronous — would need to be offloaded to an executor, defeating much of async's benefit.
- *Potential small gain*: Prefetch the next archive's HTTP download while the current one is being parsed. But this adds complexity and race-condition surface for marginal throughput gain on a single pod.

**multithreading:**
- Python GIL limits CPU parallelism (decompression would release GIL, but string parsing in parsers wouldn't).
- `confluent-kafka` already handles produce/consume I/O in its C threads — no help there.
- Would introduce thread-safety concerns: the shared `producer` global, Prometheus metrics counters, and logging ContextualFilter are not thread-safe today.
- *Potential small gain*: Run `upload_object()` (S3 upload of yum_updates) in a background thread while returning the main response. But this is an optimization of a cheap step (< 1% of total time).

**multiprocessing:**
- Achieves true parallelism, bypassing GIL — but the per-message CPU demand is too low to justify it.
- On OpenShift, a pod typically gets 0.5–2 CPU cores — very limited headroom for forking child processes.
- Adds significant complexity: shared Kafka consumer coordination, IPC for results, error handling across processes.
- Horizontal scaling (more pod replicas) is simpler and achieves the same throughput improvement without any of this complexity.

### What actually improves throughput on OpenShift

| Approach | Effort | Impact |
|---|---|---|
| **More pod replicas** (HPA based on Kafka consumer lag) | Low | High — linear throughput scaling |
| **More Kafka partitions** (more parallel consumers) | Medium | High — enables the above |
| **Larger `max.poll.interval.ms`** (reduce consumer rebalances) | Trivial | Medium — avoids processing delays |
| **Compression tuning** (faster gzip level on Kafka produce) | Trivial | Low |
| **HTTP streaming download** (avoid loading full archive into memory) | Medium | Low — memory savings, not throughput |

### Why CPU limits kill multiprocessing on OpenShift

OpenShift enforces CPU limits via CFS (Completely Fair Scheduler). When a pod has a CPU limit of, say, `1 core`, the kernel caps all processes in that pod to 1 second of CPU time per second — across **both parent and child processes combined**.

```yaml
# Typical puptoo pod resource spec
resources:
  requests:
    cpu: 500m      # 0.5 core guaranteed
  limits:
    cpu: 1         # hard cap at 1 core
```

With a 1-core limit:
- Forking 1 child → 2 processes competing for 1 core → **no throughput gain**
- Forking 4 children → 5 processes fighting over 1 core → **net negative** (context switching overhead)

The entire argument for multiprocessing is that it uses more CPU cores in parallel. If the pod only has 1 core, there's nothing to parallelize onto:

```
Single process:
  |--- download ---|--- decompress ---|--- parse ---|     throughput = 1 msg/unit_time
  CPU: ███░░░░░░░░   CPU: ████████░░░   CPU: ██░░░░░

Two processes on 1-core limit (throttled):
  |--- download ---|--- decompress ---|--- parse ---|     still ~1 msg/unit_time
  |--- download ---|--- decompress ---|--- parse ---|     (but each takes longer due to
  CPU: ██░██░██░██░  CPU: ████░████░░   CPU: █░░█░░      time-slicing)
```

Parent and child just time-slice on the same single core. You get the overhead of IPC and context switching without any wall-clock speedup.

#### What would change this

For multiprocessing to help, each child process would need its **own dedicated CPU**:

```yaml
limits:
  cpu: 4    # 4 cores for 1 pod
```

Then 1 parent + 3 children = 4 processes on 4 cores = actual parallelism. But:
- Puptoo's archive processing isn't CPU-bound enough to saturate even 1 core (the bottleneck is network download)
- Requesting 4-core pods wastes cluster resources that other services could use
- It's far simpler to run 4 single-core pod replicas than 1 four-core pod with multiprocessing

#### The OpenShift-native alternative visualized

```
Approach A (multiprocessing in 1 pod):
  ┌──────────────────────────────┐
  │ Pod with 4 CPUs              │
  │  ├─ Process 1: process msg A │  ← IPC overhead
  │  ├─ Process 2: process msg B │  ← shared state problems
  │  ├─ Process 3: process msg C │  ← complex error handling
  │  └─ Process 4: process msg D │  ← consumer group coordination
  └──────────────────────────────┘

Approach B (4 pod replicas — OpenShift way):
  ┌──────────────────────────────┐
  │ Pod 1: process msg A         │  ← stateless, simple
  ├──────────────────────────────┤
  │ Pod 2: process msg B         │  ← each independent
  ├──────────────────────────────┤
  │ Pod 3: process msg C         │  ← Kafka consumer group
  ├──────────────────────────────┤     handles partition
  │ Pod 4: process msg D         │     distribution
  └──────────────────────────────┘
```

Approach B wins because:
- No code changes needed — the existing sequential code just works
- Kafka consumer groups handle partition distribution automatically
- Each pod is independently deployable, scalable, and debuggable
- No shared state, no IPC, no GIL concerns
- OpenShift's HPA (Horizontal Pod Autoscaler) can scale based on consumer lag

### Conclusion

The current architecture already aligns well with how OpenShift is meant to work: each pod is stateless and processes one message at a time; you scale by adding replicas. The main I/O bottleneck (archive download) is constrained by network bandwidth, not Python's execution model. Concurrency patterns within a single pod would add complexity without meaningful throughput gains on typical OpenShift pod resource limits.

---

## 4. Pod CPU Sizing Practices (General OpenShift/Kubernetes)

### 4.1 Mainstream Pod CPU Configuration

There's no single number — it depends entirely on workload type — but the patterns cluster around three tiers:

| Workload Type | Typical CPU Request | Typical CPU Limit | Reasoning |
|---|---|---|---|
| **Web APIs / microservices** (stateless, request-reply) | 100m–500m | 500m–2 | Light per-request CPU; scale via replicas |
| **Background workers / message consumers** (like puptoo) | 200m–1 | 500m–2 | I/O-bound, one message at a time |
| **Batch / data processing** | 1–4 | 2–8 | Varies with job size |

The rough industry norm for most pods is **0.5–2 CPU cores**. Cloud providers' default node types (e.g., AWS `m6i.xlarge` = 4 vCPUs, `m6i.2xlarge` = 8 vCPUs) reinforce this — you want multiple pods per node, not one pod consuming a whole node.

The Kubernetes documentation and most cluster operators recommend: **smaller pods, more replicas** — it improves utilization, spread, and resilience.

### 4.2 Is 8 CPU Cores Good for a CPU-bound Pod?

**Yes, for CPU-bound projects, assigning 8 cores can be a good approach.** But the critical detail is that you should also properly set *both* `requests` and `limits`:

```yaml
resources:
  requests:
    cpu: 8    # 8 cores guaranteed
  limits:
    cpu: 8    # 8 cores max — no throttling
```

**Why it makes sense for CPU-bound workloads:**
- The workload fully utilizes each core — you get 8x the work of a single core (minus diminishing returns from Amdahl's law)
- Parallelism libraries (Golang goroutines, Python multiprocessing, Java virtual threads) actually pay off
- You avoid the throttling overhead that comes from having a low limit

**Risks to manage:**
- **Blast radius**: A single pod failure loses 8 cores of work — spread across 8 single-core pods loses only 1/8
- **Node packing**: An 8-core pod can only schedule on nodes with ≥8 free cores, limiting placement options
- **Bin packing efficiency**: Smaller pods leave less stranded capacity on partially-filled nodes
- **Startup cost**: Large pods may face longer scheduling delays waiting for sufficient capacity

**Common practice for CPU-bound services:**
- **Data processing/ETL**: 4–8 cores is common — often job-level parallelism
- **ML inference**: 2–4 cores per model replica (multi-model serving is better than one big pod)
- **Game servers / media transcoding**: 4–16 cores, matching the workload's natural thread count
- **Databases**: 4–16+ cores (but databases are usually StatefulSets with different considerations)

**Rule of thumb**: The larger the pod, the stronger the justification needs to be. 8 cores is fine for CPU-bound work. 16+ cores starts raising eyebrows unless it's a database or a very specific computation.

---

## 5. AI Agent Pod Sizing on OpenShift

For AI agents, the answer depends entirely on **where the LLM runs**:

### Scenario A: Agent calls an external LLM API (most common)

The agent itself is a lightweight orchestrator — it constructs prompts, calls an API (OpenAI, Anthropic, etc.), and processes the response. The expensive compute happens on the API provider's side.

| Resource | Typical | Notes |
|---|---|---|
| **CPU request** | 100m–500m | Tokenization, tool call orchestration, context management |
| **Memory** | 128Mi–512Mi | Prompt templates, conversation history, tool outputs |
| **GPU** | None | All inference is remote |

This is essentially an **I/O-bound API client** — identical profile to puptoo or any other message consumer. The bottleneck is network latency to the LLM API.

### Scenario B: Local LLM inference in the pod (resource-intensive)

Running a model on OpenShift is a completely different story:

| Model Size | CPU Only | GPU Required | Memory |
|---|---|---|---|
| 1–3B params (SLM) | 2–4 cores | Optional | 2–8 GB |
| 7–8B params | 8–16 cores (very slow) | Strongly recommended (1 GPU) | 16–32 GB |
| 13–70B params | Impractical | 1–4 GPUs | 32–140+ GB |
| 100B+ params | Impossible | 4–8 GPUs | 200+ GB |

**CPU-only local inference is generally not practical** for anything beyond very small models (1–3B params) running at low throughput. A 7B model on CPU can take 30–60 seconds per token — unusable for interactive agents.

### Scenario C: Agent with RAG (vector DB + LLM API)

The agent orchestrates retrieval and calls an external LLM — same as Scenario A. The vector DB might be:
- An external service (Pinecone, Weaviate, pgvector) — no local resource cost
- A sidecar container — typically needs 200m–1 CPU and 256Mi–1Gi memory

### Real-world AI Agent Deployments

What most teams actually do:

```
┌─────────────────────────────────────────────┐
│ Pod: AI Agent                                │
│                                              │
│  CPU: 200m–500m    Memory: 256Mi–512Mi       │
│                                              │
│  Flow:                                        │
│    receive request → build prompt             │
│    → call Anthropic/OpenAI API (remote)       │
│    → execute tool calls                       │
│    → return response                          │
└──────────────────────────────────────────────┘
         │
         ▼  (network call)
┌──────────────────────────────────────────────┐
│ External LLM API (OpenAI / Anthropic / etc.)  │
│ (or in-cluster: ModelMesh + GPU node)         │
└──────────────────────────────────────────────┘
```

**Typical AI Agent pod spec:**

```yaml
resources:
  requests:
    cpu: 250m
    memory: 256Mi
  limits:
    cpu: 1
    memory: 512Mi
```

Unless you're running the LLM itself in the pod, the compute requirement is very modest. The latency comes from LLM API calls, not from your agent's CPU.

### Bottom Line for AI Agents on OpenShift

| Scenario | CPU | Memory | GPU | Notes |
|---|---|---|---|---|
| Agent + external LLM API | 250m–1 | 256Mi–512Mi | None | I/O-bound orchestrator |
| Agent + local SLM (1–3B) | 2–4 | 2–8 GB | Optional | Slow on CPU-only |
| Agent + local LLM (7B+) | N/A | N/A | 1+ GPU required | Use GPU Operator + node selector |
| Agent + RAG (external vector DB) | 250m–1 | 256Mi–1Gi | None | Same as Scenario A |

If your AI agent calls an **external LLM API**: 0.25–1 CPU, 256–512Mi memory — same as any lightweight microservice. The pod is an I/O-bound orchestration wrapper.

If you're **hosting the LLM in-cluster**: that's a GPU workload, and the agent pod itself should still be lightweight — the model runs in a separate pod/container using Node Feature Discovery + GPU Operator to target GPU nodes.
