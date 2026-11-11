# FinOSWorkflow — Design & Developer Guide

**Version:** 1.0.0  
**Platform:** Blackline FinOS Orchestration Platform  
**Runtime:** FinOS Workflows Engine  
**Spec:** [`finos-workflow-api.yaml`](./finos-workflow-api.yaml)  
**Worked Example:** [§12 — Invoice Approval Workflow](#12-end-to-end-example-invoice-approval-workflow)

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Canonical Workflow YAML Format](#2-canonical-workflow-yaml-format)
   - 2.1 [Inputs](#21-inputs)
   - 2.2 [Authentication](#22-authentication)
3. [Node Type Reference](#3-node-type-reference)
   - 3.0 [FinOSStart — Explicit Workflow Entry Point](#30-finosstart--explicit-workflow-entry-point)
   - 3.1 [FinOSAgent — A2A Protocol](#31-finosagent--a2a-protocol)
   - 3.2 [FinOSAPI — REST Invocation](#32-finosapi--rest-invocation)
   - 3.3 [FinOSHITL — Human-in-the-Loop](#33-finoshitl--human-in-the-loop)
   - 3.4 [FinOSPolicy — Policy Evaluation](#34-finospolicy--policy-evaluation)
   - 3.5 [FinOSWorkflow — Child Workflow Invocation](#35-finosworkflow--child-workflow-invocation)
   - 3.6 [FinOSTerminal — Explicit Workflow Terminal](#36-finosterminal--explicit-workflow-terminal)
   - 3.7 [FinOSFanOut — Parallel Child-Workflow Fan-Out](#37-finosfanout--parallel-child-workflow-fan-out)
   - 3.8 [FinOSAsyncAPI — Long-Running Callback/Event](#38-finosasyncapi--long-running-callbackevent)
4. [Edge Routing](#4-edge-routing)
5. [Runtime State Representation](#5-runtime-state-representation)
6. [REST API — FinOSWorkflow CRUD](#6-rest-api--finosworkflow-crud)
7. [REST API — Node CRUD](#7-rest-api--node-crud)
8. [REST API — Edge CRUD](#8-rest-api--edge-crud)
9. [DAG Integrity Rules](#9-dag-integrity-rules)
10. [HITL Pause & Resume Pattern](#10-hitl-pause--resume-pattern)
11. [Running a Workflow](#11-running-a-workflow)
12. [End-to-End Example: Invoice Approval Workflow](#12-end-to-end-example-invoice-approval-workflow)
13. [FinOSAsyncAPI Callback/Event Resume Pattern](#13-finosasyncapi-callbackevent-resume-pattern)

---

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│                   Blackline FinOS Orchestration Platform                  │
│                                                                            │
│  FinOSWorkflow API  ──────────────────────────────────────────────────┐  │
│  (CRUD + Run)                                                           │  │
│       │                                                                 │  │
│       ▼                                                                 │  │
│  FinOSWorkflow DAG  ──── export ────▶  FinOS Workflows YAML           │  │
│  (design-time)                                ▼                        │  │
│       │                          FinOS Workflows Engine          │  │
│       │                                ▼                                │  │
│       │                    ┌──────────────────────┐                    │  │
│       │                    │  FinOSAgent (A2A)        │ FinOS Agent Runtime Agent  │  │
│       │                    │  FinOSAPI (REST)         │ External Systems  │  │
│       │                    │  FinOSHITL (Callback)    │ Human Reviewers   │  │
│       │                    │  FinOSPolicy (Eval)      │ Policy Engine     │  │
│       │                    │  FinOSWorkflow (Child)   │ Sub-Workflows     │  │
│       │                    └──────────────────────┘                    │  │
│       │                                ▼                                │  │
│       └──────────────────  FinOSWorkflowExecutionHistory API  ──────────┘  │
│                            (runtime audit & provenance DAG)               │
└──────────────────────────────────────────────────────────────────────────┘
```

**Key concepts:**

- **FinOSWorkflow** — design-time DAG definition (nodes + directed condition edges)
- **FinOSWorkflow YAML** — the design-time DAG definition (nodes and edges) stored as YAML; consumed directly by the FinOS interpreter
- **DAG Runtime State** — embedded in the workflow as the `dag_state` variable; persisted to `FinOSWorkflowExecutionHistory` after every node
- **FinOSWorkflowExecutionHistory** — the audit trail API that records every event as a CloudEvent-compliant vertex

---

## 2. Canonical Workflow YAML Format

A FinOS workflow definition is a **design-time DAG** stored as YAML. It describes
the logical workflow — which agents to call, what policies to evaluate, where human
review is needed — without any runtime-vendor details.

A conforming implementation stores, validates, and triggers these definitions. Execution
concerns (durability, retries, signals) are invisible to workflow authors.

```yaml
workflowId: wf-my-workflow-v1
workflowName: "My Workflow"
workflowVersion: "1.0.0"
description: "Optional description"

inputs:                          # declare expected inputs; see §2.1
  - name: account_id
    type: string
    required: true
    source: runtime

authentication:                  # OPTIONAL — only for credentials shared by
  platform:                     # more than one node; see §2.2
    oauth2cc:
      grantType: client_credentials
      .
      .

nodes:                           # see §3 for all node types
  - nodeId: ...
    nodeType: ...

edges:                           # see §4 for edge semantics
  - edgeId: ...
    sourceNodeId: ...
    targetNodeId: ...
```

### 2.1 Inputs

Declares all variables the workflow may receive. Nodes access them via
`${ .variable_name }` expressions.

| Field | Required | Description |
|---|---|---|
| `name` | ✅ | Variable name |
| `type` | ✅ | `string`, `number`, `boolean`, `array`, `object` |
| `source` | ✅ | `runtime` = passed at trigger time; `config` = set by tenant admin |
| `required` | — | Default `false`; required inputs missing from both sources reject the trigger |
| `default` | — | Applied when not supplied by caller |
| `description` | — | Human-readable description |

---

### 2.2 Authentication

A node that makes an outbound call says how to authenticate it in its **own
`auth:` block**. That block is the unit of configuration: read one node and you
know the identity it calls with, the grant it uses, and the authority it asks
for. Nothing outside the node is required — the workflow-level
`authentication:` block described in [§2.2.3](#223-sharing-a-credential--the-workflow-level-authentication-block)
is **optional**, and exists only to stop repeating a credential that several
nodes share.

Where `auth` takes effect:

| Node type | `auth` |
|---|---|
| `FinOSAPI`, `FinOSAsyncAPI` | **Honoured** — the resolved credential authenticates the outbound HTTP call. |
| `FinOSAgent` | **Honoured** — for `oauth2RO` this selects the *agent path* (see [§2.2.5](#225-oauth2ro-runtime--mint--exchange)). |
| `FinOSPolicy`, `FinOSHITL` | **Not honoured.** These call FinOS control-plane services, which the engine authenticates as *itself* under a fixed least-privilege scope no author can choose or redirect. An `auth` block on such a node parses but has no effect. |
| `FinOSStart`, `FinOSWorkflow`, `FinOSFanOut`, `FinOSTerminal` | No outbound call of their own; no `auth`. |

A node with no `auth` block — or `auth: none` — is called **unauthenticated**.

> **Secret values never live in the YAML — this is a normative requirement, not
> an implementation choice.** The credential blocks use standard OAuth2 property
> names (`tokenUrl`, `clientId`, `clientSecret`, …), but a conforming engine
> **MUST NOT** resolve these fields from workflow-authored input in a way that
> lets the same input both select which secret is used *and* choose where it is
> sent — doing so lets a workflow definition exfiltrate a credential to an
> attacker-chosen endpoint. In practice this means the fields carry an opaque,
> platform-recognized reference (a name, not an expression and not a raw value),
> and the platform maps that reference to a real secret/endpoint through its own
> configuration, refusing any reference it does not recognize.
>
> The exact reference vocabulary and how it resolves are **platform
> configuration**, out of scope for this spec. The names in the examples below
> (`SAP_TOKEN_URL`, `RATES_CLIENT_SECRET`, …) are a worked illustration of the
> *shape* — an opaque name the platform recognizes — not spec-defined constants;
> a different conforming implementation will recognize different names. This keeps
> a workflow definition portable across tenants and environments and safe to
> commit.
>
> *(`apiKey`'s `value` is not a sentinel and not an `${ENV_VAR}` reference: it is a
> **workflow-context expression** — e.g. `${ .api_key }` — resolved from workflow
> data with environment access disabled, so it can never read a process-env secret.
> See [`apiKey`](#apikey--static-header-key) below.)*

#### 2.2.1 Start at the node

Put the credential's fields **directly on the node's `auth:` block**. There is no
category subkey and no `schemeRef`: the **category** is identified by the fields
present, and `grantType` is what identifies the two OAuth ones.

| Category | Identified by | Standard | Acts as |
|---|---|---|---|
| `oauth2cc` | `grantType: client_credentials` | OAuth 2.0 client-credentials (RFC 6749 §4.4) | the **service itself** (service-to-service) |
| `oauth2RO` | `grantType: urn:ietf:params:oauth:grant-type:token-exchange` | OAuth 2.0 Token Exchange (RFC 8693) — resource owner / OBO | the **resource owner** (the user), instance-scoped |
| `apiKey` | `header` + `value` (no `grantType`) | — (non-OAuth) | a **static key** sent in a request header |

The vocabulary is closed and **case-sensitive** — `oauth2cc`, `oauth2RO`,
`apiKey`. Anything else is a permanent configuration error, never a retry.

```yaml
nodes:
  # (1) A platform API, called as the service itself.
  - nodeId: fetch-ledger
    nodeType: FinOSAPI
    endpoint: "${ .finos_base_url + \"/ledgers/\" + .account_id }"
    httpMethod: GET
    auth:
      grantType: client_credentials     # → oauth2cc
      scopes: [bl.finos.read]           # tokenUrl/clientId/clientSecret omitted →
                                        # the platform's own service credential (§2.2.2)

  # (2) The same API, called as the USER the run belongs to.
  - nodeId: fetch-user-ledger
    nodeType: FinOSAPI
    endpoint: "${ .finos_base_url + \"/ledgers/mine\" }"
    httpMethod: GET
    auth:
      grantType: urn:ietf:params:oauth:grant-type:token-exchange   # → oauth2RO
      scopes: [bl.finos.read]

  # (3) A third-party endpoint with its own platform-registered credential.
  - nodeId: fetch-quote
    nodeType: FinOSAPI
    endpoint: "https://rates.example.com/v1/quote"
    httpMethod: GET
    auth:
      grantType: client_credentials
      tokenUrl: RATES_TOKEN_URL         # a recognized URL reference
      clientId: RATES_CLIENT_ID         # recognized credential references
      clientSecret: RATES_CLIENT_SECRET
      scopes: [rates.read]

  # (4) An endpoint that takes a static key rather than OAuth.
  - nodeId: ingest-results
    nodeType: FinOSAPI
    endpoint: "${ .finos_base_url + \"/ingest\" }"
    httpMethod: POST
    auth:
      header: X-Api-Key                 # → apiKey (no grantType, no scopes)
      value: ${ .api_key }              # workflow-context expression, NOT a secret
```

Node (1) is the shortest useful block in the model, and worth reading twice: a
`grantType` and the scopes it wants. It names **no system**, so it resolves to
the **platform's own system** and takes that system's default endpoint and service
credential. An inline block can only ever have meant the platform it runs on —
that is the only system whose credentials the engine holds — so omitting the
endpoint and credential fields is the intended shorthand, not an omission to be
inferred around. See
[§2.2.6](#226-what-the-platforms-own-system-reserves) for what that default does
and does not grant.

#### 2.2.2 Credential categories and their fields

##### `grantType` is mandatory for both OAuth categories

Every `oauth2cc` / `oauth2RO` block **MUST** declare `grantType` — on the node,
or on the scheme it references ([§2.2.3](#223-sharing-a-credential--the-workflow-level-authentication-block)),
either is fine. It is the **only** input to the engine's choice of token flow, so
a missing `grantType` is not a harmless omission: it is indistinguishable from
"this node wants no token", and the call would go out anonymous. A definition
that omits it is **rejected at registration time**, before any node runs.

Selection must not be inferred from which credential fields happen to be
populated, either — for `oauth2cc`, an omitted `tokenUrl`/`clientId`/`clientSecret`
is the normal way to take the platform defaults (node (1) above), and those
defaults resolve *inside* the engine at call time. An anonymous call must never
be the result of an omission.

> **Spelling matters.** The key is camelCase `grantType`. `grant_type` is an
> unknown field and is silently ignored — which, on an inline block, leaves
> nothing to identify the category from, and the definition is rejected as an
> unsupported inline `auth` block.

##### `oauth2cc` — client-credentials (RFC 6749 §4.4)

The engine POSTs `grant_type=client_credentials&client_id=…&client_secret=…&scope=<scopes>`
to the token endpoint and sends the returned access token as
`Authorization: Bearer <token>`.

| Field | Required | Standard-OAuth2 meaning |
|---|---|---|
| `grantType` | ✅ | `client_credentials`. Node or scheme — see above. |
| `scopes` | recommended | The scopes this node requests (least privilege). Omit only if the authorization server assigns a default. |
| `tokenUrl` | conditional | The OAuth2 token endpoint, as a recognized URL reference. **Omit for the platform's own system** → the platform supplies its own endpoint. **Required** for any other system, which must name a reference the platform recognizes. |
| `clientId` | conditional | OAuth2 `client_id`, as a recognized credential reference. Same omit/require rule as `tokenUrl`. |
| `clientSecret` | conditional | OAuth2 `client_secret`, as a recognized credential reference. Same omit/require rule as `tokenUrl`. |

> A client-credentials token authenticates the **service**, not a user, so it
> carries no tenant/instance context of its own. For an instance-scoped platform
> API the engine therefore supplies the run's instance out-of-band on the
> downstream call, in whatever form that platform's APIs require — which header
> or parameter carries it is platform configuration. A node that sets that field
> itself keeps its own value.

**Token reuse.** A client-credentials token authenticates the service and is
reusable until it expires, so an engine **MAY** cache it — keyed on **endpoint +
client id + scope set**, and never for longer than the authorization server's own
`expires_in`. Keying on all three matters: the endpoint encodes the tenant, so a
token minted for one tenant is never served to another, and a narrowly-scoped
token is never handed to a call that asked for different authority. Cache
lifetimes are platform configuration.

##### `oauth2RO` — resource owner / OBO (RFC 8693)

> **Terminology.** In OAuth 2.0 the **resource owner** is the user. `oauth2RO`
> acts *as* that user without ever handling the user's password — it is
> **OAuth 2.0 Token Exchange (RFC 8693)**, not the deprecated
> Resource-Owner-Password-Credentials grant.

| Field | Required | Standard-OAuth2 meaning |
|---|---|---|
| `grantType` | ✅ | `urn:ietf:params:oauth:grant-type:token-exchange`. Node or scheme. |
| `scopes` | recommended | The scopes this node requests for the user (least privilege). |
| `userId` | — | **Node-level only.** The resource owner for this node; omit to use the run's own user. See [§2.2.5](#225-oauth2ro-runtime--mint--exchange). |

`oauth2RO` declares **no endpoint and no credential fields**. Both stages run
against the platform's own token service under a privileged minting identity that
is never nameable from a workflow — so there is nothing for an author to point at,
and a `tokenUrl` / `clientId` / `clientSecret` on an `oauth2RO` block is ignored.
The flow is also reserved to the platform's own system
([§2.2.6](#226-what-the-platforms-own-system-reserves)).

The scheme carries **no `userId`**: the resource owner is a per-node concern.
Runtime detail — the two-stage mint + exchange, the workflow and agent paths, and
token lifetime — is in [§2.2.5](#225-oauth2ro-runtime--mint--exchange).

##### `apiKey` — static header key

For an API that does not speak OAuth. `header` is the (non-secret) header name;
`value` is a **workflow-context expression** that resolves to the key. No
`grantType`, no `scopes` — an `apiKey` block never reaches token-flow selection
at all.

| Field | Required | Meaning |
|---|---|---|
| `header` | ✅ | The (non-secret) header name, e.g. `X-Api-Key`. A block with no header name is refused. |
| `value` | ✅ | A workflow-context expression that resolves to the key, e.g. `${ .api_key }`. Literals/constants are rejected; environment access is disabled. |

`value` **must** be a `${ … }` expression evaluated against workflow context —
e.g. `${ .api_key }`, typically a `source: config` input declared in the workflow's
`inputs` block ([§2.1](#21-inputs)) and supplied per tenant at configuration time.
A plain literal or a constant is **rejected**, and the expression is resolved with
**environment access disabled**, so an `apiKey` value can never read a process-env
secret (`${ $ENV.… }` is inert here). The resolved value does enter workflow
history — acceptable because it originates from workflow input, not the engine's
secret environment. For a secret credential held by the deployment, use an OAuth
category (`oauth2cc` / `oauth2RO`) instead, whose secrets never leave the engine.

#### 2.2.3 Sharing a credential — the workflow-level `authentication` block

Inline blocks stop paying once two nodes need the same credential: rotating an
endpoint or a client id means editing every node that named it. Hoist it into the
workflow-level **`authentication`** block, declare it once, and let the nodes
reference it.

`authentication` is a two-level map — *system* → *credential category*:

```yaml
authentication:
  <system>:            # a system you call — the platform itself, or an external one
    <category>:        # oauth2cc | oauth2RO | apiKey
      <fields…>
```

The **system** name is author-chosen (`sapErp`, `ratesVendor`, …) and free-form;
the platform's own system is simply one system among peers, under whatever name
the platform reserves for itself (`platform` in these examples — see
[§2.2.6](#226-what-the-platforms-own-system-reserves)).
The **category** names are the closed vocabulary from
[§2.2.1](#221-start-at-the-node). A system need only declare the categories it
actually offers. The whole block is optional: a workflow whose nodes all declare
their credentials inline has no `authentication` block at all.

A node then names one credential by its `<system>.<category>` path and adds only
what varies per node:

| Reference field | Required | Description |
|---|---|---|
| `schemeRef` | ✅ | Dotted path to a credential — `<system>.<category>`, e.g. `platform.oauth2cc`. |
| any scheme field | — | Overrides that field on the referenced scheme — `scopes`, `grantType`, `tokenUrl`, `clientId`, `clientSecret`, `header`, `value`. See [§2.2.4](#224-field-resolution--node-overrides-scheme). |
| `userId` | — | `oauth2RO` only; has no scheme-level counterpart. |

**Before** — three nodes, one repeated credential:

```yaml
nodes:
  - nodeId: fetch-invoices
    nodeType: FinOSAPI
    auth:
      grantType: client_credentials
      tokenUrl: SAP_TOKEN_URL
      clientId: SAP_CLIENT_ID
      clientSecret: SAP_CLIENT_SECRET
      scopes: [sap.invoices.read]

  - nodeId: fetch-vendors             # …same four fields again
    nodeType: FinOSAPI
    auth:
      grantType: client_credentials
      tokenUrl: SAP_TOKEN_URL
      clientId: SAP_CLIENT_ID
      clientSecret: SAP_CLIENT_SECRET
      scopes: [sap.vendors.read]
```

**After** — declared once, referenced twice:

```yaml
authentication:
  sapErp:
    oauth2cc:
      grantType: client_credentials
      tokenUrl: SAP_TOKEN_URL
      clientId: SAP_CLIENT_ID
      clientSecret: SAP_CLIENT_SECRET
      scopes:                          # a scheme-level default; a node may replace it
        - sap.invoices.read

nodes:
  - nodeId: fetch-invoices             # inherits the scheme's scopes
    nodeType: FinOSAPI
    auth: { schemeRef: sapErp.oauth2cc }

  - nodeId: fetch-vendors              # same identity, its own authority
    nodeType: FinOSAPI
    auth: { schemeRef: sapErp.oauth2cc, scopes: [sap.vendors.read] }
```

Both nodes now share one identity with per-node least privilege, and the endpoint
and client id are rotated in one place.

`scopes` is the one scheme field written as a YAML **list** rather than a scalar;
the engine collapses it to the space-delimited form the OAuth2 `scope` parameter
expects, which is the same form it builds from a node's own list — so scheme-level
and node-level `scopes` are directly interchangeable. An already-delimited string
is kept as-is.

> **`schemeRef: jwt_passthrough` and the `bearer` category are not supported.**
> Both are rejected outright rather than quietly ignored. Use `platform.oauth2RO`
> (OBO) for any node that needs to act as the user.

#### 2.2.4 Field resolution — node overrides scheme

`schemeRef` and node-level fields are **not** mutually exclusive. Every field on a
node's `auth:` block **overrides** the same field on the referenced scheme;
un-overridden fields fall through to the scheme. A node's effective credential is
therefore the scheme with the node's fields merged on top — node wins, field by
field.

```yaml
authentication:
  platform:
    oauth2cc:
      grantType: client_credentials
      scopes: [bl.finos.read]

nodes:
  - nodeId: read-ledger
    nodeType: FinOSAPI
    auth: { schemeRef: platform.oauth2cc }              # → scopes: bl.finos.read

  - nodeId: write-ledger
    nodeType: FinOSAPI
    auth: { schemeRef: platform.oauth2cc,
            scopes: [bl.finos.write] }                   # → scopes: bl.finos.write
```

Two consequences are worth stating outright:

- **`scopes` replaces, it does not add.** A node that declares any scope replaces
  the scheme's set outright — `write-ledger` above requests `bl.finos.write` and
  *not* `bl.finos.read`. This keeps a node's effective authority readable from the
  node alone. A node that declares none inherits the scheme's, so a workflow can
  state its scopes once per system+category.
- **The effective `grantType` decides the category**, overriding the `schemeRef`
  suffix. A node referencing `platform.oauth2cc` but overriding `grantType` with
  the token-exchange URN resolves as `oauth2RO` and runs the OBO flow. The shape
  of the credential and the flow that consumes it are chosen by the same value, so
  the two can never disagree.

An **inline** block has nothing to inherit from. Resolving its *system* to the
platform's own ([§2.2.1](#221-start-at-the-node)) does not make it reference that
system's scheme: scheme fields are read only through an explicit `schemeRef`. An
inline block that omits `tokenUrl`/`clientId`/`clientSecret` takes the platform's
built-in defaults, not whatever `authentication.platform.oauth2cc` happens to say.

#### 2.2.5 `oauth2RO` runtime — mint + exchange

An `oauth2RO` credential gives a node a short-lived token scoped to **exactly one
instance, one user (the resource owner), and one node's scopes**. Conceptually
this is a **two-stage** process against the platform's token-exchange endpoint:

1. **Mint** — the engine, authenticated as itself under a privileged,
   narrowly-held grant, asserts the run context (which user, which workflow
   instance) and receives a short-lived credential good for nothing except the
   exchange step.
2. **Exchange** — that credential is redeemed for a **node token**: a normal
   `Authorization: Bearer` credential for the resource API, scoped to the
   node's requested scopes.

**Standards posture.** The **exchange** (stage 2) is standard OAuth 2.0 Token
Exchange (RFC 8693): `grant_type=urn:ietf:params:oauth:grant-type:token-exchange`,
following the RFC 8693 delegation pattern (the `sub` stays the real user; actors
are recorded in `act`). The **mint** (stage 1) has no real subject token to
exchange — the engine only ever has a `user_id`, not a token issued to that
user — so a conforming platform must supply *some* platform-specific mechanism
to bridge that gap (for example: a custom grant against its own token service,
or a self-issued signed assertion presented directly as the RFC 8693
`subject_token`). This spec does not mandate which. The mint mechanism, its
grant/scope names, and how its credentials are provisioned are **platform
configuration** — see the platform's own deployment documentation.

**Two paths — derived from node type, never declared:**

| Path | Node types | Who mints / redeems | `actor_type` |
|---|---|---|---|
| workflow | `FinOSAPI`, `FinOSAsyncAPI` | The engine both mints **and** redeems the credential itself, then calls the resource API with the resulting Bearer node token. | `workflow` |
| agent | `FinOSAgent` | The engine mints a credential bound to the agent's registry `actor_id` and hands it to the agent over the A2A request's metadata channel; the agent redeems it with its **own** registered client and calls downstream. No `Authorization` header is set by the engine on the agent call. | `agent` |

On the agent path, the agent's `actor_id` is auto-resolved from the Agent Registry
using the node's `agentId`. There is **no `path` field**: the path follows from the
node type.

**`userId` — three forms.** For any `oauth2RO` node, on either path, `userId`
selects the resource owner:

| Form | Behaviour |
|---|---|
| **Omitted** | The engine acts as the **run's user** — the run-context user supplied at kickoff, inherited unchanged by child workflows. |
| **Explicit numeric value** — e.g. `userId: "48213"` | Pins this node to that user. |
| **Config variable** — e.g. `userId: "${ .approver_user_id }"` | Resolves from a `source: config` input declared in the workflow's `inputs` block ([§2.1](#21-inputs)) and supplied per tenant at configuration time. |

Whatever the source, the resolved user must still have active **consent** for
`(user, workflow, instance)` at the exchange stage, so `userId` narrows *who* the
node acts as but never bypasses authorization. `userId` is read **only** when the
effective grant type is the token-exchange URN — on any other category it is
inert, and is neither an error nor a silent identity change.

**What the platform supplies.** Beyond the credential block and the node's
`scopes` / optional `userId`, everything else is injected at runtime:

- **Run context:** `run_id`, `workflow_id`, and the tenant/instance from the live
  execution; `user_id` from `userId` or the run context.
- **Agent `actor_id`:** from the registry via the node's `agentId` (agent path).
- **Minting credentials:** the privileged minting identity is platform-owned,
  dedicated to this flow, and never referenced by the author — it is **not** the
  general service credential that `oauth2cc` uses. How it is provisioned,
  rotated, and scoped is platform configuration.

**Scope derivation.** The node token's scopes are the intersection of the node's
requested `scopes`, the minting identity's allowed scopes, and consent. A
conforming platform **MUST** enforce consent at the exchange stage; `userId`
alone never bypasses authorization. What consent checks and how it's keyed are
platform configuration.

**Lifetime and caching.** The stage-1 credential may be cached, but only under a
key that pins it to the identity it was minted for — user, run, workflow, scopes,
and (on the agent path) the agent actor. The **exchanged node token MUST NOT be
cached**: stage 2 fails closed and re-runs its full check path — actor binding,
consent, authorization — on every call, so a cache would reuse a token past a
fresh check and widen the window on deprovisioning and consent revocation. The
node token is short-lived and the engine re-mints and re-exchanges transparently
on expiry. No refresh token is ever exposed to the workflow.

#### 2.2.6 What the platform's own system reserves

One system name is **reserved** for the platform itself (`platform` in the
examples here; the actual name is platform configuration). Two things are reserved
to it, and a conforming engine **MUST** refuse both to any other system.

| Reserved to the platform's own system | Why |
|---|---|
| Its own credential and endpoint references — including **by omission**, since an omitted `oauth2cc` field falls back to them | A system that *names itself something else* and then names the platform's credential could POST that credential to its own token endpoint. Confused deputy — refused. |
| The `oauth2RO` / OBO flow in its entirety | Both stages mint credentials that act as one of the platform's own users. Honouring an `oauth2RO` block that names a foreign system would let someone else's scheme drive that impersonation. Refused. |

An **inline** block names no system and resolves to the platform's own
([§2.2.1](#221-start-at-the-node)), so it passes both reservations — and gains
nothing that `schemeRef: <platform-system>.<category>` would not already have
granted. The case the reservations exist to refuse is a scheme that *explicitly
names* a foreign system (`vendorSys.oauth2RO`, or `vendorSys.oauth2cc` reaching
for a platform-owned credential); a named system keeps its name all the way
through resolution and is still refused. An inline block can reach no token
endpoint other than the platform's own, never a YAML-supplied URL.

External systems name their own **registered** URL and credential references
(`SAP_TOKEN_URL`, `VENDOR_API_CLIENT_ID`, …), each of which the platform must be
configured to recognize; nothing external is accepted by default.

#### 2.2.7 Validation and failure modes

Auth misconfiguration is split deliberately: anything decidable from the
definition alone is rejected **at registration/parse time**, so it surfaces before
a single node runs. What remains is refused **at call time** as a permanent
(non-retryable) failure — retrying cannot change the outcome.

**Rejected at parse time — the whole definition:**

| Condition | Message shape |
|---|---|
| An `oauth2cc` / `oauth2RO` block with no effective `grantType`, on the node or its scheme | `auth` category requires a `grantType`… the node would be called unauthenticated |
| An inline block matching no category — no `grantType`, no `clientId`/`clientSecret`, no `header`+`value` (e.g. `grant_type` misspelled) | unsupported inline `auth` block |
| `schemeRef: jwt_passthrough` | removed; use `platform.oauth2RO` |
| `auth:` that is neither a mapping nor `none` | `auth` must be a mapping or 'none' |
| `authentication:` that is not a system → category → fields mapping | `authentication` must be a mapping |

**Refused at call time — the node, non-retryably:**

| Condition | Class |
|---|---|
| A category outside `oauth2cc` / `oauth2RO` / `apiKey` | auth config |
| `apiKey` with no `header`, or a `value` that is not a `${ … }` expression | auth config |
| A platform-owned credential or endpoint reference named by any other system, including by omitting the field ([§2.2.6](#226-what-the-platforms-own-system-reserves)) | auth config |
| An unrecognized credential or URL reference — an unregistered name, a `${ENV_VAR}` ref, or a raw URL/secret | auth config |
| `oauth2RO` on any system other than the platform's own | authorization |
| The platform's dedicated minting credential unavailable | auth config |
| A token endpoint answering 4xx (`invalid_scope`, `invalid_client`, consent refused, …) | token |

Token-endpoint 5xx and `temporarily_unavailable` stay **retryable** — they are the
only auth failures that are.

#### 2.2.8 Example

A workflow that calls the platform two ways (service identity and user identity),
an external system, and a static-key endpoint — with the shared credential hoisted
and the one-off left inline:

```yaml
authentication:                        # only what more than one node shares
  platform:
    oauth2cc:
      grantType: client_credentials    # endpoint + creds omitted → platform defaults
      scopes: [bl.finos.read]
    oauth2RO:
      grantType: urn:ietf:params:oauth:grant-type:token-exchange
                                       # no endpoint/creds; resource owner is per node
  sapErp:
    oauth2cc:
      grantType: client_credentials
      tokenUrl: SAP_TOKEN_URL          # an external system names its own registered
      clientId: SAP_CLIENT_ID          # references — no platform-owned one is
                                       # available to it (§2.2.6)
      clientSecret: SAP_CLIENT_SECRET

nodes:
  - nodeId: fetch-ledger               # (A) platform, service identity
    nodeType: FinOSAPI
    auth: { schemeRef: platform.oauth2cc }

  - nodeId: fetch-subledger            # (B) same identity, own authority
    nodeType: FinOSAPI
    auth: { schemeRef: platform.oauth2cc, scopes: [bl.finos.read.detail] }

  - nodeId: fetch-invoices             # (C) external, service identity
    nodeType: FinOSAPI
    auth: { schemeRef: sapErp.oauth2cc, scopes: [sap.invoices.read] }

  - nodeId: fetch-quote                # (D) one-off — inline, not worth hoisting
    nodeType: FinOSAPI
    endpoint: "https://rates.example.com/v1/quote"
    httpMethod: GET
    auth:
      grantType: client_credentials
      tokenUrl: RATES_TOKEN_URL
      clientId: RATES_CLIENT_ID
      clientSecret: RATES_CLIENT_SECRET
      scopes: [rates.read]

  - nodeId: post-adjustment            # (E) OBO — workflow path, run's own user
    nodeType: FinOSAPI
    auth: { schemeRef: platform.oauth2RO, scopes: [bl.finos.write] }

  - nodeId: recommend-actions          # (F) OBO — agent path, userId override
    nodeType: FinOSAgent
    agentId: agt-recommender-001
    auth:
      schemeRef: platform.oauth2RO
      scopes: [bl.finos.policy]
      userId: "48213"                  # omit → the run's user

  - nodeId: ingest-results             # (G) static API key, inline
    nodeType: FinOSAPI
    auth:
      header: X-Api-Key
      value: ${ .api_key }             # workflow-context expression, NOT a secret

  - nodeId: notify-webhook             # (H) a public endpoint — no credential at all
    nodeType: FinOSAPI
    auth: none
```

> The block above is a complete `authentication` + `auth` configuration: every
> category, both reference styles, and the explicitly-unauthenticated case.

---

## 3. Node Type Reference

> **Common fields:** Every node type accepts `nodeId` (required), `name` (required), `description` (optional), and — for `FinOSAgent`, `FinOSAPI` and `FinOSAsyncAPI` — `auth` (optional; per-node authentication, see [§2.2](#22-authentication)). `FinOSPolicy` and `FinOSHITL` call FinOS control-plane services that the engine authenticates as itself, so they take no `auth`. These fields are omitted from individual field tables below.

---

### 3.0 FinOSStart — Explicit Workflow Entry Point

A `FinOSStart` node marks the entry point of a workflow DAG. Every DAG must
contain **exactly one** `FinOSStart` node, and it must have **zero incoming
edges**.

`FinOSStart` carries **no node-specific fields** — common fields only
(`nodeId`, `name`, `description`). Outgoing edges use the same
condition/priority routing as every other node (§4 Edge Routing).

**Design-time DAG schema:**
```yaml
- nodeId: "start"
  nodeType: "FinOSStart"
  name: "Workflow Entry"
```

**Example — branching on an input parameter before any other node runs:**
```yaml
inputs:
  - name: invoice_amount
    type: number
    required: true
    source: runtime

nodes:
  - nodeId: start
    nodeType: FinOSStart
    name: "Workflow Entry"
  - nodeId: fast-track-review
    nodeType: FinOSHITL
    # ...
  - nodeId: classify-invoice
    nodeType: FinOSAgent
    # ...

edges:
  - edgeId: e-start-fast-track
    sourceNodeId: start
    targetNodeId: fast-track-review
    condition: '${ .invoice_amount < 500 }'
    priority: 1

  - edgeId: e-start-default       # catch-all — everything else goes through full classification
    sourceNodeId: start
    targetNodeId: classify-invoice
    condition: null
    priority: 2
```

For a workflow that doesn't need entry-point branching, `FinOSStart` still
requires exactly one outgoing edge — an unconditional (`condition: null`)
edge to the first real node.

| Field | Required | Description |
|---|---|---|
| — | — | No node-specific fields. Common fields only (`nodeId`, `name`, `description`). |

---

### 3.1 FinOSAgent — A2A Protocol

A `FinOSAgent` node invokes a FinOS Agent Runtime agent using the
[Agent-to-Agent (A2A)](https://a2a-protocol.org/latest/specification/) protocol.

**Design-time DAG schema:**
```yaml
- nodeId: "node-classify-invoice"
  nodeType: "FinOSAgent"
  name: "InvoiceClassifierAgent"
  agentId: "agt-invoice-classifier-001"
  taskInputTemplate:
    invoiceId: "${ .invoice_id }"
    instruction: "Classify the invoice..."
  outputMapping:
    classification_outcome: "${ .task.output.outcome }"
    invoice_amount: "${ .task.output.totalAmount }"
  retryPolicy:
    maxRetries: 3
    initialDelaySeconds: 2
    maxDelaySeconds: 30
    multiplier: 2
```

| Field | Required | Default | Description |
|---|---|---|---|
| `agentId` | ✅ | — | Identifier of the FinOS Agent Runtime agent to invoke. |
| `taskInputTemplate` | — | `{}` | Expression map building the agent task input from workflow context. |
| `outputMapping` | — | `{}` | Maps agent response fields into workflow context variables. |
| `requestContext` | — | `{}` | Key/value string pairs forwarded to the agent as request context. |
| `retryPolicy` | — | see [§3.2.1](#321-retrypolicy-sub-object) | Retry and backoff configuration. |
| `timeoutSeconds` | — | `300` | Max seconds to wait for the agent response. |

---

### 3.2 FinOSAPI — REST Invocation

A `FinOSAPI` node calls any external REST endpoint.

**Design-time DAG schema:**
```yaml
- nodeId: "node-fetch-vendor"
  nodeType: "FinOSAPI"
  name: "VendorDataAPI"
  endpoint: "https://erp-api.example.com/v1/vendors/${ .vendor_id }"
  httpMethod: "GET"
  requestHeaders:
    X-Tenant-Id: "${ .tenant_id }"
  queryParams:
    includeContractTerms: "true"
  responseMapping:
    vendor_tier: "${ .body.tier }"
    vendor_risk_score: "${ .body.riskScore }"
  retryPolicy:
    maxRetries: 3
    initialDelaySeconds: 1
    maxDelaySeconds: 15
    multiplier: 2
```

| Field | Required | Default | Description |
|---|---|---|---|
| `httpMethod` | ✅ | — | HTTP verb: `GET`, `POST`, `PUT`, `PATCH`, or `DELETE`. |
| `endpoint` | ✅ | — | Target URL; may contain `${ }` expressions for path interpolation. |
| `requestHeaders` | — | — | Key/value map of HTTP headers. Values **must be strings** (see §3.2.2). |
| `requestBody` | — | — | JSON request body for `POST`/`PUT`/`PATCH`. A mapping, or a single `${ }` expression that resolves to a JSON value (see §3.2.2). |
| `queryParams` | — | — | Key/value map of query string parameters. |
| `responseMapping` | — | — | Maps response fields into workflow context variables. |
| `retryPolicy` | — | see §3.2.1 | Retry and backoff configuration. |
| `timeoutSeconds` | — | `30` | Max seconds to wait for the HTTP response. |

#### 3.2.1 retryPolicy Sub-Object

Applies to **FinOSAgent** (§3.1) and **FinOSAPI** (§3.2). The key `retryConfig` is accepted as a backward-compatible alias; new YAML should use `retryPolicy`.

| Field | Required | Default | Description |
|---|---|---|---|
| `maxRetries` | — | `3` | Number of additional attempts after the first failure. |
| `initialDelaySeconds` | — | `1.0` | Seconds to wait before the first retry. |
| `maxDelaySeconds` | — | `30.0` | Maximum seconds between retries (caps exponential growth). |
| `multiplier` | — | `2.0` | Exponential backoff coefficient applied after each retry. |
| `nonRetryableErrorTypes` | — | `[]` | Error type names that must not be retried; unioned with platform defaults. An empty list means platform defaults only. |

#### 3.2.2 Request Body & Headers (POST / PUT / PATCH)

For methods that carry a payload (`POST`, `PUT`, `PATCH`), the engine resolves
`requestBody` against the workflow context and serialises the result as the JSON
request body. The following rules are part of the contract — a definition that
violates them either fails validation/execution or sends a malformed payload.

**Request body (`requestBody`)**

The value must resolve to a JSON value — normally a JSON object. Accepted forms:

| Form | Example | Sent as |
|---|---|---|
| Mapping (recommended) | `requestBody:`<br>`  invoiceId: "${ .invoice_id }"`<br>`  amount: "${ .invoice_amount }"` | `{"invoiceId": "...", "amount": ...}` |
| Full-value expression resolving to an object | `requestBody: "${ .prepared_payload }"` | the resolved object |
| No payload | `requestBody: {}` | `{}` |

- **Values may be `${ }` expressions.** Each value in a mapping is resolved
  independently; a value that is itself a full-value `${ }` expression keeps its
  resolved JSON type (object, array, number, boolean), not just a string.
- **A JSON *string* input must be parsed with jq `fromjson`.** If a workflow
  input holds a JSON document as a string (e.g. an input declared
  `type: string`), reference it as `"${ .payload | fromjson }"` so it is sent as
  a JSON object. Referencing it as `"${ .payload }"` sends a **quoted JSON
  string**, not an object.
- **Use `{}` for an empty body — never `''`.** An empty string is not a mapping
  and is not a valid body specification.
- **YAML flow-mapping needs a space after the colon.** `{dummy: 1}` is the
  object `{"dummy": 1}`; `{dummy:1}` (no space) is the single key `"dummy:1"`
  with a null value. Prefer block style to avoid the pitfall.

**Request headers (`requestHeaders`) and query params (`queryParams`)**

- **All header and query-param values must be strings.** Quote any literal that
  YAML would otherwise parse as another type — e.g. `"0"`, `"true"`, `"1.0"`.
  A bare `Content-Length: 0` is an integer and is rejected.
- **Do not set `Content-Length` (or other computed transport headers) by hand.**
  The engine's HTTP client sets them from the actual serialised body; a
  hand-supplied value is at best redundant and at worst wrong.
- Header and query values may contain `${ }` expressions
  (e.g. `Authorization: "Bearer ${ .access_token }"`); the resolved scalar is
  coerced to a string before the request is sent. A value that resolves to
  **null** is omitted (the header/param is not sent), and a value that resolves
  to a **structured type** (object or array) is rejected — headers and query
  params must be scalars.

**Example — `POST` with a JSON-string input parsed to an object:**
```yaml
- nodeId: "node-validate"
  nodeType: "FinOSAPI"
  name: "ValidateRecords"
  endpoint: "${ .validation_api_base_url }/ingestion/validation"
  httpMethod: "POST"
  requestHeaders:
    Authorization: "Bearer ${ .access_token }"
    Content-Type: "application/json"
  requestBody: "${ .validation_request_payload | fromjson }"
  responseMapping:
    records_processed: "${ .records_processed }"
```

---

### 3.3 FinOSHITL — Human-in-the-Loop

A `FinOSHITL` node suspends the workflow until a human submits a decision.
FinOS Workflows implements this via the callback pattern.

**Design-time DAG schema:**
```yaml
- nodeId: "node-hitl-manager"
  nodeType: "FinOSHITL"
  name: "ManagerReview"
  assignedRole: "finance-manager"
  timeoutSeconds: 259200        # 72 hours
  allowedDecisions: [APPROVED, REJECTED]
  escalationPolicy:
    escalateAfterSeconds: 86400
    assignedRole: "finance-director"
  taskContextTemplate:
    invoiceId: "${ .invoice_id }"
    amount: "${ .invoice_amount }"
    policyReason: "${ .policy_reason }"
```

| Field | Required | Default | Description |
|---|---|---|---|
| `assignedRole` | ✅ | — | Role identifier used to route the task to the correct reviewer. |
| `assignedTeamId` | — | — | Specific team identifier to assign the task, bypassing role routing. |
| `assignedUserId` | — | — | Specific user identifier to assign the task directly, bypassing role routing. |
| `timeoutSeconds` | — | `0` | Max seconds to wait for the human callback. `0` = no timeout. |
| `taskInstructions` | — | — | Free-text instructions displayed to the reviewer. |
| `taskContextTemplate` | — | — | Expression map building the context object shown alongside task instructions. |
| `allowedDecisions` | — | `["APPROVED", "REJECTED"]` | List of valid decision strings accepted in the callback payload. |
| `escalationPolicy` | — | — | Auto-escalation config; sub-fields: `escalateAfterSeconds` (int) plus the same assignment fields as the node — `assignedRole`, `assignedTeamId`, `assignedUserId` (at least one target required). |
| `uiComponentId` | — | — | Identifier of the custom UI component rendered for this HITL task. |

**Reviewer callback payload (POST to `callbackUrl`):**
```json
{
  "decision": "APPROVED",
  "reviewerId": "user-jane-doe-001",
  "comments": "Approved — vendor is tier-A, within budget.",
  "reviewedAt": "2026-05-18T10:15:00Z"
}
```

---

### 3.4 FinOSPolicy — Policy Evaluation

A `FinOSPolicy` node evaluates a policy via the FinOSPolicy API. The `outcome`
in the response drives outgoing edge conditions.

Policies use a **two-layer model** (see the [Policy Guide](./4-finos-policy-guide.md)):

- A tenant-agnostic **Policy Definition** publishes the *shape* of the rule —
  the variable catalog and the closed `possibleOutcomes` allow-list — and is
  referenced from a workflow definition by its `policyDefinitionId`.
- Each tenant then authors a **Policy Configuration** at
  `(tenantId, workflowConfigId, nodeId)` that supplies the actual condition and
  enforcement against a pinned Definition version. Configurations are
  authored/saved separately from the workflow definition and are looked up
  transparently at evaluate time.

The workflow node therefore references only the **Definition**, not any
tenant's Configuration:

**Design-time DAG schema:**
```yaml
- nodeId: "node-spending-policy"
  nodeType: "FinOSPolicy"
  name: "SpendingLimitPolicy"
  policyDefinitionId: "pol_spending_limit_v3"
  possibleOutcomes: [Success, Escalate, AttentionRequired, Failed]
  evaluationInputMapping:
    invoiceAmount: "${ .invoice_amount }"
    currency: "${ .invoice_currency }"
    vendorTier: "${ .vendor_tier }"
    vendorRiskScore: "${ .vendor_risk_score }"
```

| Field | Required | Default | Description |
|---|---|---|---|
| `policyDefinitionId` | ✅ | — | Identifier of the tenant-agnostic **Policy Definition** whose shape (`variables`, `possibleOutcomes`) this node is governed by. The tenant's `PolicyConfiguration` for this `(workflowConfigId, nodeId)` is resolved by the Policy Service at evaluate time. Legacy blueprints may still use `policyId` for a standalone Policy; new blueprints SHOULD use `policyDefinitionId`. |
| `possibleOutcomes` | — | inherited from the Definition | Exhaustive list of outcome strings the policy may return, drawn from the referenced Definition's `possibleOutcomes` (a closed allow-list). When declared on the node, a conforming implementation MUST validate at definition registration time that every declared outcome has a matching outgoing edge condition, and MUST reject registration if any outcome is unrouted. |
| `evaluationInputMapping` | — | — | Maps workflow context variables to policy evaluation input fields; values may be `${ }` expressions. Names on the left-hand side must match variables declared with source `EVALUATION_INPUT` on the pinned Definition version. |

> **Degenerate-input convention:** A policy that may receive structurally invalid
> inputs (e.g. an empty collection, a null value) **must** return a distinct outcome
> for each degenerate case (e.g. `NO_ITEMS`, `INSUFFICIENT_DATA`) and the workflow
> **must** define an edge routing that outcome to an appropriate
> `FinOSTerminal`. Returning a normal-path outcome (e.g. `REQUIRE_HITL`) for a
> degenerate input is a policy specification error — it routes the workflow into a
> gate with no meaningful context for the reviewer.

---

### 3.5 FinOSWorkflow — Child Workflow Invocation

A `FinOSWorkflow` node invokes another registered workflow as a durable child
execution. Unlike `FinOSAgent` (A2A HTTP), child workflow invocations are
orchestration-native: they can wait indefinitely for HITL gates without timing
out the parent. The child workflow is resolved by `workflowId` from
the Workflow Registry at runtime — the YAML never encodes runtime-vendor details.

**Key distinction:** Use `FinOSWorkflow` when the called unit is itself a
multi-step durable workflow that may contain HITL gates. Use `FinOSAgent` for
stateless LLM agents that respond within a bounded timeout.

**Design-time DAG schema:**
```yaml
- nodeId: account-prepare
  nodeType: FinOSWorkflow
  name: Account Prepare
  description: >
    Prepares a single GL reconciliation account for period close. May pause
    for human document selection and item approval decisions.
  workflowId: wf-account-prepare-v1
  taskInputTemplate:
    account_id: "${ .account_id }"
    period_id:  "${ .period_id }"
    role_id:    "${ .role_id }"
  outputMapping:
    prepare_result: "${ . }"
  timeoutSeconds: 0   # no timeout — HITL can pause execution for hours
```

**Required fields:** `workflowId`, `taskInputTemplate`
**Optional fields:** `outputMapping`, `timeoutSeconds` (default: 0 = no timeout)

**Routing:** The Workflow Registry entry for `workflowId` supplies the
execution target. Changing a workflow-agent from one runtime to another is a registry
update — the YAML is unchanged.

---

### 3.6 FinOSTerminal — Explicit Workflow Terminal

A `FinOSTerminal` node is an explicit, typed workflow terminal. Every DAG must
contain at least one `FinOSTerminal`. Terminal nodes have **no outgoing edges**.

Using an explicit type (rather than implicitly leaving a `targetNodeId` undefined)
enables structured exit semantics, human-readable outcome labels in execution
history, and enforceable DAG integrity rules.

**Design-time DAG schema:**
```yaml
- nodeId: "terminal-approved"
  nodeType: "FinOSTerminal"
  name: "Controller Approved"
  exitStatus: COMPLETED
  label: "Suggestions approved by financial controller."

- nodeId: "terminal-no-items"
  nodeType: "FinOSTerminal"
  name: "No Supporting Items"
  exitStatus: COMPLETED
  label: "Controller determined no supporting items apply. Valid close outcome."

- nodeId: "terminal-error"
  nodeType: "FinOSTerminal"
  name: "Unrecoverable Error"
  exitStatus: FAILED
  label: "Workflow could not proceed after all retries exhausted."
```

| Field | Required | Description |
|---|---|---|
| `exitStatus` | ✅ | `COMPLETED` = valid business outcome. `FAILED` = unrecoverable failure. |
| `label` | — | Human-readable outcome label surfaced in Execution History and workflow UI. |

> **Rule:** An edge whose `targetNodeId` references a `FinOSTerminal` node signals
> the end of the workflow instance. The DAG validator requires that all edge
> `targetNodeId` values reference a node defined in the `nodes` list — implicit
> terminal IDs (undefined node names used as edge targets) are not permitted.

---

### 3.7 FinOSFanOut — Parallel Child-Workflow Fan-Out

A `FinOSFanOut` node iterates over an array in the parent workflow state and
spawns one child `FinOSWorkflow` execution per item. The parent waits for all
children to complete before traversing its outgoing edge.

This is distinct from a general fork/join (see the parallel execution roadmap):
`FinOSFanOut` always fans out to registered **child workflows**, giving each item
its own state, HITL gates, retries, and independent audit trail.

**Design-time DAG schema** — fanning the child workflow of [§3.5](#35-finosworkflow--child-workflow-invocation)
out across every account in a period close, one child execution per account:
```yaml
- nodeId: "prepare-accounts"
  nodeType: "FinOSFanOut"
  name: "Prepare All Accounts"
  workflowId: "wf-account-prepare-v1"
  iterateOver: "${ .account_ids }"
  iterationVar: account_id
  taskInputTemplate:
    account_id: "${ .account_id }"
    period_id:  "${ .period_id }"
    role_id:    "${ .role_id }"
  outputKey: prepare_results
  parallel: true
  timeoutSeconds: 0
```

| Field | Required | Default | Description |
|---|---|---|---|
| `workflowId` | ✅ | — | Child workflow resolved from the Workflow Registry at runtime. |
| `iterateOver` | ✅ | — | Expression resolving to the array in parent state to iterate over. |
| `iterationVar` | ✅ | — | Loop variable name bound to each array item. |
| `taskInputTemplate` | — | — | Expression map building each child's `run_input`. |
| `parallel` | — | `true` | `true` = all children launch concurrently. `false` = sequential. |
| `outputKey` | — | — | Parent state key where collected child results are written. |
| `timeoutSeconds` | — | `0` | Max wait for all children. `0` = no timeout. |

---

### 3.8 FinOSAsyncAPI — Long-Running Callback/Event

A `FinOSAsyncAPI` node fires an initial request that kicks off a long-running
job on a partner system (minutes to days) and suspends execution until a
completion notification arrives — via an authenticated HTTP callback or an
internal event. Unlike `FinOSAPI`, the initial HTTP response is not the
result; the eventual callback/event payload is. Unlike `FinOSHITL`, resolution
is a system completing work on its own schedule, not a human decision.

**Design-time DAG schema:**
```yaml
- nodeId: reconcile-with-external-ledger
  nodeType: FinOSAsyncAPI
  name: "Trigger External Reconciliation Job"
  endpoint: "${ .ledger_base_url + \"/reconciliations\" }"
  httpMethod: POST
  requestBody:
    accountId: "${ .account_id }"
    period: "${ .period }"
  requestHeaders:
    X-Api-Key: "${ .ledger_api_key }"
  callbackMode: HTTP
  resumeTokenInjection:
    location: body
    field: "callbackReference"
  callbackUrlInjection:
    location: body
    field: "webhookUrl"
  resultMapping:
    reconciliation_status: "${ .result.status }"
    discrepancy_count: "${ .result.discrepancies | length }"
  timeoutSeconds: 86400
  statusCheckConfig:
    endpoint: "${ .statusUrl }"     # jq over the KICKOFF response body
    httpMethod: GET
    statusPath: "$.data.status"
    succeededValues: ["COMPLETED"]
    failedValues: ["FAILED", "CANCELLED"]
    resultMapping:
      reconciliation_status: "${ .data.status }"
      discrepancy_count: "${ .data.discrepancies | length }"
  retryPolicy:
    maxRetries: 2
    initialDelaySeconds: 30
    maxDelaySeconds: 300
    multiplier: 2.0
```

| Field | Required | Default | Description |
|---|---|---|---|
| `endpoint` | ✅ | — | Kickoff URL; may contain `${ }` expressions. |
| `httpMethod` | ✅ | — | HTTP verb for the kickoff call. |
| `requestHeaders` / `requestBody` / `queryParams` | — | — | Same semantics as FinOSAPI (§3.2). **When any injection below uses `location: body`, `requestBody` must resolve to a JSON object** (so the `field` can be inserted); a non-object body is rejected at execution. |
| `callbackMode` | ✅ | — | `HTTP` — partner POSTs completion to a callback endpoint provided by the FinOS Workflows Engine. `EVENT` — an internal platform adapter publishes completion to the shared event topic on the partner's behalf; partners never publish directly. |
| `resumeTokenInjection` | ✅ | — | `{ location: body\|header\|queryParam, field }` — where the resumeToken is placed in the kickoff request so the partner can echo it back. `location: body` requires `requestBody` to resolve to a JSON object. |
| `callbackUrlInjection` | — (HTTP only) | — | Same shape as `resumeTokenInjection`. Omitted for `callbackMode: EVENT` — there's no URL to inject. |
| `resultMapping` | — | — | Maps the eventual callback/event payload's `result` object into workflow context. jq expressions evaluated against `result`. |
| `timeoutSeconds` | ✅ | — | Real business deadline — the **authoritative** timeout for this node, enforced by the FinOS Workflows Engine. |
| `statusCheckConfig` | — | — | Optional pre-fail/pre-retry status-check step; see below. Absent → pure elapsed-time expiry. |
| `retryPolicy` | — | see [§3.2.1](#321-retrypolicy-sub-object) | Applies to `FAILED` outcomes on either transport, same retry/backoff semantics as FinOSAgent/FinOSAPI. |

**`statusCheckConfig` sub-object:**

| Field | Required | Description |
|---|---|---|
| `endpoint` | ✅ | Status-check URL — either a literal URL, or a jq expression over the **kickoff response body** when the partner returns a per-job status URL, e.g. `${ .statusUrl }`. Resolved at kickoff time and retained for the node's lifetime. Note the evaluation root differs from the node's top-level `resultMapping`, which binds to the completion payload's `result` object. |
| `httpMethod` | ✅ | `GET` or `POST`. |
| `statusPath` | ✅ | JSONPath into the status-check response identifying the status field, e.g. `$.data.status`. |
| `succeededValues` / `failedValues` | ✅ | Status values indicating success / terminal failure. |
| `resultMapping` | — | Same jq-expression semantics as the node's top-level `resultMapping`, evaluated against the status-check response instead. |

**Callback/event payload contract (identical shape for both transports):**
```json
{
  "resumeToken": "018f2a9e-6b3d-7000-9c21-1a2b3c4d5e6f",
  "status": "SUCCEEDED",
  "result": { "...": "partner-specific, mapped via resultMapping" },
  "error": { "code": "...", "message": "..." }
}
```

See [§13](#13-finosasyncapi-callbackevent-resume-pattern) for the full suspend/resume sequence and the sweep/timeout state machine.

---

## 4. Edge Routing

Edges connect nodes. Routing is entirely condition-driven:

- **No condition (`condition: null`)** — catch-all/default edge, always traversed. At most one per source node.
- **With condition** — traversed when the jq expression evaluates to truthy
- **Multiple outgoing edges** — conditions evaluated in `priority` order (lower number = first); first match wins. Place the catch-all edge at the highest priority number so it only fires when no conditional edge matched.

```yaml
edges:
  - edgeId: e-escalate
    sourceNodeId: spending-policy
    targetNodeId: manager-review
    condition: '${ .policy_outcome == "ESCALATE" }'
    priority: 1

  - edgeId: e-approved
    sourceNodeId: spending-policy
    targetNodeId: post-approval
    condition: '${ .policy_outcome == "APPROVED" }'
    priority: 2

  - edgeId: e-default          # catch-all — fires if no condition above matched
    sourceNodeId: spending-policy
    targetNodeId: terminal-error
    condition: null
    priority: 3
```

> **Rule:** A source node may have **at most one** `condition: null` edge. Multiple unconditional edges from the same source node are a validation error — the second can never be reached. Use `FinOSFanOut` when parallel execution is the intent.

**Edge schema:**

| Field | Required | Description |
|---|---|---|
| `edgeId` | ✅ | Unique within the DAG |
| `sourceNodeId` | ✅ | |
| `targetNodeId` | ✅ | Must reference a defined node (including FinOSTerminal nodes) |
| `condition` | ✅ if source has >1 outgoing edge | jq expression evaluated against workflow context; `null` = catch-all default |
| `priority` | — | Lower = evaluated first; list order used when omitted |

### Expression Syntax Reference

FinOS uses **[jq](https://jqlang.org)** as its expression language. The `${ }` delimiters are a FinOS convention for embedding a jq expression in a YAML string value — they are not part of jq itself. The expression inside the delimiters is standard jq; leading-dot field access (`${ .field }`) is the canonical form.

> **YAML quoting requirement:** `{` and `}` are reserved flow-mapping characters in YAML and cannot appear in unquoted values. Every `${ }` expression **must** be wrapped in YAML quotes — either single or double:
>
> ```yaml
> # ❌ YAML parse error
> field: ${ .vendor_id }
>
> # ✅ Single quotes — preferred; no escaping needed for inner double quotes
> field: '${ .vendor_id }'
> condition: '${ .status == "APPROVED" }'
>
> # ✅ Double quotes — requires backslash-escaping inner double quotes
> field: "${ .vendor_id }"
> condition: "${ .status == \"APPROVED\" }"
> ```
>
> Use **single quotes** when the expression contains string comparisons (avoids backslash noise). Use **double quotes** when the expression itself contains single quotes or when the surrounding tooling requires it.

#### Two expression forms

**Full-value expression** — the entire YAML string value is `${ expr }`:

```yaml
vendor_tier: "${ .body.tier }"       # returns typed result (string, number, bool, object, …)
enabled:     "${ .flags.enabled }"   # returns bool, not the string "true"
context:     "${ . }"                # returns entire context object
```

The result preserves the jq type (string, number, boolean, array, object). Use this form whenever a field expects a non-string value or the complete output of a jq expression.

**String interpolation** — one or more `${ }` segments embedded in a larger string:

```yaml
endpoint: "https://erp-api.example.com/v1/vendors/${ .vendor_id }/invoices"
subject:  "Review required for ${ .vendor_name } — amount: ${ .invoice_amount }"
```

Each embedded segment is evaluated and coerced to a string; the surrounding literal text is preserved. The result is always a string. Use this form to construct URLs, message text, or any string that combines literals with context values.

| Pattern | Expression | Notes |
|---|---|---|
| Field access | `"${ .account_id }"` | Full-value; returns typed result |
| Nested field | `"${ .vendor.tier }"` | Dot-separated path |
| URL interpolation | `"https://api/${ .vendor_id }"` | String interpolation; always returns string |
| Multi-segment | `"${ .base }/${ .id }/items"` | All segments evaluated and joined |
| String equality | `"${ .status == \"ACTIVE\" }"` | Full-value; returns bool |
| Numeric comparison | `"${ .invoice_amount >= 50000 }"` | Full-value; returns bool |
| Compound boolean | `"${ .a == \"X\" and .b < 100 }"` | Use `and`, `or` |
| Env variable | `"${ $ENV.MY_VAR }"` | Runtime environment variable |
| Full jq | `"${ .items \| length }"` | Any valid jq expression |
| Entire context | `"${ . }"` | Maps complete context object |

> **Accepted expression forms:**
> - **Canonical (preferred):** `"${ .field_name }"` — use this in all new workflow YAML.
> - **Legacy `$.` prefix in `responseMapping`/`outputMapping`:** `"$.body.field"` — the interpreter strips the leading `$` to produce `.body.field` for jq evaluation; supported for backward compatibility.
>
> New workflow YAML should always use the canonical `${ .field }` form.

---

## 5. Runtime State Representation

The interpreter tracks live execution state across node transitions.

At each node transition, the interpreter emits a FinOS event containing the current execution state (`workflowId`, `instanceId`, `tenantId`, `status`, `currentNodeId`, `completedNodeIds`). This event is persisted to FinOSWorkflowExecutionHistory and provides the SOX audit trail.

---

## 6. REST API — FinOSWorkflow CRUD

**Base URL:** `https://api.example.com/finos/v1`

### Create a workflow

```bash
curl -X POST https://api.example.com/finos/v1/workflows \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "workflowId": "wf-invoice-approval-v1",
    "workflowName": "Invoice Approval Workflow",
    "workflowVersion": "1.0.0",
    "description": "Multi-tier invoice approval with spending-limit policy.",
    "tenantId": "tenant-acme-prod"
  }'
# → 201 Created  Location: /workflows/wf-invoice-approval-v1
```

### Get a workflow (full DAG)

```bash
curl "https://api.example.com/finos/v1/workflows/wf-invoice-approval-v1?embed=full" \
  -H "Authorization: Bearer $TOKEN"
```

### Update metadata (partial)

```bash
curl -X PATCH https://api.example.com/finos/v1/workflows/wf-invoice-approval-v1 \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/merge-patch+json" \
  -d '{ "workflowVersion": "1.1.0", "description": "Updated approval threshold to $100k." }'
```

### Activate (transition to ACTIVE)

```bash
curl -X PATCH https://api.example.com/finos/v1/workflows/wf-invoice-approval-v1/status \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{ "status": "ACTIVE", "reason": "Validated and approved." }'
```

### Delete (hard delete, DRAFT only)

```bash
curl -X DELETE https://api.example.com/finos/v1/workflows/wf-invoice-approval-v1 \
  -H "Authorization: Bearer $TOKEN"
# → 204 No Content
```

---

## 7. REST API — Node CRUD

### Insert a FinOSAgent node

```bash
curl -X POST https://api.example.com/finos/v1/workflows/wf-invoice-approval-v1/nodes \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "nodeId": "node-classify-invoice",
    "nodeType": "FinOSAgent",
    "name": "InvoiceClassifierAgent",
    "agentId": "agt-invoice-classifier-001",
    "retryPolicy": { "maxRetries": 3, "initialDelaySeconds": 2, "maxDelaySeconds": 30, "multiplier": 2 }
  }'
# → 201 Created
```

### Insert a FinOSHITL node

```bash
curl -X POST https://api.example.com/finos/v1/workflows/wf-invoice-approval-v1/nodes \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "nodeId": "node-hitl-manager",
    "nodeType": "FinOSHITL",
    "name": "ManagerReview",
    "assignedRole": "finance-manager",
    "timeoutSeconds": 259200,
    "allowedDecisions": ["APPROVED", "REJECTED"],
    "escalationPolicy": {
      "escalateAfterSeconds": 86400,
      "assignedRole": "finance-director"
    }
  }'
# → 201 Created
```

### Append a new node after an existing one

The `append` operation creates the node **and** wires the sequential edge in one call:

```bash
curl -X POST https://api.example.com/finos/v1/workflows/wf-invoice-approval-v1/nodes/node-fetch-vendor/append \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "nodeId": "node-fraud-check",
    "nodeType": "FinOSAPI",
    "name": "FraudCheckAPI",
    "endpoint": "https://risk-api.example.com/v1/fraud-check",
    "httpMethod": "POST",
    "requestBody": { "vendorId": "${ .vendor_id }", "invoiceAmount": "${ .invoice_amount }" },
    "responseMapping": { "fraud_score": "${ .body.score }" },
    "appendEdgeCondition": null,
    "rewireExistingEdges": true
  }'
# → 201 Created  { data: { newNode: {...}, createdEdge: {...} } }
```

### Update a node (partial)

```bash
# Extend HITL timeout on an existing node
curl -X PATCH https://api.example.com/finos/v1/workflows/wf-invoice-approval-v1/nodes/node-hitl-manager \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/merge-patch+json" \
  -d '{ "timeoutSeconds": 345600, "assignedRole": "senior-finance-manager" }'
```

### Remove a node (auto-reconnect successors)

```bash
curl -X DELETE "https://api.example.com/finos/v1/workflows/wf-invoice-approval-v1/nodes/node-fraud-check?reconnectEdges=true" \
  -H "Authorization: Bearer $TOKEN"
# → 204 No Content
# Predecessor → successor edges auto-created
```

### Update node runtime status

Called by the workflow engine at each state transition:

```bash
curl -X PATCH https://api.example.com/finos/v1/workflows/wf-invoice-approval-v1/nodes/node-classify-invoice/runtime-status \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "runtimeStatus": "COMPLETED",
    "completedAt": "2026-05-18T09:00:15Z",
    "outputs": {
      "invoiceCategory": "VENDOR_INVOICE",
      "totalAmount": 48500.00,
      "vendorId": "vendor-acme-001"
    }
  }'
```

---

## 8. REST API — Edge CRUD

### Add an edge with condition

```bash
curl -X POST https://api.example.com/finos/v1/workflows/wf-invoice-approval-v1/edges \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "edgeId": "edge-03-policy-escalate",
    "sourceNodeId": "node-spending-policy",
    "targetNodeId": "node-hitl-manager",
    "condition": "${ .policy_outcome == \"Escalate\" or .policy_outcome == \"AttentionRequired\" }",
    "priority": 1
  }'
# → 201 Created
```

### Update edge condition

```bash
# Raise the CFO threshold from $50k to $100k
curl -X PATCH https://api.example.com/finos/v1/workflows/wf-invoice-approval-v1/edges/edge-06-mgr-approved-high/condition \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{ "condition": "${ .manager_decision == \"APPROVED\" and .invoice_amount >= 100000 }" }'
```

### Update edge priority

```bash
curl -X PATCH https://api.example.com/finos/v1/workflows/wf-invoice-approval-v1/edges/edge-04-policy-success \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/merge-patch+json" \
  -d '{ "priority": 2 }'
```

### Remove an edge

```bash
curl -X DELETE https://api.example.com/finos/v1/workflows/wf-invoice-approval-v1/edges/edge-old-direct-path \
  -H "Authorization: Bearer $TOKEN"
# → 204 No Content
```

### List all outgoing edges from a policy node

```bash
curl "https://api.example.com/finos/v1/workflows/wf-invoice-approval-v1/nodes/node-spending-policy/edges?direction=outgoing" \
  -H "Authorization: Bearer $TOKEN"
```

---

## 9. DAG Integrity Rules

The API enforces these invariants on every write:

| Rule | Detail | Error |
|---|---|---|
| **No cycles** | Adding an edge is rejected if it would introduce a cycle. Validated via DFS. | `409` |
| **Vertex existence** | Edge source and target must reference existing nodes in the same workflow. | `422` |
| **Immutable discriminator** | `nodeType` cannot be changed after creation. | `422` |
| **Status-gated edits** | Nodes and edges cannot be added/modified while `status = ACTIVE`. Downgrade to `DRAFT` first. | `409` |
| **Condition syntax** | Edge conditions must parse as valid jq expressions. | `422` |
| **At most one default edge** | A source node may have at most one `condition: null` (catch-all) edge. Multiple unconditional edges from the same source are a validation error. | `422` |
| **possibleOutcomes coverage** | When `possibleOutcomes` is declared on a `FinOSPolicy` node, every listed outcome must have a matching outgoing edge condition. Registration is rejected if any outcome is unrouted. | `422` |
| **Priority uniqueness** | Two edges from the same source node with the same `priority` raise a warning (last-write-wins during evaluation). | Warning |
| **Exactly one entry point** | A DAG must contain exactly one `FinOSStart` node, with zero incoming edges. | `422` |

---

## 10. HITL Pause & Resume Pattern

When a `FinOSHITL` node is reached, the workflow:

```
① The workflow runtime registers a callback endpoint and suspends execution
        → Returns { http_callback_url: "https://workflows.example.com/..." }

② FinOS posts FinOSHITLEvent to ExecutionHistory API
        → Stores callbackUrl in the HITL vertex

③ FinOS Notification Service delivers task to the reviewer
        → Email / Slack / HITL Task Portal with callbackUrl

④ Workflow suspends, waiting for the callback (up to timeoutSeconds)

⑤ Reviewer completes the task — POSTs to callbackUrl:
        { "decision": "APPROVED", "reviewerId": "user-001", "comments": "..." }

⑥ Workflow resumes — decision is extracted from callback request body

⑦ Edge conditions on the HITL node's outgoing edges route execution
        APPROVED → next approval tier or terminal
        REJECTED → rejection terminal
```

**HITL callback payload schema:**
```json
{
  "decision": "APPROVED | REJECTED | OVERRIDE",
  "reviewerId": "<string>",
  "comments": "<string>",
  "reviewedAt": "<ISO-8601 datetime>",
  "evidence": { }
}
```

**Timeout handling:** If the callback wait expires, the runtime raises a
`TimeoutError`. The workflow's `except` block should catch this, post an `Exception`
vertex to the History API, and either escalate or terminate the execution.

---

## 11. Running a Workflow

Trigger a workflow execution by submitting a run request to the workflow execution API:

```http
POST /workflows/{workflowId}/runs
{
  "tenantId": "tenant-123",
  "run_input": {
    "account_id": "C0004",
    "period_id": "P2026-06"
  }
}
```

A conforming implementation:
1. Loads tenant configuration values for `source: config` inputs
2. Merges with `run_input` (run_input takes precedence)
3. Validates all `required: true` inputs are present
4. Returns `{ "instanceId": "...", "status": "RUNNING" }`

---

## 12. End-to-End Example: Invoice Approval Workflow

The complete workflow definition below models a 12-edge DAG. It classifies an invoice with an
A2A agent, enriches it with vendor data from the ERP, evaluates a spending-limit policy, routes
escalations through one or two tiers of human review depending on amount, and posts the approval
back to the ERP with a REST `POST` call.

It exercises six of the nine node types: `FinOSStart`, `FinOSAgent`, `FinOSAPI`, `FinOSPolicy`,
`FinOSHITL`, and `FinOSTerminal`.

```
Workflow Entry (FinOSStart)
    │ e-00-start-to-classify  [unconditional]
    ▼
Invoice Classifier (FinOSAgent)
    │ e-01-classify-to-vendor  [unconditional]
    ▼
Vendor Data API (FinOSAPI — GET)
    │ e-02-vendor-to-policy  [unconditional]
    ▼
Spending Limit Policy (FinOSPolicy)
    │ e-03-policy-escalate  [ESCALATE]  → Manager Review
    │ e-04-policy-approve   [APPROVED]  → ERP Poster
    │ e-05-policy-reject    [REJECTED]  → Terminal (Rejected)

Manager Review (FinOSHITL — 72h escalation to finance-director)
    │ e-06-manager-to-cfo   [decision == "APPROVED" and amount >= 50000]  → CFO Approval
    │ e-07-manager-to-post  [decision == "APPROVED" and amount <  50000]  → ERP Poster
    │ e-08-manager-reject   [decision == "REJECTED"]                      → Terminal (Rejected)

CFO Approval (FinOSHITL — 48h escalation to cfo-backup)
    │ e-09-cfo-approve  [decision == "APPROVED"]  → ERP Poster
    │ e-10-cfo-reject   [decision == "REJECTED"]  → Terminal (Rejected)

ERP Poster (FinOSAPI — POST)
    │ e-11-post-to-terminal  [unconditional]
    ▼
Terminal (Approved) — exitStatus: COMPLETED
```

The ERP Poster is a `FinOSAPI` node, not a `FinOSAgent`. No agent performs a mutation: agents
read and recommend, and every write to a system of record is a deterministic API call. Here the
`POST` runs only after the spending policy has approved the invoice, or after the human reviewers
have approved an escalation.

Both terminals carry `exitStatus: COMPLETED`: a rejected invoice is a valid business outcome,
not a workflow failure. `FAILED` is reserved for a workflow that could not proceed — escalation
exhausted, or an unrecoverable error.

### Execution paths

| Scenario | Path | Terminal |
|---|---|---|
| Policy approves outright | Classify → VendorAPI → Policy(APPROVED) → ERPPoster | ✅ Approved |
| Escalated, below $50k | Classify → VendorAPI → Policy(ESCALATE) → Manager(APPROVED) → ERPPoster | ✅ Approved |
| Escalated, at or above $50k | Classify → VendorAPI → Policy(ESCALATE) → Manager(APPROVED) → CFO(APPROVED) → ERPPoster | ✅ Approved |
| Policy rejects outright | Classify → VendorAPI → Policy(REJECTED) | ✅ Rejected |
| Manager rejects | Classify → VendorAPI → Policy(ESCALATE) → Manager(REJECTED) | ✅ Rejected |
| CFO rejects | Classify → VendorAPI → Policy(ESCALATE) → Manager(APPROVED) → CFO(REJECTED) | ✅ Rejected |

### Inputs

The workflow declares three inputs ([§2.1](#21-inputs)). Two are supplied per run; the ERP base
URL is a `source: config` value a tenant admin sets once:

```yaml
inputs:
  - { name: invoice_id,   type: string, required: true, source: runtime }
  - { name: tenant_id,    type: string, required: true, source: runtime }
  - { name: erp_base_url, type: string, required: true, source: config  }
```

Trigger a run with the runtime inputs only — the engine merges the tenant's configured values
underneath (see [§11](#11-running-a-workflow)):

```bash
curl -X POST https://api.example.com/finos/v1/workflows/wf-invoice-approval-v1/runs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "tenantId": "tenant-acme-prod",
    "run_input": { "invoice_id": "inv-2026-0042", "tenant_id": "tenant-acme-prod" }
  }'
# → 202 Accepted  { "instanceId": "5281a21d-...", "status": "RUNNING" }
```


### Complete workflow definition

The definition below is self-contained and passes every integrity rule in
[§9](#9-dag-integrity-rules): one `FinOSStart` with no incoming edges, two `FinOSTerminal` nodes
with no outgoing edges, no cycles, no unreachable nodes, a single catch-all limit per source, and
full `possibleOutcomes` edge coverage on the policy node.

```yaml
workflowId:      wf-invoice-approval-v1
# workflowVersion: 1.0.0
# Platform:        Blackline FinOS Orchestration Platform
#
# DAG TOPOLOGY:
#
#  [start] (FinOSStart)
#       │ (unconditional)
#       ▼
#  [classify-invoice] (FinOSAgent)
#       │ (unconditional)
#       ▼
#  [fetch-vendor] (FinOSAPI)
#       │ (unconditional)
#       ▼
#  [spending-policy] (FinOSPolicy)
#       ├── outcome == ESCALATE  → [manager-review] (FinOSHITL)
#       │       ├── decision == APPROVED and amount >= 50000 → [cfo-approval] (FinOSHITL)
#       │       │       ├── decision == APPROVED → [post-approval] (FinOSAPI — POST)
#       │       │       │       │ (unconditional) → [terminal-completed]
#       │       │       └── decision == REJECTED → [terminal-rejected]
#       │       ├── decision == APPROVED and amount < 50000  → [post-approval]
#       │       └── decision == REJECTED → [terminal-rejected]
#       ├── outcome == APPROVED → [post-approval]
#       └── outcome == REJECTED → [terminal-rejected]
#
# NODE TYPES DEMONSTRATED:
#   FinOSStart    — Explicit workflow entry point (REQUIRED, exactly one, per DAG)
#   FinOSAgent    — A2A agent invocation (reads and recommends; never mutates)
#   FinOSAPI      — REST API call (GET reads vendor data; POST writes the approval)
#   FinOSHITL     — Human-in-the-loop gate
#   FinOSPolicy   — Policy evaluation
#   FinOSTerminal — Explicit workflow terminal (REQUIRED in every DAG)
# =============================================================================

workflowId: wf-invoice-approval-v1
workflowName: "Invoice Approval"
workflowVersion: "1.0.0"
description: "Classifies an invoice, evaluates spending policy, routes for human review if needed, then posts to ERP."

inputs:
  - name: invoice_id
    type: string
    required: true
    source: runtime
    description: "Invoice to process"
  - name: tenant_id
    type: string
    required: true
    source: runtime
  - name: erp_base_url
    type: string
    required: true
    source: config
    description: "ERP API base URL, configured per tenant"

nodes:
  # ── Entry point: explicit typed workflow start ──────────────────────────
  - nodeId: start
    nodeType: FinOSStart
    name: "Workflow Entry"

  # ── Step 1: Classify invoice via A2A agent ──────────────────────────────
  - nodeId: classify-invoice
    nodeType: FinOSAgent
    name: "Invoice Classifier"
    agentId: agt-invoice-classifier-001
    taskInputTemplate:
      invoice_id: "${ .invoice_id }"
    outputMapping:
      invoice_amount: "${ .amount }"
      vendor_id: "${ .vendorId }"
      classification: "${ .classification }"
    timeoutSeconds: 60
    retryPolicy:
      maxRetries: 2
      initialDelaySeconds: 2
      maxDelaySeconds: 10
      multiplier: 2.0

  # ── Step 2: Fetch vendor data via REST API ──────────────────────────────
  - nodeId: fetch-vendor
    nodeType: FinOSAPI
    name: "Vendor Data API"
    endpoint: "${ .erp_base_url + \"/vendors/\" + .vendor_id }"
    httpMethod: GET
    responseMapping:
      vendor_name: "${ .data.name }"
      vendor_tier: "${ .data.tier }"
      vendor_risk_score: "${ .data.riskScore }"
    timeoutSeconds: 15
    retryPolicy:
      maxRetries: 3
      initialDelaySeconds: 1
      maxDelaySeconds: 10
      multiplier: 2.0

  # ── Step 3: Evaluate spending limit policy ──────────────────────────────
  - nodeId: spending-policy
    nodeType: FinOSPolicy
    name: "Spending Limit Policy"
    policyDefinitionId: def-spending-limit
    evaluationInputMapping:
      invoice_amount: "${ .invoice_amount }"
      vendor_tier: "${ .vendor_tier }"
      vendor_risk_score: "${ .vendor_risk_score }"
    possibleOutcomes: [APPROVED, ESCALATE, REJECTED]

  # ── Step 4a: Manager review (HITL) ─────────────────────────────────────
  - nodeId: manager-review
    nodeType: FinOSHITL
    name: "Manager Review"
    assignedRole: finance-manager
    uiComponentId: invoice-review-panel
    taskInstructions: "Review invoice ${ .invoice_id } (${ .vendor_name }) for ${ .invoice_amount }. Classification: ${ .classification }."
    taskContextTemplate:
      invoice_id: "${ .invoice_id }"
      invoice_amount: "${ .invoice_amount }"
      vendor_name: "${ .vendor_name }"
      vendor_tier: "${ .vendor_tier }"
      classification: "${ .classification }"
    allowedDecisions: [APPROVED, REJECTED]
    escalationPolicy:
      escalateAfterSeconds: 259200
      assignedRole: finance-director
    timeoutSeconds: 0

  # ── Step 4b: CFO approval for high-value invoices (HITL) ────────────────
  - nodeId: cfo-approval
    nodeType: FinOSHITL
    name: "CFO Approval"
    assignedRole: cfo
    uiComponentId: invoice-review-panel
    taskInstructions: "High-value invoice requiring CFO sign-off. Amount: ${ .invoice_amount }."
    taskContextTemplate:
      invoice_id: "${ .invoice_id }"
      invoice_amount: "${ .invoice_amount }"
      vendor_name: "${ .vendor_name }"
      classification: "${ .classification }"
    allowedDecisions: [APPROVED, REJECTED]
    escalationPolicy:
      escalateAfterSeconds: 172800
      assignedRole: cfo-backup
    timeoutSeconds: 0

  # ── Step 5: Post approval to ERP (FinOSAPI — POST) ──────────────────────
  # The only mutation in the workflow. It is a deterministic REST call, never
  # an agent: no FinOSAgent may perform a mutation. The Idempotency-Key header
  # makes retries safe, so a retried POST cannot post the approval twice.
  - nodeId: post-approval
    nodeType: FinOSAPI
    name: "ERP Poster"
    endpoint: "${ .erp_base_url + \"/invoices/\" + .invoice_id + \"/approvals\" }"
    httpMethod: POST
    requestHeaders:
      Content-Type: "application/json"
      X-Tenant-Id: "${ .tenant_id }"
      Idempotency-Key: "${ .invoice_id + \"-approval\" }"
    requestBody:
      invoiceId: "${ .invoice_id }"
      approvalStatus: "APPROVED"
      amount: "${ .invoice_amount }"
      vendorId: "${ .vendor_id }"
      classification: "${ .classification }"
    responseMapping:
      erp_confirmation_id: "${ .body.confirmationId }"
    timeoutSeconds: 30
    retryPolicy:
      maxRetries: 3
      initialDelaySeconds: 5
      maxDelaySeconds: 30
      multiplier: 2.0

  # ── Terminals ────────────────────────────────────────────────────────────
  - nodeId: terminal-completed
    nodeType: FinOSTerminal
    name: "Invoice Approved"
    exitStatus: COMPLETED
    label: "Invoice approved and posted to ERP."

  - nodeId: terminal-rejected
    nodeType: FinOSTerminal
    name: "Invoice Rejected"
    exitStatus: COMPLETED
    label: "Invoice rejected. No ERP posting."

edges:
  # start → classify (unconditional)
  - edgeId: e-00-start-to-classify
    sourceNodeId: start
    targetNodeId: classify-invoice

  # classify → fetch vendor (unconditional)
  - edgeId: e-01-classify-to-vendor
    sourceNodeId: classify-invoice
    targetNodeId: fetch-vendor

  # fetch vendor → policy (unconditional)
  - edgeId: e-02-vendor-to-policy
    sourceNodeId: fetch-vendor
    targetNodeId: spending-policy

  # policy → manager review (escalate)
  - edgeId: e-03-policy-escalate
    sourceNodeId: spending-policy
    targetNodeId: manager-review
    condition: "${ .policy_outcome == \"ESCALATE\" }"
    priority: 1

  # policy → post approval (direct approve)
  - edgeId: e-04-policy-approve
    sourceNodeId: spending-policy
    targetNodeId: post-approval
    condition: "${ .policy_outcome == \"APPROVED\" }"
    priority: 2

  # policy → rejected terminal
  - edgeId: e-05-policy-reject
    sourceNodeId: spending-policy
    targetNodeId: terminal-rejected
    condition: "${ .policy_outcome == \"REJECTED\" }"
    priority: 3

  # manager review → CFO (approved, high value)
  - edgeId: e-06-manager-to-cfo
    sourceNodeId: manager-review
    targetNodeId: cfo-approval
    condition: "${ .decision == \"APPROVED\" and .invoice_amount >= 50000 }"
    priority: 1

  # manager review → post approval (approved, normal value)
  - edgeId: e-07-manager-to-post
    sourceNodeId: manager-review
    targetNodeId: post-approval
    condition: "${ .decision == \"APPROVED\" and .invoice_amount < 50000 }"
    priority: 2

  # manager review → rejected
  - edgeId: e-08-manager-reject
    sourceNodeId: manager-review
    targetNodeId: terminal-rejected
    condition: "${ .decision == \"REJECTED\" }"
    priority: 3

  # CFO approval → post approval
  - edgeId: e-09-cfo-approve
    sourceNodeId: cfo-approval
    targetNodeId: post-approval
    condition: "${ .decision == \"APPROVED\" }"
    priority: 1

  # CFO approval → rejected
  - edgeId: e-10-cfo-reject
    sourceNodeId: cfo-approval
    targetNodeId: terminal-rejected
    condition: "${ .decision == \"REJECTED\" }"
    priority: 2

  # post approval → completed terminal (unconditional)
  - edgeId: e-11-post-to-terminal
    sourceNodeId: post-approval
    targetNodeId: terminal-completed
```

## 13. FinOSAsyncAPI Callback/Event Resume Pattern

When a `FinOSAsyncAPI` node is reached, the workflow:

```
① The FinOS Workflows Engine registers this attempt and suspends execution
        → Returns { resumeToken: "018f2a9e-..." }

② resumeToken is injected into the outbound kickoff request per
   resumeTokenInjection (and, for callbackMode: HTTP, a callback URL
   embedding resumeToken is injected per callbackUrlInjection)

③ The runtime fires the kickoff request to the partner API, which starts
   their long-running job. Unlike FinOSHITL, no callback URL is surfaced
   to a human reviewer — the workflow simply waits for the partner (or
   its internal adapter) to report back.

④ If statusCheckConfig is present, the runtime resolves its `endpoint`
   against the kickoff response body and retains the resolved URL for the
   lifetime of the node. This must happen here, while the kickoff response
   is in hand — step ⑥ may run days later, long after that response is
   gone. Nodes whose statusCheckConfig.endpoint is a literal URL need no
   resolution.

⑤ Exactly one of two things resolves the suspension:
     - An authenticated caller POSTs to the runtime's callback endpoint
       (the only ingress for genuinely external callers), or
     - An internal platform adapter service publishes to the shared event
       topic (internal-to-internal only; never a direct partner publish)
   Both paths resolve resumeToken to the correct suspended node and
   resume it, independently of one another

⑥ If the engine's timeout sweep fires before resolution (timeoutSeconds
   elapsed with no callback/event):
     - No statusCheckConfig → fail immediately; node's retryPolicy decides
       whether to reschedule (new outbound call, new resumeToken)
     - statusCheckConfig present → poll the URL retained in step ④ once
       first:
         still running  → extend the deadline once (bounded), stay pending
         succeeded       → resolve success via statusCheckConfig.resultMapping
         failed          → fail per retryPolicy

⑦ Workflow resumes — resultMapping (or statusCheckConfig.resultMapping on
   the status-check path) extracts fields from the payload into context

⑧ Edge conditions on the node's outgoing edges route execution as normal
```

**Key distinction from FinOSHITL ([§10](#10-hitl-pause--resume-pattern)):**
both suspend workflow execution pending an external event, but `FinOSHITL`
waits on a human decision submitted through a callback URL delivered to a
reviewer; `FinOSAsyncAPI` waits on an external system (or its internal
adapter) reporting completion on its own schedule, with no reviewer
notification or human-facing callback involved.

**Timeout authority:** the FinOS Workflows Engine's own timeout tracking —
not any lower-level execution safeguard — is authoritative, because
deciding whether to retry a stalled async job needs business logic (a
`statusCheckConfig` poll) that a purely time-based retry policy can't
express.

**Idempotency:** the event transport is at-least-once delivery. A
duplicate/replayed message against an already-resolved resumeToken is a
no-op, not an error.

---

## Appendix — File Index

| File | Purpose |
|---|---|
| `finos-workflow-api.yaml` | OpenAPI 3.0.3 spec for FinOSWorkflow / Node / Edge CRUD |
| `finos-policy-api.yaml` | OpenAPI 3.1.0 spec for the Policy Service evaluated by `FinOSPolicy` nodes |
| `4-finos-policy-guide.md` | Policy Service guide — Definition / Configuration model, evaluation, outcome routing |
| `0-finos-workflow-guide.md` | This document |

## Appendix — Key HTTP Status Codes

| Code | Meaning in FinOSWorkflow API |
|---|---|
| `201` | Node, edge, or workflow created |
| `204` | Node or edge deleted |
| `400` | Malformed request body or invalid query parameter |
| `401` | Missing or invalid authentication |
| `403` | Insufficient permissions for tenant or resource |
| `404` | Workflow, node, or edge not found |
| `409` | Conflict — cycle detected, duplicate ID, or invalid status transition |
| `422` | Validation failed — missing required field, invalid expression syntax |

## Appendix — Expression Syntax Reference

FinOS uses **[jq](https://jqlang.org)** as its expression language. The `${ }` delimiters are a FinOS convention for embedding a jq expression in a YAML string value — they are not part of jq itself. The expression inside the delimiters is standard jq; leading-dot field access (`${ .field }`) is the canonical form.

> **YAML quoting requirement:** `{` and `}` are reserved flow-mapping characters in YAML and cannot appear in unquoted values. Every `${ }` expression **must** be wrapped in YAML quotes — either single or double:
>
> ```yaml
> # ❌ YAML parse error
> field: ${ .vendor_id }
>
> # ✅ Single quotes — preferred; no escaping needed for inner double quotes
> field: '${ .vendor_id }'
> condition: '${ .status == "APPROVED" }'
>
> # ✅ Double quotes — requires backslash-escaping inner double quotes
> field: "${ .vendor_id }"
> condition: "${ .status == \"APPROVED\" }"
> ```
>
> Use **single quotes** when the expression contains string comparisons (avoids backslash noise). Use **double quotes** when the expression itself contains single quotes or when the surrounding tooling requires it.

#### Two expression forms

**Full-value expression** — the entire YAML string value is `${ expr }`:

```yaml
vendor_tier: "${ .body.tier }"       # returns typed result (string, number, bool, object, …)
enabled:     "${ .flags.enabled }"   # returns bool, not the string "true"
context:     "${ . }"                # returns entire context object
```

The result preserves the jq type. Use this form whenever a field expects a non-string value.

**String interpolation** — one or more `${ }` segments embedded in a larger string:

```yaml
endpoint: "https://erp-api.example.com/v1/vendors/${ .vendor_id }/invoices"
subject:  "Review required for ${ .vendor_name } — amount: ${ .invoice_amount }"
```

Each embedded segment is evaluated and coerced to a string; surrounding literal text is preserved. The result is always a string.

| Pattern | Expression | Notes |
|---|---|---|
| Field access | `"${ .account_id }"` | Full-value; returns typed result |
| Nested field | `"${ .vendor.tier }"` | Dot-separated path |
| URL interpolation | `"https://api/${ .vendor_id }"` | String interpolation; always returns string |
| Multi-segment | `"${ .base }/${ .id }/items"` | All segments evaluated and joined |
| String equality | `"${ .status == \"ACTIVE\" }"` | Full-value; returns bool |
| Numeric comparison | `"${ .invoice_amount >= 50000 }"` | Full-value; returns bool |
| Compound boolean | `"${ .a == \"X\" and .b < 100 }"` | Use `and`, `or` |
| Env variable | `"${ $ENV.MY_VAR }"` | Runtime environment variable |
| Full jq | `"${ .items \| length }"` | Any valid jq expression |
| Entire context | `"${ . }"` | Maps complete context object |

> **Accepted expression forms:**
> - **Canonical (preferred):** `"${ .field_name }"` — use this in all new workflow YAML.
> - **Legacy `$.` prefix in `responseMapping`/`outputMapping`:** `"$.body.field"` — the interpreter strips the leading `$` to produce `.body.field` for jq evaluation; supported for backward compatibility.
>
> New workflow YAML should always use the canonical `${ .field }` form.
