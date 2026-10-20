# 14 — Ratatui Management Interface

## 1. Purpose

The Ratatui Management Interface provides an interactive terminal application for observing and managing MarketForge Live.

It allows operators to inspect runtime health, manage source and derived streams, control recording, review operational diagnostics, and modify supported configuration settings.

The TUI is an independent management client.

It communicates exclusively through the versioned Runtime Control API defined in Document 13.

The Live daemon must remain fully operational without an attached terminal interface.

### 1.1 Primary Objectives

The interface must support:

- Monitoring global runtime health.
- Viewing exchange connectivity.
- Inspecting source-stream health and performance.
- Adding, activating, deactivating, and removing source streams.
- Inspecting trade continuity and L2 synchronization.
- Creating and managing derived streams.
- Monitoring merge ordering and currency conversion.
- Enabling and disabling recording.
- Inspecting PostgreSQL recording and Live Retention.
- Viewing recovery progress.
- Editing supported runtime settings.
- Inspecting configuration revisions and validation results.
- Displaying operational errors and warnings.
- Remaining responsive during runtime failures.

The interface must prioritize operational clarity and ease of use.

### 1.2 Design Principles

The TUI follows these principles:

1. Reuse existing MarketForge Ratatui components and visual conventions.
2. Preserve the existing Tokyo Night theme.
3. Keep rendering separate from runtime execution.
4. Use the same Control API as Python and other management clients.
5. Avoid duplicating authoritative runtime state.
6. Display meaningful health information rather than overwhelming users with raw counters.
7. Keep common operations accessible with minimal navigation.
8. Require confirmation for destructive or disruptive actions.
9. Make asynchronous operation progress visible.
10. Remain usable when some runtime components are unavailable.
11. Keep refresh and rendering overhead bounded.
12. Support keyboard-first interaction.

### 1.3 Initial Interface Scope

The initial TUI contains five primary views:

    Dashboard

    Streams

    Derived Streams

    Recording

    Settings

Each view may contain contextual detail panels, tabs, dialogs, or overlays.

The initial implementation should avoid introducing additional top-level views unless they provide a clear operational benefit.

### 1.4 Non-Responsibilities

The Ratatui interface must not:

- Own exchange WebSocket connections.
- Perform canonical market-data normalization.
- Validate native trade continuity.
- Maintain L2 order books.
- Execute recovery algorithms.
- Run merge or currency-conversion processors.
- Write market events into PostgreSQL.
- Own retention buffers.
- Maintain independent authoritative metrics.
- Evaluate stream health independently.
- Directly modify persisted configuration files.
- Manage Prometheus or Grafana processes.
- Require a running Python orchestration process for ordinary management operations.

### 1.5 Fundamental Invariant

    Ratatui Is An
    Independent Control Client.

    It Observes And Requests
    Runtime Operations.

    It Does Not Execute
    Market-Data Processing.

---

## 2. Architectural Boundaries

The Ratatui application runs independently of the MarketForge Live daemon.

Its only operational dependency on the daemon is the public Runtime Control API.

### 2.1 Process Architecture

    +--------------------------------------+
    |           marketforge-live           |
    |                                      |
    |  Exchange Connectivity               |
    |  Source Stream Processing            |
    |  BookStore / Trade Continuity        |
    |  Recovery Coordination               |
    |  Derived Streams                     |
    |  Publisher                           |
    |  Retention / PostgreSQL              |
    |  Metrics Registry                    |
    |  HealthEvaluator                     |
    |                                      |
    |          Runtime Control API         |
    +------------------+-------------------+
                       |
                       |
                Unix Domain Socket
                       |
                       |
    +------------------v-------------------+
    |        marketforge-live-tui          |
    |                                      |
    |  Control Client                      |
    |  Snapshot Cache                      |
    |  Application State                   |
    |  Navigation                          |
    |  Widgets                             |
    |  Dialogs                             |
    |  Ratatui Renderer                    |
    +--------------------------------------+

The diagram describes logical ownership and process isolation.

### 2.2 Executable Separation

The intended executable structure is:

    marketforge-live

    marketforge-live-tui

The Live daemon can be started through a system service manager or directly from the command line.

The TUI connects to an already running daemon.

Closing the TUI must not terminate the daemon.

### 2.3 Control Protocol Dependency

The TUI uses the existing Control API contracts.

Relevant operations include:

    GetRuntimeStatus

    ListSourceStreams

    GetSourceStream

    GetSourceStreamHealth

    ListDerivedStreams

    GetDerivedStream

    GetRecordingStatus

    GetMetrics

    GetHealth

    GetConfiguration

    ValidateConfiguration

    ApplyConfiguration

    GetOperation

The TUI must not define a separate command protocol.

### 2.4 Metrics Integration

The TUI consumes monitoring snapshots from Document 12.

It does not maintain independent throughput counters or latency histograms.

Conceptually:

    Metrics Registry
           |
           v
    Runtime Monitoring API
           |
           v
    Ratatui Snapshot Cache
           |
           v
    Dashboard / Detail Panels

### 2.5 Health Integration

The HealthEvaluator determines health status.

Ratatui displays:

    Healthy

    Degraded

    Unhealthy

    Unknown

    Disabled

It also displays structured health reasons and applicable lifecycle states.

The interface must not infer synchronization health from the number of price levels or infer trade continuity from throughput.

### 2.6 Configuration Integration

The TUI submits configuration changes through:

    Configuration Validation
             |
             v
    Expected Revision Check
             |
             v
    Configuration Manager
             |
             v
    Runtime Reconciler

The TUI may maintain local draft values.

It must not directly overwrite TOML configuration files.

### 2.7 Optional Prometheus Integration

Prometheus and Grafana remain optional external monitoring applications.

Ratatui may display:

    Prometheus Exporter:
        Disabled

or:

    Prometheus Exporter:
        Listening

The TUI does not embed Grafana.

It continues operating when Prometheus and Grafana are unavailable.

### 2.8 Ownership Invariant

    The TUI Owns
    Presentation State.

    The Daemon Owns
    Runtime State.

---

## 3. Reuse of Existing MarketForge TUI

MarketForge already includes Ratatui/Crossterm infrastructure for historical processing, merging, and conversion workflows.

The Live management interface should reuse compatible components rather than establish an unrelated UI implementation.

### 3.1 Existing Infrastructure

Reuse opportunities include:

- Ratatui application setup.
- Crossterm terminal management.
- Tokyo Night theme.
- Keyboard event handling.
- Layout utilities.
- Tables.
- Status indicators.
- Scrollable panels.
- Progress displays.
- Modal dialogs.
- Text input widgets.
- Existing UI error-handling patterns.

Reuse must be based on actual implementation compatibility.

### 3.2 Shared vs Live-Specific Components

The recommended boundary is:

    Shared TUI Infrastructure

        Theme
        Terminal Setup
        Layout Helpers
        Common Widgets
        Input Primitives

    Live-Specific Interface

        Dashboard
        Streams
        Derived Streams
        Recording
        Settings
        Control Client Integration

Live-specific management state must not be inserted into historical processing widgets merely to reuse their appearance.

### 3.3 Proposed Module Structure

Conceptually:

    engine/src/
    |
    +-- tui/
    |    |
    |    +-- theme.rs
    |    |
    |    +-- components/
    |    |    |
    |    |    +-- table.rs
    |    |    +-- status.rs
    |    |    +-- dialog.rs
    |    |    +-- input.rs
    |    |    +-- progress.rs
    |    |
    |    +-- live/
    |         |
    |         +-- mod.rs
    |         +-- app.rs
    |         +-- state.rs
    |         +-- navigation.rs
    |         +-- events.rs
    |         +-- client.rs
    |         |
    |         +-- views/
    |         |    +-- dashboard.rs
    |         |    +-- streams.rs
    |         |    +-- derived.rs
    |         |    +-- recording.rs
    |         |    +-- settings.rs
    |         |
    |         +-- widgets/
    |         +-- dialogs/
    |
    +-- bin/
         |
         +-- marketforge-live-tui.rs

This is a proposed organization.

The actual layout should follow existing project conventions where compatible.

### 3.4 Shared Theme Ownership

The Tokyo Night theme should remain defined once.

Live-specific screens may use existing theme tokens.

They should not duplicate hardcoded color values throughout individual view modules.

### 3.5 Shared Component Compatibility

An existing widget should be reused when:

- Its rendering behavior is appropriate.
- Its state ownership is compatible.
- It does not depend on historical processing internals.
- Its interaction semantics match the Live use case.
- It can operate on supplied presentation data.

A historical progress widget that directly reads historical worker state should be adapted or wrapped rather than reused with an invalid dependency.

### 3.6 Reuse Invariant

    Preserve Existing
    Ratatui Infrastructure
    Where Compatible.

    Avoid Duplicating
    Theme And Widget Logic.

---

## 4. TUI Application Architecture

The TUI should use a simple event-driven architecture.

It maintains presentation state, processes input, communicates with the Control API, and renders current snapshots.

### 4.1 Application Loop

Conceptually:

    Terminal Event
          |
          v
    Input Dispatcher
          |
          v
    Application State Update
          |
          +----------------------+
          |                      |
          v                      v
    Local UI Action        Control API Request
          |                      |
          |                      v
          |               Async Response
          |                      |
          +-----------+----------+
                      |
                      v
               Snapshot Cache
                      |
                      v
                 Render Frame

The application must not block terminal input while waiting for an exchange operation to complete.

### 4.2 Core Application State

Conceptual representation:

    LiveTuiApp {
        connection_state

        navigation_state

        dashboard_state
        streams_state
        derived_state
        recording_state
        settings_state

        snapshot_cache

        pending_requests
        tracked_operations

        active_dialog
        notification_state

        ui_preferences
    }

This is a logical model.

The implementation should use concrete Rust types and avoid unnecessary dynamic dispatch.

### 4.3 Presentation State

The TUI owns:

    SelectedView

    SelectedTableRow

    ScrollOffset

    ActiveFilter

    SortOrder

    FocusedWidget

    OpenDialog

    DraftFormValues

    ExpandedDetails

    LocalNotificationQueue

These values do not modify runtime state until an authorized command is submitted.

### 4.4 Runtime State Cache

The TUI maintains read-only cached snapshots received from the daemon.

Examples:

    RuntimeSnapshot

    StreamSnapshots

    MetricsSnapshot

    HealthSnapshot

    RecordingStatus

    ConfigurationSnapshot

The cache is not authoritative.

### 4.5 Snapshot Metadata

Cached snapshots must preserve:

    SnapshotId

    SnapshotEpoch

    CapturedAt

    ConfigurationRevision

    ResourceGeneration

where applicable.

This allows the TUI to distinguish current observations from stale or obsolete data.

### 4.6 Application Events

Recommended event categories:

    KeyboardInput

    MouseInput

    TerminalResize

    RefreshTick

    ControlConnected

    ControlDisconnected

    ControlResponse

    ControlNotification

    OperationStatusChanged

    LocalValidationResult

### 4.7 Application Event Processing

Events should be processed through a predictable state-update path.

Conceptually:

    AppEvent
       |
       v
    Validate Current UI State
       |
       v
    Apply State Transition
       |
       v
    Schedule Optional Control Request
       |
       v
    Request Rendering

UI event processing must not execute blocking network operations.

### 4.8 Asynchronous Communication

Control requests should be issued through an asynchronous client task or equivalent nonblocking mechanism.

The rendering loop must remain responsive while requests are pending.

### 4.9 Input and Rendering Separation

Input processing updates application state.

Rendering observes application state.

Rendering must not:

- Submit control commands.
- Mutate configuration.
- Initiate recovery.
- Create runtime resources.
- Block waiting for server responses.

### 4.10 Frame Rendering

The renderer should receive a consistent local view of presentation state for each frame.

It may use the latest available daemon snapshots.

The UI does not require a globally atomic runtime snapshot.

### 4.11 Application Architecture Invariant

    Input Updates
    Presentation State.

    Control Requests
    Modify Runtime Intent.

    Rendering Observes
    Presentation State.

---

## 5. Navigation Model

The initial interface contains five primary views.

Navigation should be keyboard-first, predictable, and consistent.

### 5.1 Primary Views

| View | Main responsibility |
|---|---|
| Dashboard | Global runtime overview |
| Streams | Source-stream management and diagnostics |
| Derived Streams | Merge and currency-conversion management |
| Recording | PostgreSQL and retention inspection |
| Settings | Runtime configuration and revisions |

### 5.2 Navigation Layout

Recommended structure:

    +--------------------------------------------------------+
    | MarketForge Live                         Connected     |
    +--------------------------------------------------------+
    | Dashboard | Streams | Derived | Recording | Settings   |
    +--------------------------------------------------------+
    |                                                        |
    |                    Active View                         |
    |                                                        |
    |                                                        |
    +--------------------------------------------------------+
    | Status / Notifications                                 |
    +--------------------------------------------------------+
    | Keyboard Shortcuts                                     |
    +--------------------------------------------------------+

This is conceptual.

The final layout should adapt to terminal dimensions and the existing MarketForge visual style.

### 5.3 Primary Navigation

Recommended bindings:

| Key | Action |
|---|---|
| `1` | Dashboard |
| `2` | Streams |
| `3` | Derived Streams |
| `4` | Recording |
| `5` | Settings |
| `Tab` | Next focusable region |
| `Shift+Tab` | Previous focusable region |
| `Esc` | Close dialog or return |
| `q` | Quit TUI when no modal input is active |
| `?` | Show contextual help |

Number-key navigation must not intercept input while the user is editing a text field.

### 5.4 Navigation State

Conceptual representation:

    NavigationState {
        active_view

        focused_region

        previous_view

        navigation_history

        active_context
    }

Navigation history should remain bounded.

### 5.5 View State Preservation

Switching views should normally preserve local UI state.

For example:

    Streams View:
        Selected BTCUSDT Depth
        Filter = Binance

    Switch To Dashboard

    Return To Streams

The previous selection and filter should remain when the selected resource still exists.

### 5.6 Resource Removal

If a selected stream is removed while the user is viewing it:

- Detect that the resource is unavailable.
- Clear or update the selection.
- Display a bounded notification.
- Preserve the current view.
- Avoid dereferencing obsolete cached state.

### 5.7 Contextual Actions

Each view may expose relevant actions.

Examples:

**Streams**

    Add
    Activate
    Deactivate
    Remove
    Inspect

**Derived Streams**

    Create
    Edit
    Activate
    Deactivate
    Remove

**Recording**

    Enable
    Disable
    Flush
    Inspect Coverage

**Settings**

    Edit
    Validate
    Apply
    Reset Override

Contextual actions must be discoverable through visible shortcut hints or help.

### 5.8 Keyboard Focus

Only one UI region should own keyboard focus at a time.

Focus must be explicit for:

    Tables

    Forms

    Dialogs

    Search Fields

    Detail Panels

The application must not execute global shortcuts while a focused text editor is consuming the same keys.

### 5.9 Modal Navigation

Opening a modal temporarily transfers focus to the modal.

Conceptually:

    Main View
        |
        v
    Open Dialog
        |
        v
    Modal Focus
        |
        +-- Confirm
        |
        +-- Cancel
        |
        v
    Return To Previous Focus

Modal dismissal must restore a valid focus target.

### 5.10 Navigation Invariant

    Navigation Must Be
    Predictable And
    Independent Of
    Runtime Processing State.

---

## 6. Layout and Visual Design System

The Live interface should preserve MarketForge's existing Tokyo Night visual identity.

The design should prioritize dense but readable operational information.

### 6.1 Theme

Use the existing Tokyo Night theme implementation.

The design system should provide semantic color roles for:

    Background

    Foreground

    MutedText

    Borders

    Selection

    Focus

    PrimaryAccent

    SecondaryAccent

    Success

    Warning

    Error

    Disabled

Exact palette values should come from the existing theme rather than be independently redefined.

### 6.2 Visual Hierarchy

The interface should distinguish:

1. Current view and runtime identity.
2. Primary operational information.
3. Selected resource.
4. Contextual details.
5. Operational notifications.
6. Available keyboard actions.

The user should be able to identify critical failures without reading every metric.

### 6.3 Status Presentation

Health statuses should have consistent visual treatment.

Conceptually:

| Status | Presentation |
|---|---|
| Healthy | Success style |
| Degraded | Warning style |
| Unhealthy | Error style |
| Unknown | Neutral or muted warning |
| Disabled | Muted style |

Text labels must remain visible.

Color alone must not convey operational meaning.

### 6.4 Lifecycle Presentation

Lifecycle state is displayed separately from health.

Example:

    Lifecycle:
        Reconnecting

    Health:
        Degraded

The interface must not present these as interchangeable values.

### 6.5 Tables

Tables should support:

- Stable row identities.
- Keyboard selection.
- Vertical scrolling.
- Optional horizontal scrolling.
- Sorting.
- Filtering.
- Bounded rendering.
- Contextual row actions.

The table should not rebuild expensive presentation state on every terminal frame.

### 6.6 Column Widths

Column widths should account for terminal dimensions.

For example, the Streams table may prioritize:

    Exchange

    Instrument

    Channel

    Health

    Rate

    Latency

Lower-priority columns may be hidden in narrow terminals.

### 6.7 Detail Panels

A selected table row may open a contextual detail panel.

Conceptually:

    +---------------------------+----------------------------+
    | Streams                   | Stream Details             |
    |                           |                            |
    | > BTCUSDT Trades          | Exchange: Binance          |
    |   ETHUSDT Trades          | Market: Spot               |
    |   BTCUSDT Depth           | Health: Healthy            |
    |                           | Rate: 15,000/s             |
    |                           | Continuity: Verified       |
    +---------------------------+----------------------------+

This arrangement is preferred when sufficient terminal width is available.

### 6.8 Narrow Terminal Layout

On narrow terminals, the interface may use:

    Full-Width Table

followed by:

    Full-Width Detail View

rather than squeezing both into unreadable columns.

### 6.9 Minimum Terminal Requirements

The implementation must define a minimum supported terminal size.

When the terminal is smaller than that size:

- Avoid rendering outside terminal bounds.
- Display a compact resize message.
- Preserve application state.
- Continue processing control responses.
- Resume normal rendering when sufficient space becomes available.

### 6.10 Status Bar

The status bar should provide concise global information.

Examples:

    Runtime: Connected

    Overall Health: Degraded

    Active Operations: 2

    Config Revision: 42

A status bar should not attempt to display every metric.

### 6.11 Notifications

Operational notifications should be brief and actionable.

Examples:

    BTCUSDT Depth synchronized.

    Recording unavailable.

    Configuration revision conflict.

    OKX connection reconnecting.

Notifications must distinguish informational events from errors requiring attention.

### 6.12 Metric Formatting

Metrics should use consistent human-readable units.

Examples:

    15,250 events/s

    2.4 ms

    128 MiB

    3.2 GiB

    42%

The underlying Metrics Registry retains its own typed numerical representation.

### 6.13 Unavailable Values

The interface must distinguish:

    Zero

    Unavailable

    Disabled

    Not Applicable

Examples:

    Processing latency: N/A

    Recording: Disabled

    Sequence gaps: 0

The UI must not display missing measurements as zero.

### 6.14 Loading States

When a view is waiting for a control response:

    Loading...

may be displayed.

Loading must not freeze keyboard navigation.

If previously cached data is displayed, its freshness must remain visible.

### 6.15 Error States

A failed query should display:

    Error Category

    Human-Readable Reason

    Affected Operation

    Retry Option

where applicable.

The UI must not replace failed queries with empty tables that could be mistaken for valid zero-resource states.

### 6.16 Visual Consistency

The same resource must use consistent status and identity formatting across views.

For example, Binance BTCUSDT Trades should not appear healthy on the Dashboard while appearing unhealthy in Streams because the two views interpret different cached counters.

The TUI should use the same HealthEvaluator snapshot and preserve observation freshness.

### 6.17 Visual Design Invariant

    The Interface Must
    Prioritize Operational
    Meaning Over
    Decorative Complexity.

---

## 7. Dashboard View

The Dashboard provides a concise operational overview of MarketForge Live.

It is the default view displayed after the TUI successfully connects to the daemon.

The Dashboard must prioritize actionable information over comprehensive metric presentation.

### 7.1 Responsibilities

The Dashboard must display:

- Runtime connection status.
- Overall system health.
- Active exchange connections.
- Active source streams.
- Active derived streams.
- Aggregate event throughput.
- Processing latency.
- Current recovery activity.
- Recording status.
- Retention resource pressure.
- Recent significant operational incidents.

The Dashboard must not reconstruct health assessments from individual counters.

### 7.2 Dashboard Layout

Recommended layout:

    +----------------------------------------------------------------+
    | MARKETFORGE LIVE                                  CONNECTED     |
    +----------------------------------------------------------------+
    | Dashboard | Streams | Derived | Recording | Settings           |
    +----------------------------------------------------------------+
    |                                                                |
    |  RUNTIME          STREAMS          THROUGHPUT       HEALTH      |
    |  Running          30 Active        125,000/s        Degraded    |
    |                                                                |
    +--------------------------------+-------------------------------+
    | EXCHANGE CONNECTIONS           | SYSTEM RESOURCES              |
    |                                |                               |
    | Binance      3/3 Healthy       | Memory       2.4 GiB          |
    | Bybit        3/3 Healthy       | Retention    640 MiB          |
    | OKX          2/3 Degraded      | Recording    128 MiB Pending  |
    | Bitget       3/3 Healthy       | CPU          18%              |
    | Gate.io      3/3 Healthy       | Subscribers  4                |
    |                                |                               |
    +--------------------------------+-------------------------------+
    | STREAM HEALTH                  | ACTIVE RECOVERIES             |
    |                                |                               |
    | Healthy       28              | OKX BTCUSDT Depth             |
    | Degraded       2              |   L2 Resynchronization       |
    | Unhealthy      0              |                               |
    | Unknown        0              | PostgreSQL                    |
    |                                |   Recording Catch-Up          |
    +--------------------------------+-------------------------------+
    | RECENT EVENTS                                                  |
    | 12:03:41  WARN  OKX BTCUSDT Depth synchronization lost         |
    | 12:03:42  INFO  Recovery started                                |
    | 12:03:43  INFO  New book generation initialized                |
    +----------------------------------------------------------------+
    | [1-5] Views  [Tab] Focus  [Enter] Inspect  [?] Help  [q] Quit |
    +----------------------------------------------------------------+

All values are illustrative.

The actual layout must adapt to available terminal dimensions.

### 7.3 Dashboard Data Sources

The Dashboard consumes the following existing contracts:

| Display Element | Authoritative Source |
|---|---|
| Runtime state | Runtime Supervisor |
| Configuration revision | Configuration Manager |
| Connection counts | Connection Manager |
| Source-stream counts | Stream Registry |
| Derived-stream counts | Derived Stream Registry |
| Throughput | Metrics Registry |
| Processing latency | Metrics Registry |
| Health summaries | HealthEvaluator |
| Active recoveries | Recovery Coordinator |
| Recording state | Recording Manager |
| Retention pressure | Retention Manager |
| Recent incidents | Operational diagnostics |

The Dashboard must not independently maintain counters for runtime components.

### 7.4 Runtime Status

The runtime summary should display:

    Runtime Lifecycle

    Control Connection State

    Desired Configuration Revision

    Applied Configuration Revision

    Runtime Uptime

The TUI's connection to the daemon is distinct from the daemon's operational health.

For example:

    Control Connection:
        Connected

    Runtime Health:
        Degraded

This is a valid state.

### 7.5 Stream Counts

The Dashboard should distinguish:

    Registered Source Streams

    Active Source Streams

    Active Derived Streams

    Degraded Streams

    Unhealthy Streams

A source stream referenced by several derived streams must not be counted multiple times as a source acquisition.

### 7.6 Throughput

The primary throughput measurement is:

    Accepted Canonical Source Events / Second

Derived output throughput may be shown separately.

The Dashboard must not add source and derived event counts and describe the result as unique exchange events.

A source event may contribute to several derived outputs.

### 7.7 Latency

The primary latency display should identify its measurement boundary.

Recommended initial measurement:

    Local Receive To Canonical Acceptance

The Dashboard may display:

    Processing Latency p50

    Processing Latency p99

Detailed latency distributions may be unavailable when detailed timing is disabled.

The interface must not label exchange-to-local event delay as pure network latency.

### 7.8 Exchange Connection Summary

The Dashboard should present one row per exchange.

Recommended information:

    Exchange

    Active Connections

    Degraded Connections

    Recovering Connections

    Overall Connectivity Health

Selecting an exchange should open a contextual connection summary or navigate to the relevant filtered Streams view.

### 7.9 System Resource Summary

Initially useful measurements:

    Process Memory

    Retention Memory

    Retention Disk

    Publisher Queue Memory

    Recording Backlog

    Active Subscribers

CPU measurements may be omitted when unavailable.

The Dashboard must not display unsupported resource measurements as zero.

### 7.10 Stream Health Summary

The Dashboard displays health counts using the shared HealthEvaluator contract:

    Healthy

    Degraded

    Unhealthy

    Unknown

    Disabled

Source and derived streams should be distinguishable when both are included.

The total must not double-count one logical stream.

### 7.11 Active Recoveries

The Dashboard displays a bounded list of active recovery operations.

Each entry may include:

    Target Resource

    Recovery Category

    Current State

    Elapsed Recovery Time

    Next Retry

Selecting a recovery should open the relevant stream or recovery detail.

### 7.12 Recent Operational Events

The Dashboard displays a small bounded history of significant events.

Examples:

    ConnectionLost

    StreamDegraded

    BookInvalidated

    RecoveryStarted

    RecoveryCompleted

    RecordingFailed

    ConfigurationApplied

These are operational events, not canonical market-data events.

Ordinary trades and L2 updates must not appear in this history.

### 7.13 Dashboard Refresh

The Dashboard consumes periodic monitoring snapshots.

It should not issue one control query per displayed metric on every render frame.

The preferred model is:

    Periodic Snapshot Fetch
              |
              v
    Update Local Snapshot Cache
              |
              v
    Render From Cached State

Rendering frequency and snapshot collection frequency remain independent.

### 7.14 Dashboard Failure State

If monitoring data becomes unavailable:

- Preserve the last known snapshot when useful.
- Mark the snapshot as stale.
- Display the control connection status.
- Avoid presenting stale values as current.
- Continue accepting local navigation input.

### 7.15 Dashboard Invariant

    The Dashboard
    Summarizes Existing
    Runtime State.

    It Does Not Become
    An Independent
    Monitoring Authority.

---

## 8. Source Streams View

The Streams view provides detailed management and inspection of canonical source streams.

It is the primary interface for adding, activating, deactivating, removing, and diagnosing market-data subscriptions.

### 8.1 Responsibilities

The Streams view must support:

- Listing registered source streams.
- Filtering by exchange and market family.
- Filtering by channel.
- Filtering by lifecycle and health.
- Searching instruments.
- Sorting by operational measurements.
- Inspecting stream configuration.
- Inspecting stream health.
- Viewing trade continuity.
- Viewing L2 synchronization.
- Viewing connection assignments.
- Adding source streams.
- Activating and deactivating streams.
- Removing source streams.
- Enabling and disabling recording.
- Inspecting pending stream operations.

### 8.2 Streams Layout

Recommended wide-terminal layout:

    +--------------------------------------------------------------------------+
    | STREAMS                                                                  |
    +--------------------------------------------------------------------------+
    | Filter: All Exchanges | All Markets | All Channels | Health: All         |
    | Search: BTC                                                             |
    +------------------------------------------------+-------------------------+
    | EXCHANGE  MARKET  SYMBOL   TYPE   RATE   HEALTH | STREAM DETAILS          |
    |------------------------------------------------|-------------------------|
    | Binance   Spot    BTCUSDT  Trade  15K/s  Healthy| Binance BTCUSDT         |
    | Binance   Spot    BTCUSDT  L2      4K/s  Healthy|                         |
    | Bybit     Spot    BTCUSDT  Trade  12K/s  Healthy| Channel: Trades         |
    | Bybit     Spot    BTCUSDT  L2      3K/s  Healthy| Connection: Connected   |
    | OKX       Spot    BTCUSDT  Trade   9K/s  Healthy| Health: Healthy         |
    | OKX       Spot    BTCUSDT  L2      2K/s  Degraded| Continuity: Verified    |
    |                                                | Rate: 15,000/s          |
    |                                                | Latency p99: 2.1 ms     |
    |                                                | Recording: Disabled     |
    +------------------------------------------------+-------------------------+
    | [a] Add  [Enter] Details  [e] Enable  [d] Disable  [r] Remove             |
    +--------------------------------------------------------------------------+

All values are illustrative.

### 8.3 Stream Table Columns

Recommended columns:

| Column | Purpose |
|---|---|
| Exchange | Exchange identity |
| Market | Spot, Linear, Inverse |
| Instrument | Canonical instrument display |
| Channel | Trades or L2 |
| Lifecycle | Current stream lifecycle |
| Health | Overall stream health |
| Rate | Accepted canonical events per second |
| Latency | Selected processing latency |
| Recording | Recording status |

Not every column must be visible simultaneously.

The layout should adapt to terminal width.

### 8.4 Stable Stream Identity

Table rows must be keyed by authoritative StreamId values.

The TUI must not identify a stream solely through its visible row index.

Sorting, filtering, or refreshing the table must not accidentally redirect an operation to another stream.

### 8.5 Stream Selection

The selected stream remains identified by StreamId.

When new snapshots arrive:

- Preserve selection if the stream still exists.
- Update its displayed measurements.
- Recalculate its visible row position.
- Clear selection safely if it has been removed.

### ### 8.6 Stream Lifecycle Presentation

The Streams view must distinguish logical stream lifecycle,
connection lifecycle, acquisition readiness, and health.

For example:

    StreamLifecycle:     Active
    ConnectionState:     Reconnecting
    AcquisitionReadiness: Not Ready
    Health:              Degraded

This represents a running logical source-stream runtime
whose physical connection is recovering.

Another example:

    StreamLifecycle:     Active
    ConnectionState:     Connected
    AcquisitionReadiness: Ready
    Health:              Unhealthy

This is possible when an independently required operational
guarantee is failing despite current acquisition readiness.

These states are not contradictory.

Ratatui must not represent `Reconnecting` or `Synchronizing`
as `StreamLifecycle` variants.

Operational observations must retain their authoritative
state dimensions.

### 8.7 Stream Readiness

A stream must not be presented as fully operational before its channel-specific readiness conditions are established.

For trades:

    Subscription Active

    Processor Ready

For L2:

    Subscription Active

    Snapshot Initialized

    Book Synchronized

The Stream Registry remains authoritative for lifecycle readiness.

### 8.8 Source Identity Display

The detail panel should expose:

    StreamId

    Exchange

    MarketFamily

    InstrumentId

    NativeSymbol

    Channel

    ChannelParameters

The TUI may display a human-readable symbol, but management commands must use the authoritative stream identity.

### 8.9 Configuration Revision

A stream detail should expose:

    DesiredConfigurationRevision

    AppliedConfigurationRevision

If the revisions differ, the UI should indicate that configuration reconciliation remains pending or degraded.

### 8.10 Recording Status

The Streams view may show:

    Recording Enabled

    Recording Disabled

    Recording Recovering

    Recording Failed

The display must distinguish configured recording intent from actual persistence health.

### 8.11 Stream Dependencies

A source may be required by:

    Persistent Configuration

    Recording

    Unix Subscribers

    Derived Streams

    Retention Reservations

The Streams view should expose a bounded summary of dependency ownership.

This helps explain why a source remains active after a particular consumer is removed.

### 8.12 Streams View Invariant

    Source Streams
    Are Identified By
    Authoritative Registry
    Identities.

    UI Row Position
    Must Never Become
    A Runtime Identity.

---

## 9. Stream Filtering, Sorting, and Search

The Streams view must remain usable when many source streams are registered.

Filtering and sorting should be fast and predictable.

### 9.1 Exchange Filter

Supported values include:

    All

    Binance

    Bybit

    OKX

    Bitget

    Gate.io

The filter operates on existing stream descriptors.

### 9.2 Market Filter

Supported values:

    All

    Spot

    Linear

    Inverse

Market filtering must use the canonical market classification rather than guess from symbol strings.

### 9.3 Channel Filter

Initially supported:

    All

    Trades

    L2

Future channels may be added through the existing channel capability model.

### 9.4 Health Filter

Supported values:

    All

    Healthy

    Degraded

    Unhealthy

    Unknown

    Disabled

Health filtering must use the shared HealthEvaluator assessment.

### 9.5 Lifecycle Filter

The user may optionally filter streams by their logical
runtime lifecycle.

The lifecycle filter uses the authoritative `StreamLifecycle`
defined in Document 02, Section 7.2.

Supported values are:

    Registered
    Starting
    Active
    Idle
    Stopping
    Stopped
    Failed

The Streams view may additionally provide separate filters for:

- Physical connection state, including `Reconnecting`.
- Market-data integrity, including `Synchronizing`.
- Persistent desired acquisition state, including `Inactive`.
- Acquisition readiness.
- Recovery progress.
- Health classification.

These filters must not introduce additional logical
`StreamLifecycle` variants.

A stream may match multiple independent operational filters
simultaneously.

For example:

    StreamLifecycle: Active
    ConnectionState: Reconnecting
    PersistentDesiredState: Inactive
    EffectiveAcquisitionRequired: true

This is valid when independent demand continues to require
the source after persistent acquisition has been disabled.

Health and lifecycle filters must remain independent.

### 9.6 Instrument Search

The Streams view should support case-insensitive search using:

    Display Symbol

    Native Symbol

    Exchange

    StreamId

Search must not modify authoritative runtime state.

### 9.7 Sorting

Recommended sort options:

    Exchange

    Instrument

    Channel

    Health

    Event Rate

    Processing Latency

    Recording State

    Last Activity

Sorting should be deterministic.

Equal values should use a stable secondary key such as StreamId.

### 9.8 Unavailable Metric Sorting

Unavailable measurements must not be interpreted as zero.

For example:

    Latency = Unavailable

should be handled separately from:

    Latency = 0 ns

The sort order for unavailable values must be consistent.

### 9.9 Local vs Server Filtering

For small bounded stream collections, the TUI may filter and sort local snapshots.

For large collections, the Control API may provide server-side filtering and pagination.

The choice must not change query semantics.

### 9.10 Refresh Stability

Frequent metric changes can reorder tables unexpectedly.

The initial interface should avoid unstable automatic sorting while the user is navigating a selected row.

Possible policy:

    Preserve Selected StreamId

    Reorder Only On Snapshot Refresh

    Keep Selected Row Visible

The user should not accidentally act on another stream because its throughput changed.

### 9.11 Filtering Invariant

    Filtering And Sorting
    Affect Presentation Only.

    They Must Not Change
    Source Acquisition
    Or Stream Ownership.

---

## 10. Trade Stream Detail Panel

Trade streams require diagnostics focused on accepted executions, continuity, deduplication, and recovery.

The panel consumes Stream Registry, Trade Processor, Metrics Registry, and Recovery Coordinator snapshots.

### 10.1 Trade Detail Layout

Conceptually:

    +---------------------------------------------------------+
    | BINANCE BTCUSDT — TRADES                                |
    +---------------------------------------------------------+
    | StreamId: binance_spot_btcusdt_trades                    |
    | Market: Spot                                            |
    |                                                         |
    | Lifecycle: Active          Health: Healthy              |
    | Connection: Connected      Subscription: Active         |
    |                                                         |
    | THROUGHPUT                                               |
    | Accepted Trades: 15,250/s                               |
    | Received Messages: 15,300/s                             |
    |                                                         |
    | LATENCY                                                  |
    | Processing p50: 0.4 ms                                  |
    | Processing p99: 1.8 ms                                  |
    |                                                         |
    | INTEGRITY                                                |
    | Continuity: Verified                                    |
    | Duplicates: 12                                          |
    | Detected Gaps: 0                                        |
    | Unresolved Gaps: 0                                      |
    |                                                         |
    | RECOVERY                                                 |
    | State: Idle                                             |
    |                                                         |
    | RECORDING                                                |
    | Enabled: No                                             |
    +---------------------------------------------------------+

All values are illustrative.

### 10.2 Required Identity Information

Display:

    StreamId

    Exchange

    MarketFamily

    Instrument

    Channel

    ConnectionId

    SubscriptionGeneration

    AppliedConfigurationRevision

### 10.3 Throughput Measurements

Recommended measurements:

    TransportMessagesReceivedPerSecond

    DecodedTradeCandidatesPerSecond

    AcceptedCanonicalTradesPerSecond

The interface must not label transport messages as accepted trades.

One transport message may contain multiple native executions.

### 10.4 Continuity State

Display the authoritative trade continuity classification.

Possible states include:

    Verified

    PartiallyVerified

    Unverified

    Incomplete

The panel must not infer continuity from throughput or the absence of recent recovery activity.

### 10.5 Gap Diagnostics

Display:

    ContinuityFailuresTotal

    UniqueDetectedGaps

    UnresolvedGapCount

    OldestUnresolvedGapAge

where available.

The panel should distinguish:

    Currently Unresolved Gaps

from:

    Historical Gap Incidents

A previously recovered gap may remain in cumulative incident counters.

### 10.6 Duplicate Diagnostics

Display the number of duplicate executions detected by the Trade Processor.

The TUI must not implement independent duplicate detection.

### 10.7 Recovery Information

If trade recovery is active, display:

    RecoveryOperationId

    RecoveryState

    RecoveryAttempt

    RecoveryTargetInterval

    RecoveryProgress

    LastFailure

Progress must reflect authoritative recovery state.

### 10.8 Event-Time Information

Display:

    LastAcceptedEventTimestamp

    LastAcceptedReceiveTimestamp

    LastRecoveredEventTimestamp

These timestamps must retain their original meanings.

A recently recovered execution may have an old event timestamp.

### 10.9 Latency Information

Recommended fields:

    ReceiveToCanonicalAcceptance

    CanonicalAcceptanceToPublication

    ExchangeEventToLocalReceive

The third measurement must be explicitly labeled as clock-sensitive event-to-receive delay.

### 10.10 Trade Integrity Warning

If execution continuity is unverified:

    Continuity:
        Unverified

The panel should indicate the relevant reason without declaring a confirmed missing execution unless the Trade Processor provides that evidence.

### 10.11 Trade Detail Invariant

    Trade Detail
    Presents Authoritative
    Continuity And Recovery
    Information.

    It Does Not Reconstruct
    Trade Integrity.

---

## 11. L2 Stream Detail Panel

L2 streams require diagnostics focused on initialization, sequence continuity, book generations, and current book validity.

The panel consumes the shared BookStore synchronization state and Metrics Registry measurements.

### 11.1 L2 Detail Layout

Conceptually:

    +---------------------------------------------------------+
    | BINANCE BTCUSDT — L2 DEPTH                              |
    +---------------------------------------------------------+
    | StreamId: binance_spot_btcusdt_depth                     |
    | Market: Spot                                            |
    |                                                         |
    | Lifecycle: Active          Health: Healthy              |
    | Connection: Connected      Subscription: Active         |
    |                                                         |
    | BOOK STATE                                              |
    | Synchronized: Yes                                       |
    | Book Generation: 4                                      |
    | Bid Levels: 1000                                        |
    | Ask Levels: 1000                                        |
    | Best Bid: 65000.10                                     |
    | Best Ask: 65000.20                                     |
    | Spread: 0.10                                            |
    |                                                         |
    | THROUGHPUT                                               |
    | Depth Messages: 450/s                                   |
    | Level Changes: 4,800/s                                  |
    |                                                         |
    | INTEGRITY                                                |
    | Sequence Gaps: 0                                        |
    | Synchronization Failures: 0                             |
    | Last Valid Update: 12:05:14                             |
    |                                                         |
    | RECOVERY                                                 |
    | State: Idle                                             |
    +---------------------------------------------------------+

All values are illustrative.

### 11.2 Book Synchronization State

Display the authoritative synchronization state.

Conceptually:

    Uninitialized

    Synchronizing

    Synchronized

    Invalid

    Recovering

The exact external status names must remain compatible with Document 06.1.

### 11.3 Book Generation

The panel must display:

    CurrentBookGeneration

    InitializationTimestamp

    LastValidUpdateTimestamp

A book generation identifies one authoritative synchronization lifecycle.

It is not equivalent to a WebSocket connection generation.

### 11.4 Book Level Counts

Useful measurements:

    BidLevelCount

    AskLevelCount

These represent levels currently maintained within the subscribed book scope.

They do not imply that MarketForge possesses the exchange's entire order book.

### 11.5 Best Bid and Ask

When the book is synchronized, the panel may display:

    BestBid

    BestAsk

    Spread

Values must use the existing exact Decimal formatting conventions.

The TUI must not calculate prices through floating-point transformations that introduce misleading precision.

### 11.6 Invalid Book State

When the book is invalid:

    Book Synchronization:
        Invalid

The TUI must not present old best bid and ask values as current authoritative market data.

It may display:

    Last Known Values

with an explicit stale or invalid label.

### 11.7 Sequence Diagnostics

Display:

    SequenceGapsDetectedTotal

    SequenceValidationFailuresTotal

    LastAcceptedSequence

where supported.

Native sequence information must retain exchange-specific semantics.

The TUI must not treat unrelated sequence counters as directly comparable across exchanges.

### 11.8 Snapshot Diagnostics

Recommended fields:

    SnapshotsReceivedTotal

    InitializationsCompletedTotal

    LastInitializationTimestamp

    SnapshotApplyLatency

A received snapshot does not necessarily represent a successfully synchronized book.

### 11.9 Depth Processing Metrics

Display:

    DepthMessagesReceivedPerSecond

    DepthEventsAcceptedPerSecond

    LevelChangesEmittedPerSecond

The interface must distinguish one native depth message from its constituent price-level changes.

### 11.10 L2 Recovery Status

During synchronization recovery, display:

    RecoveryOperationId

    RecoveryState

    SnapshotRequestState

    CurrentBookGeneration

    SynchronizationProgress

The UI must not invent a numerical percentage when the synchronization process lacks a meaningful progress denominator.

### 11.11 Book Invalidation

When a sequence gap invalidates the book:

    Book Generation:
        Invalid

    Recovery:
        Resynchronizing

    Current Book:
        Unavailable

The interface should explain that a new snapshot may restore current book validity without reconstructing every missed historical L2 update.

### 11.12 Manual Resynchronization

If manual book resynchronization is exposed, the action must:

- Require appropriate authorization.
- Display the affected stream.
- Explain the possible book-generation transition.
- Submit a validated Control API request.
- Track the returned operation.
- Display the final authoritative synchronization outcome.

The TUI must not mutate BookStore state directly.

### 11.13 L2 Detail Invariant

    Book Readiness
    Is Determined By
    Authoritative
    Synchronization State.

    A Nonempty Book
    Is Not Necessarily
    A Valid Book.

---

## 12. Source Stream Management Interactions

Source-stream operations are initiated through dialogs and contextual actions.

All mutations must use the Runtime Control API.

### 12.1 Supported Actions

The initial Streams view supports:

    Add Stream

    Activate Stream

    Deactivate Stream

    Remove Stream

    Inspect Stream

    Enable Recording

    Disable Recording

    Inspect Dependencies

    Inspect Recovery

Some actions may require additional permissions.

### 12.2 Add Stream Workflow

Recommended interaction:

    User Presses [a]
           |
           v
    Open Add Stream Dialog
           |
           v
    Select Exchange
           |
           v
    Select Market Family
           |
           v
    Select Instrument
           |
           v
    Select Channel
           |
           v
    Configure Channel Parameters
           |
           v
    Configure Recording
           |
           v
    Validate Draft
           |
           v
    Display Configuration Impact
           |
           v
    Confirm Submission
           |
           v
    Control API
           |
           v
    Runtime Reconciliation

The dialog must not create an exchange subscription before validation and submission.

### 12.3 Add Stream Fields

Recommended fields:

| Field | Input |
|---|---|
| Exchange | Selection |
| Market family | Selection |
| Instrument | Searchable selection |
| Channel | Selection |
| Depth | Conditional numeric input |
| Update interval | Conditional selection |
| Enabled | Toggle |
| Recording | Toggle |

Only channel parameters supported by the selected exchange profile should be displayed.

### 12.4 Exchange-Aware Form Validation

The form must use compiled exchange capabilities.

For example:

    Exchange:
        Binance

    Market:
        Spot

    Channel:
        L2

Available depth and update-frequency options must reflect the corresponding protocol profile.

The TUI must not maintain a separate hardcoded table of exchange capabilities.

### 12.5 Instrument Selection

Instrument choices must originate from the validated instrument catalog snapshot or authoritative runtime instrument registry.

The user should not manually enter contract values, quantity denominations, or normalization-critical metadata.

If the selected instrument is unavailable, the UI must display the relevant catalog or capability error.

### 12.6 Source Creation Result

After successful submission, the TUI should display:

    Configuration Revision Accepted

    Stream Registration:
        Completed

    Stream Activation:
        Applying

The user must be able to inspect operation progress.

### 12.7 Activate Stream

Activation requests the configured stream to become operational.

The TUI must distinguish:

    Request Accepted

from:

    Stream Ready

For L2 streams, readiness requires successful book synchronization.

### 12.8 Deactivate Stream

Deactivation must display the intended effect.

If a source has other active demand owners, the UI must not promise that its physical subscription will stop.

Example:

    Persistent Activation:
        Disabled

    Active Derived Dependencies:
        2

    Physical Acquisition:
        May Remain Active

The authoritative Stream Registry determines the final acquisition lifecycle.

### 12.9 Remove Stream

Removal is potentially disruptive.

The TUI must display:

    Stream Identity

    Current Lifecycle

    Active Dependencies

    Recording State

    Expected Resource Changes

If required dependencies prevent removal, the UI must display the dependency conflict.

### 12.10 Removal Confirmation

Removal requires confirmation.

The dialog should identify the exact stream being removed.

Example:

    Remove Stream?

    Binance Spot BTCUSDT Trades

    This removes its persistent
    source definition.

    Active derived dependencies
    may prevent removal.

    [Confirm] [Cancel]

The confirmation must be tied to the selected StreamId, not the current table row number.

### 12.11 Enable Recording

Enabling recording must display the selected stream and effective recording policy.

The UI should explain that recording begins at an explicit activation boundary.

Previously accepted events are not automatically made durable.

### 12.12 Disable Recording

Disabling recording must distinguish:

    Stop New Recording Admission

from:

    Complete Existing Pending Commits

The Recording Manager determines the final durable frontier.

### 12.13 Manual Recovery Actions

Manual recovery actions should be available only when supported by the selected stream type and protocol capabilities.

Potential actions:

    Request Trade Recovery

    Request L2 Resynchronization

    Request Connection Restart

These commands must use Document 13's authorization and recovery contracts.

### 12.14 Asynchronous Action Feedback

Every long-running action must expose:

    OperationId

    OperationState

    AffectedResource

    CurrentProgress

    LatestError

The TUI should not freeze while the operation is running.

### 12.15 Operation Completion

The UI must display a successful outcome only when the Control API reports the operation's declared completion condition.

For example:

    L2 Stream Activated

requires authoritative synchronization readiness.

### 12.16 Revision Conflict

If a configuration mutation encounters a revision conflict:

- Preserve the user's local draft.
- Display the current server revision.
- Explain that another client modified the configuration.
- Allow the user to refresh and revalidate.
- Avoid silently overwriting the newer revision.

### 12.17 Stream Management Invariant

    The Streams View
    Submits Validated
    Management Requests.

    The Stream Registry
    And Runtime Reconciler
    Execute Their Effects.

---

## 13. Derived Streams View

The Derived Streams view provides a unified interface for inspecting and managing MarketForge Live transformations.

Initially supported transformations include:

- Cross-exchange event merging.
- Trade-only merging.
- L2 depth-only merging.
- Combined trade and depth timelines.
- Currency conversion.
- Derived streams depending on other derived streams.

The interface must reuse the authoritative derived-stream contracts established in Document 10.

### 13.1 Responsibilities

The Derived Streams view must support:

- Listing registered derived streams.
- Inspecting transformation types.
- Inspecting source dependencies.
- Viewing lifecycle and health.
- Monitoring input and output throughput.
- Inspecting ordering and watermark state.
- Inspecting conversion-reference availability.
- Creating derived streams.
- Activating and deactivating derived streams.
- Editing supported transformation policies.
- Removing derived streams.
- Inspecting dependent streams.
- Viewing pending operations and recovery state.

### 13.2 Derived Streams Layout

Recommended wide-terminal layout:

    +--------------------------------------------------------------------------+
    | DERIVED STREAMS                                                          |
    +--------------------------------------------------------------------------+
    | Filter: All Types | Health: All | Search: BTC                            |
    +------------------------------------------------+-------------------------+
    | NAME                         TYPE    HEALTH     | DERIVED DETAILS         |
    |------------------------------------------------|-------------------------|
    | > btc_spot_trades_merged       Merge   Healthy   | BTC Spot Trades Merge  |
    |   btc_spot_depth_merged        Merge   Healthy   |                         |
    |   binance_btc_spot_combined    Merge   Healthy   | Mode: Trades            |
    |   btc_spot_trades_extended     Merge   Degraded  | Sources: 3/3 Active     |
    |   bybit_ethbtc_to_usdt         Convert Disabled  | Input: 24,000/s         |
    |   bybit_btcusdc_to_usdt        Convert Disabled  | Output: 23,800/s        |
    |                                                | Watermark Lag: 18 ms    |
    |                                                | Ordering: Operational   |
    |                                                | Recording: Disabled     |
    +------------------------------------------------+-------------------------+
    | [a] Create  [Enter] Inspect  [e] Enable  [d] Disable  [r] Remove         |
    +--------------------------------------------------------------------------+

All values are illustrative.

### 13.3 Derived Table Columns

Recommended columns:

| Column | Purpose |
|---|---|
| Name | Derived stream identifier or display name |
| Type | Merge or conversion |
| Mode | Trades, Depth, Combined, or applicable conversion mode |
| Lifecycle | Current execution state |
| Health | Overall derived health |
| Sources | Available required dependencies |
| Input Rate | Input event throughput |
| Output Rate | Produced derived-event throughput |
| Recording | Recording status |

The view may hide lower-priority columns when terminal width is limited.

### 13.4 Stable Derived Identity

Rows must be identified by authoritative DerivedStreamId values.

The TUI must not derive runtime identity from:

- Display names.
- Current row positions.
- Sorting order.
- Source symbols.
- Transformation descriptions.

### 13.5 Derived Stream Selection

Selecting a derived stream displays:

    DerivedStreamId

    TransformationType

    RuntimeGeneration

    DesiredState

    LifecycleState

    OverallHealth

    AppliedConfigurationRevision

    SourceDependencies

The selected identity must remain stable across filtering and sorting.

### 13.6 Derived Stream Types

The initial interface supports:

    Merge

    Conversion

The UI should expose only transformation types supported by the runtime's validated capability model.

A future transformation should not require redesigning the entire navigation system.

### 13.7 Derived Lifecycle

The TUI must distinguish:

    Registered

    Starting

    Active

    Degraded

    Recovering

    Stopping

    Disabled

    Failed

The displayed lifecycle vocabulary must follow the authoritative derived-stream state contract.

These values must not be confused with the separate HealthStatus classification.

### 13.8 Derived Health

Derived health originates from the HealthEvaluator.

Relevant dimensions include:

    SourceParticipation

    InputIntegrity

    Ordering

    Conversion

    Publication

    Recording

    Retention

Not every dimension applies to every transformation.

### 13.9 Input and Output Rates

The view should distinguish:

    InputEventsPerSecond

    OutputEventsPerSecond

Input throughput measures admitted upstream events.

Output throughput measures produced derived events.

The two rates may differ because of:

- Event-time buffering.
- Watermark stalls.
- Missing source participation.
- Transformation eligibility.
- Processing failures.
- Recovery transitions.

A low output rate does not automatically establish event loss.

### 13.10 Source Availability

The view should display:

    RequiredSources

    AvailableSources

    DegradedSources

    UnavailableSources

A source may be connected while failing an integrity requirement.

Therefore, source participation must use authoritative dependency capabilities rather than connection status alone.

### 13.11 Derived View Refresh

The view consumes bounded snapshots from the Runtime Control API.

It must not inspect canonical events or derived output buffers directly.

### 13.12 Derived View Invariant

    Derived Stream Management
    Must Remain Independent
    Of Transformation Execution.

---

## 14. Merge Detail Panel

The Merge Detail Panel exposes the operational state of one combined stream.

It must preserve the distinction between combining event timelines and consolidating economic market state.

### 14.1 Merge Detail Layout

Conceptually:

    +-----------------------------------------------------------+
    | BTC SPOT TRADES MERGED                                    |
    +-----------------------------------------------------------+
    | ID: btc_spot_trades_merged                                |
    |                                                           |
    | Lifecycle: Active           Health: Healthy               |
    | Generation: 3                                             |
    |                                                           |
    | TRANSFORMATION                                            |
    | Mode: Trades                                              |
    | Ordering: Adaptive Event Time                             |
    | Late Policy: Emit Late                                    |
    |                                                           |
    | SOURCES                                                   |
    | Binance BTCUSDT Trades      Healthy                       |
    | Bybit BTCUSDT Trades        Healthy                       |
    | OKX BTCUSDT Trades          Healthy                       |
    |                                                           |
    | ORDERING                                                  |
    | Watermark: 12:00:00.125                                   |
    | Watermark Lag: 18 ms                                      |
    | Buffered Events: 1,240                                   |
    | Late Events: 4                                           |
    |                                                           |
    | THROUGHPUT                                                |
    | Input: 24,000/s                                          |
    | Output: 23,800/s                                         |
    |                                                           |
    | PUBLICATION                                              |
    | State: Healthy                                           |
    |                                                           |
    | RECORDING                                                |
    | Disabled                                                 |
    +-----------------------------------------------------------+

All values are illustrative.

### 14.2 Supported Merge Modes

The interface supports the merge modes established in Document 10:

    Trades

    Depth

    Combined

**Trades**

Combines eligible canonical trade events.

**Depth**

Combines eligible canonical L2 events while preserving their original source-book identities and logical message boundaries.

**Combined**

Produces one ordered timeline containing eligible trades and L2 events.

### 14.3 Depth Merge Semantics

A depth merge is not automatically a consolidated order book.

For example:

    Binance BTCUSDT Book
              |
              +------+
                     |
    Bybit BTCUSDT Book
              |      |
              +------+----> Combined L2 Event Timeline
                     |
    OKX BTCUSDT Book |
              +------+

The resulting stream preserves source-specific L2 identity.

The TUI must not label this output as a unified order book unless a future explicitly defined transformation implements that operation.

### 14.4 Merge Identity

The panel displays:

    DerivedStreamId

    RuntimeGeneration

    TransformationRevision

    AppliedConfigurationRevision

These identities must remain distinct.

### 14.5 Input Sources

The panel lists required dependencies.

Each dependency displays:

    StreamId

    StreamType

    RequiredCapability

    LifecycleState

    HealthStatus

    ParticipationStatus

The TUI must not assume that every dependency is a direct exchange source.

A dependency may itself be another derived stream.

### 14.6 Source Ordering

The panel should display the configured deterministic source order.

For example:

    Source Rank 0:
        Binance BTCUSDT Trades

    Source Rank 1:
        Bybit BTCUSDT Trades

    Source Rank 2:
        OKX BTCUSDT Trades

Source ranking is relevant to deterministic tie-breaking.

The UI must not silently reorder configured source ranks when sorting their visual presentation.

### 14.7 Ordering Policy

The panel exposes the effective ordering strategy.

Examples:

    Arrival

    AdaptiveEventTime

The runtime may support additional validated ordering modes in the future.

Only supported policy values should be editable.

### 14.8 Watermark State

For event-time ordering, display:

    CombinedWatermark

    PerSourceWatermarks

    WatermarkLag

    WatermarkStallDuration

    WatermarkLimitingSource

Watermark values are event-time frontiers.

The TUI must not label them as local processing timestamps.

### 14.9 Watermark Lag

The UI must identify the reference used for a lag measurement.

Examples:

    EventTimeProgressLag

    WallClockRelativeWatermarkAge

These are different measurements.

The initial detail panel should prefer event-time progress lag.

### 14.10 Reorder Buffer

Display:

    BufferedEvents

    BufferedBytes

    BufferCapacity

    BufferOccupancy

    OldestBufferedEventAge

The Merge Engine remains responsible for buffer allocation and admission.

### 14.11 Late Events

Display:

    LateEventsDetected

    LateEventsPublished

    LateEventsRejected

    OrderingViolations

An event successfully reordered inside the configured buffer is not automatically a late-event policy violation.

### 14.12 Source Failure Policy

Display the effective source failure policy.

Examples:

    ContinueDegraded

    PauseUntilRecovered

    FailDerivedStream

The exact names and supported behavior must follow Document 10.

The interface must not infer a failure policy from current source health.

### 14.13 Merge Health

Merge health may be degraded because of:

- Missing required sources.
- Unresolved input integrity.
- Watermark stalls.
- Reorder buffer pressure.
- Late-event policy violations.
- Publication failures.

The panel should expose the corresponding structured health reasons.

### 14.14 Generation Transitions

When a merge is reconfigured, display:

    PreviousGeneration

    CurrentGeneration

    TransformationRevision

    TransitionReason

A change to source ranking or ordering semantics may establish a new derived generation.

### 14.15 Merge Detail Invariant

    A Combined Stream
    Preserves Declared
    Source Identity,
    Ordering Semantics,
    And Transformation
    Provenance.

---

## 15. Currency Conversion Detail Panel

The Currency Conversion Detail Panel exposes the configuration and runtime status of one conversion transformation.

It must distinguish an event's conversion outcome from the overall health of the conversion runtime.

### 15.1 Conversion Detail Layout

Conceptually:

    +-----------------------------------------------------------+
    | BYBIT ETH/BTC → ETH/USDT                                  |
    +-----------------------------------------------------------+
    | ID: bybit_ethbtc_to_usdt                                  |
    |                                                           |
    | Lifecycle: Active           Health: Healthy               |
    |                                                           |
    | CONVERSION                                                |
    | Source: Bybit ETHBTC Trades                               |
    | Target Currency: USDT                                     |
    | Reference Policy: Event Time                              |
    |                                                           |
    | RESOLVED PATH                                             |
    | ETH/BTC                                                   |
    |    × BTC/USDT                                             |
    |    = ETH/USDT                                             |
    |                                                           |
    | REFERENCES                                                |
    | Bybit BTCUSDT Trades       Available                      |
    |                                                           |
    | FRESHNESS                                                 |
    | Maximum Reference Age: 5 seconds                          |
    | Current Reference Age: 120 ms                             |
    |                                                           |
    | PROCESSING                                                |
    | Evaluated: 1,200/s                                       |
    | Converted: 1,198/s                                       |
    | Unavailable: 2/s                                         |
    |                                                           |
    | PUBLICATION                                              |
    | Healthy                                                   |
    +-----------------------------------------------------------+

All values are illustrative.

### 15.2 Conversion Identity

Display:

    DerivedStreamId

    SourceStreamId

    RuntimeGeneration

    TransformationRevision

    AppliedConfigurationRevision

The conversion stream has an independent identity and cursor namespace.

### 15.3 Source Instrument

The panel displays:

    Source Exchange

    Source Market

    Source Instrument

    Source Base Asset

    Source Quote Asset

These fields must originate from the authoritative instrument metadata.

### 15.4 Target Currency

The conversion target must be selected from supported canonical currency identities.

The TUI must not interpret arbitrary user strings as validated currency identifiers.

### 15.5 Conversion Path

The panel should display the resolved conversion path.

For example:

    ETH/BTC
        |
        v
    BTC/USDT
        |
        v
    ETH/USDT

The path must originate from the Currency Conversion Engine's validated reference graph.

### 15.6 Direct Conversion

A conversion may require one eligible reference observation.

Example:

    ETH/BTC × BTC/USDT = ETH/USDT

The interface should show:

    Source:
        ETHBTC

    Reference:
        BTCUSDT

    Target:
        ETHUSDT

### 15.7 Multi-Hop Conversion

Some conversions may require multiple reference observations.

The detail panel should support displaying each required reference leg.

Conceptually:

    Source Instrument
           |
           v
    Reference Leg 1
           |
           v
    Reference Leg 2
           |
           v
    Target Currency

Each leg must preserve its source and conversion direction.

The UI must not assume that every conversion path consists of exactly one reference stream.

### 15.8 Reference Source Health

For each reference dependency, display:

    ReferenceStreamId

    RequiredCapability

    CurrentAvailability

    ReferenceAge

    FreshnessLimit

    LastEligibleObservation

Reference health must follow the conversion engine's event-time eligibility rules.

### 15.9 Event-Time Reference Policy

Under event-time reference selection, the conversion engine chooses references according to the target event's timestamp.

The UI should display:

    ReferencePolicy:
        EventTime

    MaximumReferenceAge:
        Configured Duration

The TUI must not use the latest received reference simply because it arrived most recently.

### 15.10 Reference Freshness

The panel may display:

    Fresh

    Stale

    Missing

    Unavailable

These are reference diagnostics, not necessarily overall stream-health classifications.

### 15.11 Missing Conversion Path

A conversion may fail configuration validation because no compatible reference path exists.

This must be distinguished from a valid conversion path whose reference observations are temporarily unavailable.

Example:

    Path Resolution:
        Failed

    Reason:
        Required USDC/USDT reference
        cannot be resolved.

This is different from:

    Path Resolution:
        Valid

    Reference State:
        Temporarily Stale

### 15.12 Conversion Outcomes

The interface must preserve historical-compatible conversion statuses:

    Identity

    Converted

    Unavailable

    Skipped

These are event-level statuses.

They must not be used as replacements for runtime health.

### 15.13 Conversion Metrics

Relevant measurements include:

    EventsEvaluated

    EventsConverted

    IdentityConversions

    ConversionUnavailable

    ConversionSkipped

    MissingReferenceFailures

    StaleReferenceFailures

    ArithmeticFailures

The panel may display rates and cumulative counts.

### 15.14 Numerical Semantics

The Currency Conversion Engine uses exact Decimal arithmetic according to existing MarketForge historical semantics.

The TUI must not introduce an independent floating-point conversion implementation.

Displayed converted values are diagnostic outputs provided by the runtime.

### 15.15 Original Values

Conversion must preserve the original canonical source values.

The panel may show:

    Original Currency

    Target Currency

    Transformation Status

It must not suggest that conversion mutates the original trade or L2 event.

### 15.16 Conversion Health

Conversion health may be degraded when:

- Required reference streams are unavailable.
- References are repeatedly stale.
- Conversion paths cannot be resolved.
- Numerical conversion fails.
- Publication is impaired.

The HealthEvaluator remains authoritative for runtime health.

### 15.17 Conversion Detail Invariant

    Conversion Presentation
    Must Preserve
    Original Market-Data
    Semantics And
    Reference Provenance.

---

## 16. Derived Stream Creation Workflow

The TUI provides a configuration-driven workflow for creating derived streams.

The form must reflect the capabilities supported by the runtime configuration compiler.

### 16.1 Creation Workflow

    User Presses [a]
           |
           v
    Select Transformation Type
           |
           v
    Select Source Dependencies
           |
           v
    Configure Transformation
           |
           v
    Configure Ordering / References
           |
           v
    Configure Recording
           |
           v
    Validate Draft
           |
           v
    Inspect Configuration Diff
           |
           v
    Confirm Creation
           |
           v
    Submit Control API Command
           |
           v
    Track Operation

### 16.2 Transformation Selection

Initial choices:

    Merge

    Currency Conversion

The UI should display a short explanation of each transformation.

### 16.3 Merge Creation Fields

Recommended fields:

| Field | Input |
|---|---|
| Derived stream ID | Validated text |
| Merge mode | Trades, Depth, Combined |
| Source streams | Multi-selection |
| Ordering policy | Supported enum |
| Initial reorder window | Duration |
| Maximum reorder window | Duration |
| Late-event policy | Supported enum |
| Source failure policy | Supported enum |
| Enabled | Toggle |
| Recording | Toggle |

Only fields applicable to the selected ordering mode should be displayed.

### 16.4 Source Selection

The source selector should list available source and compatible derived streams.

Useful columns:

    StreamId

    Exchange

    Instrument

    Channel

    Health

    Current Dependencies

The selected sources must be validated against the transformation's required input contract.

### 16.5 Source Ranking

For merges requiring deterministic tie-breaking, the UI must support explicitly ordering selected sources.

Conceptually:

    1. Binance BTCUSDT Trades

    2. Bybit BTCUSDT Trades

    3. OKX BTCUSDT Trades

Moving a source changes its configured rank.

The interface must preserve source rank independently of visual table sorting.

### 16.6 Ordering Configuration

When event-time ordering is selected, expose:

    InitialWindow

    MaximumWindow

    LateEventPolicy

    SourceFailurePolicy

The form must validate:

    InitialWindow <= MaximumWindow

Additional constraints remain defined by the configuration compiler.

### 16.7 Currency Conversion Creation Fields

Recommended fields:

| Field | Input |
|---|---|
| Derived stream ID | Validated text |
| Source stream | Source selection |
| Target currency | Currency selection |
| Reference policy | Supported enum |
| Maximum reference age | Duration |
| Missing-reference policy | Supported enum |
| Path-resolution policy | Supported enum |
| Enabled | Toggle |
| Recording | Toggle |

The form must not require manual entry of contract values or native quantity semantics.

### 16.8 Conversion Path Preview

Before submission, the TUI should request a validated path preview where supported.

For example:

    Source:
        Bybit ETHBTC

    Target:
        USDT

    Resolved Path:
        ETH/BTC × BTC/USDT

    Required References:
        Bybit BTCUSDT Trades

The preview must originate from the authoritative conversion planner.

The TUI must not independently discover or validate conversion paths.

### 16.9 Reference Availability Warning

A valid conversion path may depend on an inactive source stream.

The preview should distinguish:

    Path Valid:
        Yes

    Required Reference Active:
        No

    Activation Action:
        Acquire Reference Dependency

The runtime may activate the required source through the normal dependency-management mechanism.

### 16.10 Missing Path Validation

If a required conversion path cannot be resolved, the form must display the configuration failure.

Example:

    Validation Failed

    Target:
        USDT

    Missing Reference:
        USDC/USDT

    No Valid Conversion Path

The form must not silently assume equivalence between different stablecoins.

### 16.11 Dependency Validation

Before creation, the configuration compiler must validate:

- Referenced stream existence.
- Input schema compatibility.
- Supported transformation type.
- Required source capabilities.
- Dependency graph acyclicity.
- Valid source ranking.
- Ordering constraints.
- Conversion path resolution.
- Resource limits.

### 16.12 Creation Result

After submission, the TUI may display:

    Configuration Accepted

    Derived Stream Registered

    Activation:
        Applying

The user must be able to inspect asynchronous progress.

### 16.13 Creation Invariant

    The TUI Collects
    Transformation Intent.

    The Runtime Compiler
    Validates Semantics.

    The Derived Registry
    Executes Activation.

---

## 17. Derived Stream Editing and Removal

Existing derived streams may be edited or removed through validated configuration changes.

Semantic changes must preserve generation boundaries and already published event meanings.

### 17.1 Editing Workflow

    Select Derived Stream
           |
           v
    Open Edit Dialog
           |
           v
    Load Effective Configuration
           |
           v
    Create Local Draft
           |
           v
    Modify Supported Fields
           |
           v
    Validate Draft
           |
           v
    Inspect Semantic Diff
           |
           v
    Confirm Update
           |
           v
    Submit Revision-Checked Command
           |
           v
    Track Reconciliation

### 17.2 Supported Editing

Depending on transformation capabilities, supported changes may include:

    Source Dependencies

    Source Ranking

    Ordering Policy

    Reorder Window

    Late-Event Policy

    Source Failure Policy

    Conversion Target

    Reference Policy

    Reference Freshness

    Recording Policy

Not every change is hot-editable.

### 17.3 Semantic Changes

Changes affecting derived output semantics require an explicit transformation-generation boundary.

Examples:

    Add Merge Source

    Remove Merge Source

    Change Source Rank

    Change Ordering Policy

    Change Conversion Target

    Change Conversion Path

The TUI should identify such changes as potentially requiring derived-runtime replacement.

### 17.4 Configuration Impact Preview

The edit dialog should display:

    Affected Derived Stream

    Previous Generation

    Required Replacement

    Added Dependencies

    Removed Dependencies

    Recording Impact

    Expected Output Boundary

The preview must come from the Configuration Manager and Runtime Reconciler.

### 17.5 Runtime Replacement

A configuration update may require:

    Prepare Replacement Generation

    Validate Dependencies

    Establish Input Boundary

    Fence Previous Generation

    Activate Replacement

    Release Obsolete Dependencies

The TUI displays operation progress.

It must not execute these steps itself.

### 17.6 Dependency Preservation

Removing one dependency from a derived stream must not terminate that source if other consumers still require it.

The Stream Registry remains authoritative for demand ownership.

### 17.7 Deactivation

Deactivation stops the derived runtime while retaining its persistent definition.

The TUI must distinguish:

    Disable Derived Stream

from:

    Remove Derived Stream

A disabled derived stream may be activated again later.

### 17.8 Removal

Removal deletes the persistent derived definition through the validated configuration system.

The runtime must release its dependency handles according to the derived registry's lifecycle contract.

### 17.9 Removal Confirmation

Example:

    Remove Derived Stream?

    btc_spot_trades_merged

    This stream is referenced by:

        btc_spot_trades_extended

    Removal may invalidate
    downstream dependencies.

    [Confirm] [Cancel]

The system must validate dependency effects before accepting the removal.

### 17.10 Dependent Stream Protection

If another active derived stream requires the selected stream, the default removal behavior should reject the operation.

Explicit cascading removal may be supported through a separately validated command.

The TUI must not silently remove dependent streams.

### 17.11 Recording During Removal

If a derived stream is being recorded, removal must follow the Recording Manager's shutdown and commitment policies.

The UI must not claim complete recording durability until the relevant commit boundary is established.

### 17.12 Revision Conflicts

If another client changes the derived definition while the user is editing:

    ExpectedRevision != CurrentRevision

The TUI must preserve the draft and request revalidation against the current configuration.

### 17.13 Editing Invariant

    Derived Configuration
    Changes Must Preserve
    Explicit Semantic
    Generation Boundaries.

---

## 18. Dependency Inspection and Diagnostics

Derived streams may depend on multiple source and derived streams.

The TUI should provide an understandable view of this dependency structure without implementing an independent dependency registry.

### 18.1 Dependency Inspector

The inspector displays upstream dependencies and downstream consumers.

Example:

    btc_spot_trades_extended
              |
              +-- btc_spot_trades_merged
              |         |
              |         +-- Binance BTCUSDT Trades
              |         |
              |         +-- Bybit BTCUSDT Trades
              |         |
              |         +-- OKX BTCUSDT Trades
              |
              +-- Bitget BTCUSDT Trades

The structure comes from the authoritative derived dependency graph.

### 18.2 Dependency Information

Each dependency may expose:

    StreamId

    StreamType

    RequiredCapability

    LifecycleState

    HealthStatus

    DependencyPolicy

    CurrentParticipation

### 18.3 Shared Source Acquisition

The inspector should make shared acquisition visible.

For example:

    Binance BTCUSDT Trades

    Required By:
        btc_spot_trades_merged
        binance_btc_spot_combined

    Physical Acquisition:
        Shared

This confirms that multiple derived consumers reuse one native source stream.

### 18.4 Dependency Health

Dependency health must preserve the actual required capability.

For example:

    Connection:
        Healthy

    L2 Synchronization:
        Invalid

A derived stream requiring synchronized L2 state must not present the dependency as healthy merely because its transport remains connected.

### 18.5 Root-Cause Inspection

The inspector should allow navigation through health reasons.

Example:

    Derived Stream:
        Degraded

    Reason:
        Source Unavailable

    Source:
        OKX BTCUSDT Depth

    Root Cause:
        Book Synchronization Invalid

The TUI must not independently infer the root cause from metric correlations.

### 18.6 Watermark-Limiting Source

For event-time merges, the dependency inspector may highlight the source currently limiting watermark progress.

Example:

    Watermark Limiting Source:
        OKX BTCUSDT Trades

    Current State:
        Recovering

This information must originate from the Merge Engine.

### 18.7 Conversion Reference Dependencies

Conversion dependencies should be distinguishable from ordinary merge inputs.

Example:

    bybit_ethbtc_to_usdt
              |
              +-- Primary Source
              |       |
              |       +-- Bybit ETHBTC Trades
              |
              +-- Reference Dependency
                      |
                      +-- Bybit BTCUSDT Trades

The interface must preserve the purpose of each dependency.

### 18.8 Dependency Cycles

The authoritative configuration compiler rejects dependency cycles.

If the TUI receives an invalid dependency graph during draft validation, it should display the relevant cycle.

Example:

    Derived A
        -> Derived B
        -> Derived A

The UI must not attempt to execute or repair the graph independently.

### 18.9 Large Dependency Graphs

The inspector must support bounded rendering.

For large graphs, it may use:

- Collapsible tree nodes.
- Search.
- Selected-branch inspection.
- Paginated dependency listings.

It must not attempt to render an unbounded recursive tree.

### 18.10 Dependency Inspector Invariant

    Dependency Visualization
    Reflects The
    Authoritative Registry.

    It Does Not Own
    Dependency State.

---

## 19. Derived-Stream Failure and Recovery Presentation

Derived streams may continue producing output while one or more capabilities are degraded.

The TUI must make these conditions visible without incorrectly labeling every degraded stream as completely unavailable.

### 19.1 Failure Categories

Relevant failure categories include:

    SourceUnavailable

    SourceIntegrityDegraded

    WatermarkStalled

    ReorderBufferPressure

    StrictOrderingViolation

    ConversionReferenceMissing

    ConversionReferenceStale

    ConversionArithmeticFailure

    PublicationUnavailable

    RecordingUnavailable

The categories must map to the authoritative failure and health contracts.

### 19.2 Partial Source Failure

Example:

    BTC Spot Trades Merge

    Binance:
        Healthy

    Bybit:
        Healthy

    OKX:
        Reconnecting

    Output:
        Continuing

    Health:
        Degraded

The UI must identify that output is continuing with incomplete source participation.

### 19.3 Watermark Stall

Example:

    Ordering:
        Adaptive Event Time

    Watermark:
        Stalled

    Limiting Source:
        OKX BTCUSDT Trades

    Buffer:
        72% Utilization

The panel should expose whether the runtime is:

    Waiting

    Recovering

    Applying Source Failure Policy

    Unable To Progress

A stalled watermark does not automatically prove lost events.

### 19.4 Late-Event Handling

The UI should distinguish:

    Successfully Reordered Events

    Events Beyond Finalized Frontier

    Events Emitted As Late

    Events Rejected By Policy

A late-event count must not be presented as a confirmed native exchange-loss count.

### 19.5 Ordering Failure

If a strict ordering guarantee is violated:

    Ordering Health:
        Unhealthy

The UI should display the authoritative failure reason.

It must not attempt to determine ordering correctness by inspecting a small sample of recent output timestamps.

### 19.6 Conversion Reference Failure

Example:

    Bybit ETH/BTC → ETH/USDT

    Source:
        Healthy

    BTC/USDT Reference:
        Stale

    Conversion:
        Unavailable

    Overall:
        Degraded

The original ETHBTC source stream remains independently healthy.

### 19.7 Recovery Progress

Where the derived runtime has an active recovery operation, display:

    RecoveryOperationId

    RecoveryCategory

    CurrentState

    AffectedDependencies

    LatestError

The Recovery Coordinator and derived component owners remain authoritative.

### 19.8 Generation Recovery

A derived runtime may require reinitialization after a dependency failure or semantic configuration change.

The UI should display the new generation only after the authoritative registry reports it.

### 19.9 Diagnostic History

The panel may show a bounded history of recent incidents.

Examples:

    SourceUnavailable

    WatermarkStalled

    SourceRecovered

    DerivedGenerationActivated

These operational events remain separate from canonical market-data events.

### 19.10 Failure Presentation Invariant

    Derived Health
    Must Reflect
    Required Source Participation,
    Ordering Integrity,
    Conversion Availability,
    And Publication State.

---

**## 20. Recording View

The Recording view provides a unified interface for inspecting and managing PostgreSQL market-data recording.

It also presents the Live Retention state relevant to recording recovery and historical bootstrap.

PostgreSQL recording and Live Retention remain independently owned subsystems.

The TUI must preserve the distinction between:

- Canonical event acquisition.
- Recording admission.
- Database transaction commitment.
- Verified recorded coverage.
- Temporary retained coverage.
- Historical archive availability.

### 20.1 Responsibilities

The Recording view must support:

- Inspecting PostgreSQL connectivity.
- Viewing global recording health.
- Listing streams with recording enabled.
- Inspecting per-stream recording progress.
- Monitoring recording throughput.
- Monitoring pending batches.
- Inspecting database commit latency.
- Viewing committed recording frontiers.
- Identifying unresolved recording gaps.
- Enabling and disabling recording.
- Requesting supported recording flush operations.
- Inspecting recording recovery.
- Viewing Live Retention resource usage.
- Inspecting protected retention coverage.

The view must not directly execute SQL or modify database schemas.

### 20.2 Recording View Layout

Recommended wide-terminal layout:

    +--------------------------------------------------------------------------+
    | RECORDING                                                                |
    +--------------------------------------------------------------------------+
    | PostgreSQL: Connected    Recording: Active    Health: Degraded           |
    +--------------------------------------------------------------------------+
    | DATABASE                       | RETENTION                                |
    |                                |                                          |
    | Recording Streams: 24          | RAM Usage: 640 MiB                       |
    | Events Committed: 45,000/s     | Parquet Usage: 12.4 GiB                 |
    | Pending Events: 120,000        | Pending Spill: 32 MiB                    |
    | Oldest Pending: 2.7 s          | Protected Sessions: 2                    |
    | Commit p99: 84 ms              | Health: Healthy                          |
    +--------------------------------+------------------------------------------+
    | STREAM                    STATE      LAG       GAPS    HEALTH             |
    |--------------------------------------------------------------------------|
    | BTCUSDT Binance Trades    Active     0.2 s      0      Healthy            |
    | BTCUSDT Binance L2        Active     1.1 s      0      Healthy            |
    | BTCUSDT OKX Trades        Recovering 5.4 s      2      Degraded           |
    | BTCUSDT Bybit L2          Active     0.3 s      0      Healthy            |
    +--------------------------------------------------------------------------+
    | [Enter] Inspect  [e] Enable  [d] Disable  [f] Flush  [Tab] Focus         |
    +--------------------------------------------------------------------------+

All values are illustrative.

### 20.3 Recording Data Sources

| Display Element | Authoritative Source |
|---|---|
| Database connection state | Recording Manager |
| Recording lifecycle | Recording Manager |
| Committed event counts | Recording Manager |
| Commit frontiers | Recording Manager |
| Recording gaps | Recording Manager |
| Recording throughput | Metrics Registry |
| Commit latency | Metrics Registry |
| Recording health | HealthEvaluator |
| Retention coverage | Retention Manager |
| Retention resource usage | Retention Manager |
| Recovery progress | Recovery Coordinator / Recording Manager |

The TUI must not reconstruct recording coverage from event timestamps or database row counts.

### 20.4 Database Connection Status

The view displays:

    Connected

    Connecting

    Recovering

    Unavailable

    Disabled

The exact lifecycle values must come from the Recording Manager's established contract.

A connected PostgreSQL database does not necessarily imply healthy recording.

### 20.5 Global Recording Summary

Recommended measurements:

    ActiveRecordingStreams

    EventsAdmittedPerSecond

    EventsCommittedPerSecond

    PendingEvents

    PendingBytes

    PendingBatches

    CommitLatency

    RecordingRecoveryOperations

    KnownRecordingGaps

Admission and commitment throughput must remain separate.

### 20.6 Recording Backlog

The view should display:

    PendingEvents

    PendingBytes

    OldestPendingAge

    RecordingQueueOccupancy

A growing backlog may indicate that database commitment cannot keep pace with acquisition.

However, a temporary backlog is not automatically a confirmed recording gap.

### 20.7 Commit Latency

Commit latency must identify its measurement boundary.

Recommended values:

    BatchCommitDuration

    AdmissionToCommitDuration

These are different measurements.

The TUI must not display them under one ambiguous label.

### 20.8 Recording Stream Table

The per-stream table should include:

| Column | Purpose |
|---|---|
| Stream | Authoritative source or derived identity |
| Recording State | Current recording lifecycle |
| Admission Rate | Events entering the recorder |
| Commit Rate | Successfully committed events |
| Backlog | Pending events or bytes |
| Lag | Defined recording lag measurement |
| Gaps | Known unresolved recording gaps |
| Health | Recording health assessment |

The table must support sorting and filtering.

### 20.9 Recording Frontier

The detail panel should expose:

    LastAdmittedCursor

    HighestCommittedCursor

    LastContiguousCommittedCursor

    CurrentRecordingEpoch

These positions must retain their respective meanings.

The highest committed cursor does not prove continuous persistence when an earlier batch remains uncommitted.

### 20.10 Recording Coverage

A per-stream coverage inspector may display:

    VerifiedCommittedIntervals

    UnresolvedRecordingGaps

    RecoverableIntervals

    UnrecoverableIntervals

Coverage information comes from the Recording Manager.

The TUI must not infer completeness because stored timestamps appear continuous.

### 20.11 Recording Recovery

During database failure or recovery, the view may show:

    RecoveryState

    RecoveryOperationId

    PendingRetainedEvents

    RecoveryCursor

    LastCommittedCursor

    UnresolvedGaps

Recovery progress must be based on authoritative cursor and commitment state.

### 20.12 Recording Enabled vs Recording Healthy

The view must distinguish desired recording configuration from operational health.

Example:

    Recording Enabled:
        Yes

    Recording State:
        Recovering

    Recording Health:
        Degraded

This is a valid combination.

### 20.13 Recording View Invariant

    Recording Status
    Must Preserve The
    Difference Between
    Requested Persistence,
    Accepted Events,
    Committed Events,
    And Verified Coverage.

---

## 21. Recording Management Interactions

The Recording view provides supported recording controls through the Runtime Control API.

The Recording Manager remains responsible for execution.

### 21.1 Supported Actions

Initially:

    Enable Recording

    Disable Recording

    Request Flush

    Inspect Stream Recording

    Inspect Recording Coverage

    Inspect Recovery

These operations must be available only when authorized.

### 21.2 Enable Recording Workflow

    Select Stream
          |
          v
    Open Recording Dialog
          |
          v
    Load Effective Policy
          |
          v
    Configure Supported Settings
          |
          v
    Validate Draft
          |
          v
    Inspect Configuration Impact
          |
          v
    Submit Revision-Checked Update
          |
          v
    Track Recording Activation

The interface must not activate recording before configuration acceptance.

### 21.3 Recording Configuration Form

Recommended fields:

| Field | Input |
|---|---|
| Stream | Selected registered stream |
| Enabled | Toggle |
| Batch size | Positive integer or inherited value |
| Flush interval | Duration or inherited value |
| Required durability | Supported policy |
| Recording policy origin | Read-only effective-value information |

Only supported settings should be editable.

The form must distinguish explicit overrides from inherited defaults.

### 21.4 Recording Activation Boundary

Before enabling recording, the UI should explain:

    Recording Begins
    At An Explicit
    Canonical Cursor Boundary.

Previously accepted events are not automatically included.

If historical backfill is supported through a separate operation, it must not be implied by enabling ordinary live recording.

### 21.5 Disable Recording Workflow

Disabling recording must show the effect on:

    New Recording Admission

    Pending Batches

    Current Commit Frontier

    Recording Demand Ownership

The Recording Manager defines how admitted events are finalized or explicitly left unresolved.

### 21.6 Disable Confirmation

An illustrative confirmation dialog:

    Disable Recording?

    Stream:
        Binance BTCUSDT Trades

    New recording admission
    will stop at the validated
    deactivation boundary.

    Existing pending batches
    remain subject to the
    Recording Manager policy.

    [Confirm] [Cancel]

The dialog must not promise that every pending event has committed.

### 21.7 Recording Flush

The user may request a flush for:

    One Recording Stream

or:

    All Eligible Recording Streams

if supported by the Recording Manager.

The request returns an asynchronous operation identity when necessary.

### 21.8 Flush Progress

Display:

    Flush Requested

    Batches Sealed

    Transactions Pending

    Commits Completed

    Errors

A successful flush request is not equivalent to confirmed durable commitment.

### 21.9 Recording Recovery Controls

Optional manual recovery controls may be exposed when supported.

They must route through the Recording Manager and Recovery Coordinator.

The TUI must not bypass:

- Commit-state reconciliation.
- Idempotent retries.
- Cursor validation.
- Retention coverage checks.
- Recovery generation fencing.

### 21.10 Recording Operation Feedback

Every operation must display:

    OperationId

    AffectedStreams

    CurrentState

    CompletionCondition

    LatestError

The UI must remain responsive while recording operations execute.

### 21.11 Recording Management Invariant

    Recording Controls
    Modify Recording Intent.

    The Recording Manager
    Establishes Actual
    Persistence Guarantees.

---

## 22. Live Retention Inspection

Live Retention preserves accepted canonical events in bounded RAM and Parquet-backed storage.

Its primary purposes include bootstrap coverage, replay, and recovery support.

The Recording view should provide a compact Retention inspector without implementing retention management itself.

### 22.1 Retention Overview

The Retention inspector displays:

    Retention State

    RAM Usage

    Parquet Disk Usage

    Pending Spill Bytes

    Active Protection Sessions

    Available Cursor Coverage

    Retention Health

### 22.2 Memory Usage

Recommended measurements:

    RetainedMemoryBytes

    ConfiguredMemoryBudget

    MemoryUtilizationRatio

    ProtectedMemoryBytes

The UI must use the Retention Manager's accounting rather than estimate allocations from displayed event counts.

### 22.3 Parquet Usage

Display:

    CommittedParquetSegments

    CommittedParquetBytes

    PendingSpillBytes

    SpillFailures

    LastSuccessfulSpill

A pending spill must not be reported as an already committed segment.

### 22.4 Coverage Inspection

A selected stream may expose:

    EarliestAvailableCursor

    LatestAvailableCursor

    AvailableCursorIntervals

    MissingCursorIntervals

    ProtectedCursorIntervals

The detailed inspector must use bounded pagination or interval queries.

### 22.5 Protection Sessions

The inspector may list active retention protection sessions.

Recommended fields:

    ReservationId

    OwningSession

    ProtectedStream

    ProtectedInterval

    ProtectionAge

    ResourceUsage

    ExpirationPolicy

These values originate from the Retention Manager.

### 22.6 Protected Data

Protected retention data must not be presented as freely reclaimable.

Example:

    Retention Memory:
        640 MiB

    Protected:
        512 MiB

    Reclaimable:
        128 MiB

These values are illustrative.

The authoritative resource accounting must determine reclaimability.

### 22.7 Replay Availability

The inspector may show whether replay is available for a requested cursor interval.

It must distinguish:

    Available

    PartiallyAvailable

    Unavailable

    Unverified

The exact availability vocabulary must follow the Retention Manager's existing coverage contract.

### 22.8 Historical Completeness

Retention coverage does not independently establish complete native exchange history.

For example:

    Retained MarketForge Cursor Range:
        Continuous

does not imply:

    Exchange Trade History:
        Complete

The TUI must preserve this distinction.

### 22.9 Disk Pressure

Display:

    ConfiguredDiskBudget

    CurrentDiskUsage

    AvailableDiskSpace

    DiskPressureState

Unavailable filesystem measurements must not appear as zero.

### 22.10 Retention Failures

Relevant diagnostic conditions:

    SpillFailed

    CapacityExceeded

    ProtectionUnavailable

    SegmentInvalid

    ReplayCoverageUnavailable

The UI should expose affected streams and reservation identities where relevant.

### 22.11 Retention vs Recording

The two subsystems have independent health dimensions.

Example:

    Retention:
        Healthy

    PostgreSQL Recording:
        Degraded

Another example:

    Retention:
        CapacityExceeded

    PostgreSQL Recording:
        Healthy

Neither status automatically replaces the other.

### 22.12 Retention Controls

The initial TUI should prioritize read-only inspection.

If retention protection or release operations are exposed, they must require the appropriate authorization and follow Document 13's reservation ownership rules.

### 22.13 Retention Invariant

    The Retention Inspector
    Reports Existing
    Cursor Coverage
    And Resource Guarantees.

    It Does Not Perform
    Historical Bootstrap.

---

## 23. Settings View

The Settings view provides access to supported MarketForge Live configuration.

It uses the Configuration Manager and Runtime Control API established in Documents 03 and 13.

The interface must distinguish editable settings, inherited values, restart-required settings, and immutable protocol definitions.

### 23.1 Responsibilities

The Settings view must support:

- Viewing the current desired configuration.
- Viewing effective runtime configuration.
- Inspecting configuration revision.
- Editing supported settings.
- Resetting overrides to inherited defaults.
- Validating configuration drafts.
- Previewing affected resources.
- Applying revision-checked changes.
- Tracking reconciliation.
- Inspecting restart requirements.
- Viewing configuration errors.
- Inspecting optional Prometheus exporter configuration.

### 23.2 Settings Layout

Recommended layout:

    +--------------------------------------------------------------------------+
    | SETTINGS                                                                 |
    +--------------------------------------------------------------------------+
    | Desired Revision: 42       Applied Revision: 42       Converged           |
    +----------------------------+---------------------------------------------+
    | CATEGORIES                 | CONFIGURATION                               |
    |                            |                                             |
    | > Runtime                  | Shutdown Timeout       15 s                |
    |   Logging                  |                                             |
    |   Metrics                  | Metrics Enabled        Yes                 |
    |   Prometheus               | Detailed Timing        No                  |
    |   Publication              | Snapshot Interval      1000 ms             |
    |   Recording                |                                             |
    |   Retention                | Recording Batch Size   10000               |
    |   Recovery                 | Recording Flush        100 ms              |
    |                            |                                             |
    |                            | Maximum Subscribers    64                  |
    +----------------------------+---------------------------------------------+
    | [Enter] Edit  [v] Validate  [a] Apply  [r] Reset  [Esc] Back              |
    +--------------------------------------------------------------------------+

All values are illustrative.

### 23.3 Settings Categories

Recommended categories:

    Runtime

    Logging

    Metrics

    Prometheus

    Publication

    Recording

    Retention

    Recovery

Additional categories may be introduced when supported by the Configuration System.

### 23.4 Configuration Sources

The Settings view consumes:

    DesiredConfiguration

    EffectiveConfiguration

    ConfigurationRevision

    ConfigurationOriginMetadata

    ReconfigurationPolicy

The TUI must not reconstruct inheritance rules independently.

### 23.5 Desired vs Effective Values

A setting may have:

    DesiredValue

    EffectiveValue

    AppliedValue

For example:

    Desired:
        2000 ms

    Applied:
        1000 ms

    Reconciliation:
        Pending

The UI must not present the requested value as already active before its application succeeds.

### 23.6 Configuration Origin

Where supported, display whether a value originates from:

    Global Default

    Exchange Constraint

    Stream Override

    Explicit Global Setting

The effective-value origin must come from the Configuration Compiler.

### 23.7 Hot-Editable Settings

Initially supported settings remain:

| Setting | Application boundary |
|---|---|
| Logging level | Immediate |
| Metrics enabled | Subsequent collection |
| Detailed timing | Subsequent measurements |
| Metrics snapshot interval | Next collection cycle |
| Recording batch size | Next batch |
| Recording flush interval | Next batch |
| Maximum subscribers | Future admission |

The UI must not add unsupported hot-editing behavior.

### 23.8 Stream-Level Overrides

Applicable per-stream settings may be edited through contextual stream configuration forms.

Examples:

    Recording Enabled

    Recovery Policy Override

    Detailed Timing Override

These changes must preserve source identity unless the field explicitly changes acquisition semantics.

### 23.9 Restart-Required Settings

A restart-required setting should be clearly identified.

Examples:

    WebSocket Endpoint

    Control Socket Path

    Prometheus Listen Address

The UI must not promise immediate application.

### 23.10 Immutable Settings

Immutable settings must be displayed read-only.

Examples include incompatible canonical schema changes or unsupported protocol strategy replacements.

### 23.11 Settings View Invariant

    Settings Are Edited
    Through Validated
    Desired-State Revisions.

    The TUI Must Not
    Modify Runtime
    Configuration Directly.

---

## 24. Configuration Forms and Validation

Configuration forms collect user intent and submit it to the authoritative Configuration Manager.

Client-side validation improves usability but must not replace server-side validation.

### 24.1 Form Responsibilities

A configuration form must:

- Display effective values.
- Display supported editing policies.
- Preserve inheritance semantics.
- Validate basic input structure.
- Support cancel without side effects.
- Submit drafts for server validation.
- Display semantic differences.
- Preserve drafts after recoverable errors.
- Detect revision conflicts.
- Avoid changing runtime state before confirmation.

### 24.2 Form State

Conceptual representation:

    ConfigurationFormState {
        base_revision

        target_resource

        original_values
        draft_values

        field_errors
        validation_state

        semantic_diff

        focused_field
        dirty_fields
    }

The form is local presentation state.

### 24.3 Supported Input Types

Initial form primitives may include:

    Toggle

    IntegerInput

    DecimalInput

    DurationInput

    EnumSelection

    TextInput

    MultiSelection

    ReadOnlyValue

Inputs should reuse existing Ratatui components where compatible.

### 24.4 Typed Values

The TUI must preserve typed configuration semantics.

For example:

    1000 milliseconds

must remain a validated duration rather than an arbitrary string passed directly into runtime state.

### 24.5 Numerical Validation

Numeric fields must validate:

- Parsing.
- Supported range.
- Units.
- Applicable precision.
- Resource limits.
- Field-specific constraints.

The TUI must not silently clamp invalid values unless that behavior is explicitly part of the field contract.

### 24.6 Optional Values

Optional configuration fields must distinguish:

    Inherit

    Explicit(value)

This distinction is critical for per-stream overrides.

### 24.7 Reset to Default

Resetting an override means:

    Explicit(value)
          |
          v
        Inherit

It does not mean copying the current global value into an explicit override.

### 24.8 Draft Editing

Editing a form must not immediately mutate desired configuration.

Conceptually:

    Current Configuration
             |
             v
         Local Draft
             |
             v
       User Modifications
             |
             v
       Server Validation
             |
             v
       User Confirmation
             |
             v
      Configuration Apply

### 24.9 Client-Side Validation

The TUI may validate simple constraints locally.

Examples:

    Required Field Present

    Valid Integer Syntax

    Nonnegative Duration

    Supported Selection

However, authoritative validation belongs to the Configuration Manager.

### 24.10 Server-Side Validation

The server validates:

    Configuration Schema

    Exchange Capabilities

    Instrument Metadata

    Dependency Graph

    Resource Budgets

    Reconfiguration Policies

    Revision Compatibility

The TUI must display the resulting validation errors.

### 24.11 Validation Errors

Errors should identify:

    ConfigurationPath

    ErrorCategory

    ErrorMessage

    Suggested Correction

where available.

The form should preserve user input after validation failure.

### 24.12 Semantic Diff

Before applying a valid draft, the TUI should display the effective changes.

Example:

    Recording Batch Size:
        10000 -> 20000

    Application:
        Next Batch Boundary

    Affected Resources:
        Recording Manager

    Source Connections Restarted:
        None

The diff originates from the Configuration Manager.

### 24.13 Impact Preview

The preview may identify:

    Affected Source Streams

    Affected Derived Streams

    Required Runtime Operations

    Generation Transitions

    Resource Budget Changes

    Restart Requirements

The TUI must not independently calculate authoritative impact.

### 24.14 Confirmation

Potentially disruptive changes require confirmation.

Examples:

    Remove Source Stream

    Replace Derived Generation

    Disable Required Recording

    Restart Connection

Ordinary low-risk settings may use a simpler confirmation workflow.

### 24.15 Configuration Submission

A submitted update includes the revision against which the draft was prepared.

Conceptually:

    ExpectedRevision

    ProposedChanges

    RequestedOperation

The server must reject conflicting revisions.

### 24.16 Form Cancellation

Cancelling a draft must:

- Discard unsubmitted changes when requested.
- Preserve current daemon configuration.
- Restore the previous UI focus.
- Avoid submitting control commands.

### 24.17 Form Invariant

    Editing Is Local.

    Validation Is Authoritative.

    Application Is Revisioned.

---

## 25. Configuration Revisions and Runtime Convergence

The Settings view must make configuration history and runtime reconciliation understandable.

Configuration acceptance does not imply that every affected component has already adopted the new settings.

### 25.1 Revision Summary

Display:

    CurrentDesiredRevision

    CurrentAppliedRevision

    ReconciliationState

    PendingOperations

    FailedOperations

The applied revision may be reported per component when the runtime is partially converged.

### 25.2 Revision History

The TUI may display a bounded list of accepted configuration revisions.

Recommended columns:

    RevisionId

    Timestamp

    ChangeOrigin

    AffectedResources

    ApplicationStatus

The history must originate from the Configuration Manager.

### 25.3 Revision Details

A selected revision may expose:

    ParentRevision

    ConfigurationHash

    ChangedFields

    PreviousValues

    NewValues

    AffectedResources

    RuntimeApplicationOutcome

Sensitive fields must be redacted.

### 25.4 Revision Conflict

Example:

    Configuration Conflict

    Your Draft Revision:
        42

    Current Server Revision:
        43

    Another client changed
    the configuration.

    [Reload] [Review Draft] [Cancel]

The TUI must not silently overwrite revision 43.

### 25.5 Conflict Resolution

The recommended workflow:

    Detect Conflict
          |
          v
    Fetch Latest Revision
          |
          v
    Compare Draft Against Current State
          |
          v
    Reapply Intended Changes
          |
          v
    Validate New Draft
          |
          v
    Submit New Expected Revision

The UI should preserve the original draft until the user discards or successfully reapplies it.

### 25.6 Partial Convergence

Example:

    Revision 44:
        Accepted

    Binance BTC Trades:
        Applied

    Bybit BTC Trades:
        Applied

    OKX BTC Trades:
        Pending Recovery

    Overall:
        Degraded

The UI must not report complete convergence until all required transitions satisfy their declared completion conditions.

### 25.7 Reconciliation Progress

A reconciliation inspector may display:

    TargetRevision

    OperationId

    AffectedComponent

    CurrentState

    CompletionCondition

    LatestError

The progress model must come from the Runtime Reconciler.

### 25.8 Restart-Required Changes

A valid configuration change classified as `ServiceRestart` may be accepted
and durably persisted as a new desired configuration revision without
immediately changing the running service.

The Control API must distinguish:

- **Desired configuration:** The accepted and persisted configuration revision.
- **Applied configuration:** The configuration whose required runtime effects
  have successfully completed.
- **Actual runtime state:** The resources, connections, and settings currently
  active in the running daemon.

For restart-required changes, the desired revision may be newer than the
applied revision.

The UI should display:

    Configuration Accepted

    Restart Required

    Desired Revision:
        45

    Applied Revision:
        44

    Current Runtime Uses:
        Previous Effective Value

The revision numbers above are illustrative.

The TUI must not claim that newly configured listeners, exchange endpoints,
or other restart-required settings are active until the required restart
and subsequent runtime reconciliation have succeeded.

Successful persistence does not imply successful runtime application.

The Ratatui interface obtains desired configuration, applied revision
information, and actual runtime state through the authoritative Control API
defined in Document 13. It must not infer successful application merely
because a configuration update was accepted.

### 25.9 Rollback

A rollback request creates a new configuration revision based on a previous valid configuration.

The UI must clearly communicate that rollback:

- Does not reverse published market data.
- Does not delete database commits.
- Does not undo completed recording.
- Does not erase historical coverage gaps.
- May require runtime component transitions.

### 25.10 Rollback Confirmation

Example:

    Restore Configuration?

    Source Revision:
        40

    Current Revision:
        44

    A new revision will be
    created using revision 40's
    desired configuration.

    Existing market-data history
    will not be modified.

    [Confirm] [Cancel]

### 25.11 Revision Invariant

    Configuration History
    Describes Desired-State
    Changes And Their
    Application Outcomes.

    It Is Not A Log
    Of Canonical Events.

---

## 26. Optional Prometheus Settings

The Settings view may expose the optional Prometheus exporter configuration established in Documents 03 and 12.

Prometheus and Grafana remain external applications.

### 26.1 Prometheus Settings

Recommended fields:

    Enabled

    ListenAddress

    ExpositionPath

    MaximumConcurrentRequests

    MaximumResponseBytes

    CacheInterval

Only supported fields should be editable.

### 26.2 Default State

The initial default is:

    Enabled:
        False

    ListenAddress:
        127.0.0.1:9091

    Path:
        /metrics

The exporter must not create an HTTP listener when disabled.

### 26.3 Exporter Status

The UI may display:

    Disabled

    Starting

    Listening

    Failed

The lifecycle status originates from the exporter supervisor.

### 26.4 Restart Requirements

Initially, changes to exporter activation and listener configuration require a service restart.

The UI must distinguish:

    Desired Exporter Configuration

from:

    Currently Applied Exporter Configuration

### 26.5 Security Warning

When an operator configures a non-loopback listener, the UI should display the corresponding security implications.

Example:

    Prometheus Endpoint
    Will Be Accessible
    Outside Loopback.

    Verify Network Access
    Restrictions Before
    Deployment.

The server remains responsible for final configuration validation.

### 26.6 External Prometheus and Grafana

The TUI may display the configured exposition endpoint.

It must not attempt to:

- Launch Prometheus.
- Launch Grafana.
- Manage external dashboard credentials.
- Install monitoring infrastructure.
- Treat external monitoring availability as a requirement for Live operation.

### 26.7 Prometheus Invariant

    External Monitoring
    Is Optional.

    Native Metrics,
    Health Evaluation,
    And Ratatui
    Remain Independent.

---

## 27. Operational Notifications and Diagnostics

The TUI displays significant operational events through a bounded notification system.

Notifications help operators understand failures, configuration changes, and recovery progress.

They must not replace authoritative runtime snapshots.

### 27.1 Notification Categories

Relevant categories include:

    Information

    Warning

    Error

    OperationProgress

    OperationCompleted

    ConfigurationChanged

    ConnectionChanged

    HealthChanged

### 27.2 Notification Sources

Notifications originate from:

    Control API Responses

    Control API Notifications

    Local TUI Validation

    TUI Connection State

The TUI must distinguish server-originated operational events from local presentation events.

### 27.3 Notification Model

Conceptually:

    UiNotification {
        notification_id

        category

        title
        message

        affected_resource

        created_at
        expires_at

        action
    }

This is presentation state.

It does not replace the server's authoritative operational-event contract.

### 27.4 Notification Presentation

Short-lived notifications may appear in:

    Status Bar

    Notification Panel

    Modal Dialog

The presentation depends on severity and required operator action.

### 27.5 Informational Notifications

Examples:

    BTCUSDT Trades activated.

    Configuration revision 45 accepted.

    PostgreSQL recording resumed.

Informational notifications should not interrupt active form editing unnecessarily.

### 27.6 Warnings

Examples:

    OKX connection recovering.

    BTCUSDT L2 synchronization unavailable.

    Recording backlog approaching limit.

Warnings should identify the relevant resource and current operational consequence.

### 27.7 Errors

Examples:

    Configuration validation failed.

    Stream removal rejected due to dependencies.

    Recording flush failed.

    Control connection unavailable.

Errors should expose a structured reason and an appropriate follow-up action.

### 27.8 Notification Deduplication

Repeated notifications describing the same unresolved condition should be consolidated where possible.

For example:

    Connection Reconnecting

must not produce an unlimited number of identical visible notifications during repeated retry attempts.

The UI may update the existing notification with the latest attempt count.

### 27.9 Notification History

The TUI maintains bounded local notification history.

This history is not guaranteed to contain every operational event emitted by the daemon.

### 27.10 Notification Gaps

If notification delivery is interrupted:

- Preserve current UI responsiveness.
- Mark relevant cached state stale.
- Reconnect or resubscribe when appropriate.
- Refresh authoritative snapshots.

The UI must not reconstruct current runtime state solely from possibly incomplete notification history.

### 27.11 Operation Notifications

For an asynchronous operation, the TUI may show:

    Operation Accepted

    Operation Applying

    Operation Completed

    Operation Failed

The completion notification must reflect the authoritative operation status.

### 27.12 Notification Focus

Informational notifications should not steal keyboard focus.

Destructive confirmation dialogs and critical errors may require explicit focus.

### 27.13 Diagnostics Inspector

A contextual diagnostics inspector may display:

    Timestamp

    Component

    Severity

    Event Category

    Resource Identity

    Structured Reason

    Recovery Reference

The inspector must not require reading arbitrary local log files directly from the TUI process.

### 27.14 Local Logs

Operational logs remain local to the Live runtime.

They are not stored in PostgreSQL.

The TUI may inspect recent structured diagnostic summaries exposed through the Control API.

It must not assume that its local notification history is a complete persistent operational log.

### 27.15 Notification Invariant

    Notifications Provide
    Operational Awareness.

    Authoritative Runtime
    Snapshots Determine
    Current State.

---
## 28. Keyboard Interaction and Focus Management

MarketForge Live uses a keyboard-first terminal interface.

Keyboard interaction must remain predictable across all five primary views, dialogs, tables, and configuration forms.

The application should reuse existing MarketForge keyboard handling and Ratatui/Crossterm conventions wherever compatible.

### 28.1 Interaction Principles

Keyboard handling must:

- Preserve consistent shortcuts across views.
- Respect the currently focused widget.
- Avoid accidental destructive operations.
- Support efficient table navigation.
- Support modal dialogs.
- Support searchable selections.
- Preserve user input during asynchronous refresh.
- Avoid blocking on control requests.
- Remain functional during daemon disconnection.

### 28.2 Global Keyboard Shortcuts

Recommended initial bindings:

| Key | Action |
|---|---|
| `1` | Dashboard |
| `2` | Streams |
| `3` | Derived Streams |
| `4` | Recording |
| `5` | Settings |
| `Tab` | Focus next region |
| `Shift+Tab` | Focus previous region |
| `Esc` | Close dialog or return |
| `?` | Contextual help |
| `q` | Quit TUI |
| `Ctrl+C` | Exit TUI safely |

Global shortcuts must not intercept text entered into an active form field.

For example, typing `1` inside a numeric input must not navigate to Dashboard.

### 28.3 Table Navigation

Recommended bindings:

| Key | Action |
|---|---|
| `j` / Down | Select next row |
| `k` / Up | Select previous row |
| `PageDown` | Move down one page |
| `PageUp` | Move up one page |
| `Home` | First row |
| `End` | Last row |
| `Enter` | Inspect selected resource |
| `/` | Open search |
| `s` | Change sorting |
| `f` | Open filters |

These bindings apply when a table owns keyboard focus.

### 28.4 Contextual Actions

Contextual shortcuts depend on the active view.

For example:

**Streams**

    a = Add
    e = Activate
    d = Deactivate
    r = Remove

**Derived Streams**

    a = Create
    e = Activate
    d = Deactivate
    r = Remove

**Recording**

    e = Enable
    d = Disable
    f = Flush

**Settings**

    Enter = Edit
    v = Validate
    a = Apply
    r = Reset Override

These actions must be displayed in the contextual shortcut bar.

### 28.5 Focus Model

Conceptual representation:

    FocusState {
        active_view

        focused_region
        focused_widget

        modal_focus

        previous_focus
    }

Only one widget may own keyboard focus at a time.

### 28.6 Input Precedence

Input should be dispatched in this order:

    Terminal Event
          |
          v
    Active Modal
          |
          +-- Consumed
          |
          v
    Focused Input Widget
          |
          +-- Consumed
          |
          v
    Focused View Region
          |
          +-- Consumed
          |
          v
    Global Navigation

The implementation must prevent one keypress from triggering multiple conflicting actions.

### 28.7 Text Editing

Text and numeric inputs must support:

- Character insertion.
- Cursor movement.
- Backspace and deletion.
- Selection where supported.
- Input cancellation.
- Validation feedback.
- Preservation of draft values.

The application should reuse existing text-input widgets.

### 28.8 Search Mode

Entering search mode transfers focus to the search field.

While search is active:

    Character Input
        -> Search Query

not:

    Global Shortcut

Closing search restores table focus.

### 28.9 Help Overlay

The `?` shortcut opens a contextual help overlay.

It should display:

- Global shortcuts.
- Current-view shortcuts.
- Navigation instructions.
- Relevant configuration hints.

The help overlay must not issue runtime commands.

### 28.10 Keyboard Interaction Invariant

    Keyboard Input
    Must Be Handled By
    One Explicit
    Focus Owner.

---

## 29. Dialog and Modal System

Dialogs provide structured interactions for configuration editing, confirmations, errors, and asynchronous operations.

The initial implementation should reuse existing MarketForge dialog components.

### 29.1 Dialog Categories

Recommended categories:

    ConfirmationDialog

    ConfigurationForm

    ResourceSelector

    ValidationResults

    ConfigurationDiff

    OperationProgress

    ErrorDetails

    ContextualHelp

### 29.2 Dialog State

Conceptual representation:

    DialogState {
        dialog_kind

        target_resource

        initial_values
        draft_values

        focused_field

        validation_errors

        associated_operation
    }

Not every dialog requires every field.

The concrete Rust implementation should use appropriate enums and typed state.

### 29.3 Modal Ownership

Only one modal interaction should own keyboard focus at a time.

Nested dialogs should be avoided unless their behavior is explicitly defined.

For example, a configuration form may transition to a validation-results view rather than opening several stacked modals.

### 29.4 Confirmation Dialogs

Destructive or disruptive actions require confirmation.

Examples:

    Remove Source Stream

    Remove Derived Stream

    Restart Connection

    Disable Required Recording

    Apply Restart-Required Configuration

The dialog must identify the exact target resource.

### 29.5 Confirmation Safety

A confirmation must be bound to:

    TargetResourceId

    ResourceGeneration

    ConfigurationRevision

where applicable.

If the target changes before submission, the operation must be revalidated.

The TUI must not apply a confirmation to whichever resource happens to occupy the previously selected table row.

### 29.6 Configuration Forms

Forms maintain local drafts until submission.

They must distinguish:

    OriginalValue

    DraftValue

    EffectiveValue

    AppliedValue

The Configuration Manager remains authoritative for validation and application.

### 29.7 Validation Results

Validation errors should be displayed near affected fields where practical.

A detailed error panel may expose:

    ConfigurationPath

    ErrorCategory

    ErrorMessage

    ApplicableConstraint

Invalid form submission must not discard the user's draft.

### 29.8 Configuration Diff Dialog

Before applying meaningful changes, the TUI should display the semantic diff returned by the Control API.

Example:

    Configuration Revision:
        42 -> 43

    Recording Batch Size:
        10000 -> 20000

    Application Policy:
        Next Batch Boundary

    Affected Components:
        Recording Manager

    WebSocket Restarts:
        None

The TUI must not independently calculate authoritative configuration impact.

### 29.9 Operation Progress Dialog

Long-running operations should expose:

    OperationId

    TargetResource

    OperationKind

    CurrentState

    ProgressDescription

    LatestError

    CompletionCondition

The progress dialog must not block the UI event loop.

### 29.10 Progress Without Percentages

Not every operation has a meaningful numerical progress percentage.

For example:

    Waiting For Exchange Subscription

    Waiting For L2 Snapshot

    Waiting For PostgreSQL Recovery

The UI should display these states directly rather than fabricate a percentage.

### 29.11 Dialog Cancellation

Cancelling a dialog may mean:

    Close Local UI

    Discard Draft

    Cancel Pending Query

    Request Operation Cancellation

These actions are not interchangeable.

Closing an operation-progress dialog must not automatically cancel an accepted persistent runtime operation.

### 29.12 Error Dialogs

Error dialogs should contain:

    Error Category

    Human-Readable Reason

    Affected Resource

    Retryability

    Available Action

Sensitive information must remain redacted.

### 29.13 Modal Invariant

    Dialogs Collect
    And Present Intent.

    They Do Not
    Directly Execute
    Runtime Logic.

---

## 30. Asynchronous Control Client Integration

The Ratatui application communicates with MarketForge Live through the
versioned Control API defined in Document 13.

The Ratatui control client uses the configured Unix-domain control socket.
Requests and responses are serialized as JSON and use the length-prefixed
framing defined in Document 13, Section 20.

This control socket is separate from the binary Unix-domain socket used
for canonical market-data distribution in Document 08. Ratatui must not
interpret market-data distribution frames as Control API messages.

Document 13 is authoritative for control protocol versioning, message
framing, request correlation, response envelopes, and error semantics.

Control communication must remain independent of terminal rendering.

### 30.1 Control Client Responsibilities

The client must:

- Connect to the configured Unix control socket.
- Negotiate protocol compatibility.
- Serialize typed requests.
- Apply length-prefixed framing.
- Correlate responses.
- Track asynchronous operations.
- Receive supported notifications.
- Detect connection failures.
- Reconnect safely.
- Refresh authoritative snapshots.
- Enforce client-side resource limits.

### 30.2 Client Architecture

Conceptually:

    Ratatui Application
             |
             v
       UI Command Queue
             |
             v
       Control Client Task
             |
             v
       Unix Control Socket
             |
             v
       MarketForge Daemon

Responses follow:

    Control Socket
          |
          v
    Control Client Task
          |
          v
    Bounded UI Event Queue
          |
          v
    Application State
          |
          v
    Ratatui Renderer

The UI must not perform blocking socket reads.

### 30.3 Client Task

The implementation may use a dedicated Tokio task for control communication.

Conceptual representation:

    LiveControlClient {
        connection_state

        protocol_version

        next_request_id

        pending_requests

        tracked_operations

        outgoing_queue

        incoming_notifications
    }

The implementation should reuse existing asynchronous infrastructure where practical.

### 30.4 Request Correlation

Each outgoing request receives a RequestId.

Responses are correlated using:

    ClientSessionId

    RequestId

Responses may arrive out of order.

The UI must not associate a response with the most recently submitted request merely because it arrived next.

### 30.5 Request Ownership

A request should retain enough context to identify its intended UI consumer.

Conceptually:

    PendingRequest {
        request_id

        request_kind

        target_view

        target_resource

        submitted_at

        timeout
    }

The target view may no longer be active when the response arrives.

The response may still update shared snapshot state.

### 30.6 Obsolete Responses

An asynchronous response may arrive after the user changes selection or submits a newer query.

Example:

    GetStreamHealth(A)
             |
             v
    User Selects Stream B
             |
             v
    GetStreamHealth(B)
             |
             v
    Response For A Arrives

The response for A must not overwrite B's detail panel.

### 30.7 Resource Generation Checks

Where applicable, responses must preserve:

    StreamGeneration

    ConnectionGeneration

    DerivedGeneration

    ConfigurationRevision

A response associated with an obsolete resource generation must not replace current-generation state.

### 30.8 Request Timeouts

A timed-out read query may be retried according to policy.

A timed-out mutation requires more care.

The TUI must query operation status or refresh authoritative state before resubmitting a command whose outcome may be ambiguous.

### 30.9 Client Reconnection

On control connection loss:

    Detect Disconnect
          |
          v
    Mark Control State Disconnected
          |
          v
    Mark Cached Snapshots Stale
          |
          v
    Schedule Reconnection
          |
          v
    Reestablish Control Session
          |
          v
    Negotiate Protocol
          |
          v
    Refresh Runtime Snapshots
          |
          v
    Reconcile Tracked Operations
          |
          v
    Resume Normal Interaction

The client must use bounded reconnection backoff.

### 30.10 Accepted Operations

An operation accepted by the daemon may continue after the TUI disconnects.

The TUI must not assume that the operation was cancelled.

After reconnection, it should query the operation outcome when the operation identity is known.

If the acceptance response was lost, the client must reconcile the requested effect using authoritative state or a supported idempotency key.

### 30.11 Notification Integration

The client may subscribe to operational notifications.

Notifications must carry sufficient identity and revision information to reject obsolete updates.

If notification continuity cannot be established, the client refreshes the affected authoritative snapshots.

### 30.12 Control Client Invariant

    Control Communication
    Must Remain Asynchronous,
    Bounded, And
    Independent Of Rendering.

---

## 31. Snapshot Cache and Refresh Strategy

The TUI uses a read-only cache of runtime snapshots.

The cache improves rendering performance and prevents repeated expensive queries.

It is not authoritative runtime state.

### 31.1 Snapshot Cache

Conceptual representation:

    SnapshotCache {
        runtime_snapshot

        source_stream_snapshots
        derived_stream_snapshots

        metrics_snapshot
        health_snapshot

        recording_snapshot
        retention_snapshot

        configuration_snapshot

        last_refresh_at
    }

Each cached object retains its own observation metadata.

### 31.2 Refresh Categories

The TUI should distinguish:

    Global Summary Refresh

    Stream List Refresh

    Selected Resource Refresh

    Metrics Refresh

    Configuration Refresh

    Operation Status Refresh

Not every category requires the same frequency.

### 31.3 Suggested Initial Refresh Policy

Illustrative defaults:

| Data | Refresh behavior |
|---|---|
| Global metrics | Every 1 second |
| Global health | Every 1 second |
| Source-stream list | Periodic or on lifecycle notification |
| Selected stream details | Every 1 second |
| Derived-stream details | Every 1 second |
| Recording summary | Every 1 second |
| Configuration | On opening Settings or revision notification |
| Operation progress | While relevant operation remains active |
| Static exchange capabilities | On configuration/capability revision |

These values are starting points and should be refined through profiling.

### 31.4 Render Frequency

Rendering frequency is independent of monitoring refresh frequency.

The renderer should redraw when:

- Input changes presentation state.
- A relevant snapshot changes.
- An animation or progress indicator requires refresh.
- The terminal is resized.

Constant high-frequency redraws are unnecessary for mostly static management screens.

### 31.5 Refresh Coalescing

When several views require the same metrics snapshot, the client should reuse one response.

For example:

    Dashboard
        |
        +-- Runtime Metrics

    Streams
        |
        +-- Runtime Metrics

These views should not independently initiate duplicate global queries during the same refresh cycle.

### 31.6 Selected Resource Queries

Detailed stream queries should target the selected resource rather than retrieve complete details for every registered stream.

For example:

    Stream List:
        Summary Snapshots

    Selected Stream:
        Detailed Health + Metrics

This reduces control-plane load.

### 31.7 Snapshot Freshness

Every cached snapshot must retain:

    CapturedAt

    ReceivedAt

    SnapshotEpoch

    SnapshotId

where available.

The UI must distinguish snapshot capture time from local receipt time.

### 31.8 Stale Snapshot Policy

If a snapshot exceeds its configured freshness limit:

    SnapshotStatus = Stale

The UI may preserve its values for diagnostic context.

However, it must visibly indicate that they are no longer current.

### 31.9 Disconnected State

When the daemon connection is lost:

    Control:
        Disconnected

    Metrics:
        Last Known

    Health:
        Last Known

The TUI must not continue displaying the last received healthy status as current.

### 31.10 Snapshot Updates

A snapshot update should replace only the applicable cache entry.

For example:

    New Recording Snapshot

must not invalidate an unrelated current source-stream snapshot unless the API provides an explicit dependency revision requiring it.

### 31.11 Snapshot Epoch Changes

After daemon restart, the snapshot epoch or runtime-instance identity may change.

The TUI must discard or invalidate cache entries belonging to the previous runtime instance.

It must not compare unrelated snapshot IDs as though they belonged to one uninterrupted sequence.

### 31.12 Cache Memory Limits

The cache must remain bounded by:

- Registered resource summaries.
- Selected detail snapshots.
- Recent metric history.
- Pending operation summaries.
- Local notifications.

The TUI must not retain unlimited snapshot history.

### 31.13 Refresh Invariant

    Rendering Reads
    Local Snapshot State.

    Snapshot Collection
    Remains Independent
    Of Terminal Frames.

---

## 32. Failure Handling and Offline Behavior

The TUI must remain usable when the daemon or individual runtime components become unavailable.

It must distinguish local UI errors from server-side operational failures.

### 32.1 Failure Categories

Relevant failures include:

    DaemonUnavailable

    ControlSocketUnavailable

    ProtocolVersionMismatch

    RequestTimeout

    AuthorizationFailure

    ConfigurationValidationFailure

    ConfigurationRevisionConflict

    RuntimeOperationFailure

    SnapshotUnavailable

    SnapshotStale

    TerminalRenderingFailure

### 32.2 Daemon Unavailable

If the TUI starts before the daemon:

    MarketForge Live
        Not Connected

    Control Socket:
        Unavailable

The UI should remain open and permit reconnection.

It must not display an empty stream registry as though the server had confirmed zero streams.

### 32.3 Control Connection Loss

If the control socket disconnects during normal operation:

- Display the disconnected state.
- Preserve local navigation.
- Preserve unsent configuration drafts.
- Mark runtime snapshots stale.
- Disable unsupported mutations.
- Attempt bounded reconnection.
- Refresh authoritative state after reconnecting.

### 32.4 Protocol Version Mismatch

If the daemon and TUI use incompatible protocol versions:

    Control Protocol:
        Incompatible

The UI must display the mismatch clearly.

It must not send mutating commands using an unknown serialization contract.

### 32.5 Authorization Failure

If the connected client lacks permission for a command:

- Preserve the current view.
- Display the authorization error.
- Avoid retry loops.
- Keep permitted read-only functionality available.

The TUI must not attempt to bypass authorization through alternate commands.

### 32.6 Component Failure

A component failure does not necessarily imply control-plane failure.

Example:

    Control:
        Connected

    Binance BTCUSDT Depth:
        Unhealthy

    PostgreSQL:
        Recovering

The TUI must continue operating and exposing diagnostics.

### 32.7 Ambiguous Mutation Results

If the control connection fails after submitting a mutation:

    Mutation Submitted
          |
          v
    Response Lost

The UI must not assume that the mutation failed.

It should display:

    Outcome Unknown

until authoritative reconciliation establishes the result.

### 32.8 Configuration Draft Preservation

Local drafts should survive ordinary view navigation and transient control disconnection while the TUI process remains running.

A disconnected draft must be revalidated against the current server revision before submission.

### 32.9 Terminal Resize

Terminal resizing must not corrupt application state.

If the terminal becomes too small:

- Render a compact resize notice.
- Preserve selection and drafts.
- Continue processing client events.
- Resume normal rendering after resizing.

### 32.10 Terminal Restoration

On normal exit or recoverable terminal errors, the application must restore terminal state.

This includes:

- Raw mode.
- Alternate screen state.
- Cursor visibility.
- Mouse capture, if enabled.

Cleanup must not depend on a successful daemon connection.

### 32.11 Panic and Error Cleanup

The TUI should use appropriate terminal guards or equivalent cleanup mechanisms.

Unexpected errors must not leave the terminal unnecessarily stuck in raw mode.

The implementation should preserve existing MarketForge terminal-cleanup patterns.

### 32.12 Offline Behavior Invariant

    Daemon Unavailability
    Must Not Corrupt
    Local UI State.

    Cached Runtime State
    Must Never Be
    Misrepresented As Current.

---

## 33. TUI Testing and Validation

The Ratatui interface requires deterministic tests for navigation, state updates, rendering, control communication, and failure handling.

Tests should reuse the existing MarketForge TUI testing infrastructure where appropriate.

### 33.1 Testing Categories

Required categories:

    Application State

    Navigation

    Keyboard Focus

    Tables

    Filters

    Dialogs

    Configuration Forms

    Control Client

    Snapshot Cache

    Async Operations

    Error Handling

    Terminal Rendering

### 33.2 Navigation Tests

Verify:

- Switching between all five views.
- Preserving view selection.
- Restoring focus after dialogs.
- Handling removed resources.
- Avoiding invalid row indices.
- Preserving filters across navigation.

### 33.3 Keyboard Tests

Verify:

- Global shortcuts.
- Table navigation.
- Text-input focus.
- Modal input precedence.
- Search mode.
- Disabled actions.
- Confirmation shortcuts.

Typing into a form must not trigger unrelated navigation commands.

### 33.4 Table Tests

Verify:

- Stable resource selection.
- Sorting.
- Filtering.
- Searching.
- Pagination.
- Empty results.
- Removed resources.
- Unavailable metrics.
- Dynamic snapshot updates.

### 33.5 Dialog Tests

Verify:

- Dialog opening.
- Focus ownership.
- Field editing.
- Validation feedback.
- Confirmation.
- Cancellation.
- Draft preservation.
- Target identity stability.

### 33.6 Configuration Tests

Verify:

    Draft Created
        -> No Daemon Mutation

    Draft Validated
        -> No Daemon Mutation

    Draft Applied
        -> Versioned Control Command

    Revision Conflict
        -> Draft Preserved

    Invalid Configuration
        -> User Receives Structured Error

### 33.7 Source Management Tests

Verify:

- Add source.
- Activate source.
- Deactivate source.
- Remove source.
- Show dependency conflicts.
- Display L2 synchronization progress.
- Display trade continuity failures.
- Display recording state independently.

### 33.8 Derived Management Tests

Verify:

- Create merge.
- Configure source rank.
- Configure ordering.
- Create conversion.
- Preview reference path.
- Detect dependency cycles.
- Display watermark stalls.
- Display conversion failures.
- Replace derived generation.
- Remove dependent streams safely.

### 33.9 Recording Tests

Verify:

- Recording summary rendering.
- Recording backlog display.
- Commit-frontier display.
- Recording coverage inspection.
- Recording enable/disable commands.
- Flush progress.
- Retention protection display.

### 33.10 Settings Tests

Verify:

- Effective-value display.
- Inherited-value display.
- Reset to inheritance.
- Hot-setting validation.
- Restart-required indication.
- Semantic diff presentation.
- Configuration revision conflicts.
- Partial convergence.
- Rollback confirmation.

### 33.11 Snapshot Tests

Verify:

- Initial snapshot retrieval.
- Snapshot replacement.
- Obsolete response rejection.
- Generation changes.
- Snapshot epoch changes.
- Stale data indicators.
- Missing measurements.
- Bounded cache history.

### 33.12 Control Client Tests

Use a deterministic fake Control Server.

Verify:

- Successful connection.
- Protocol negotiation.
- Request correlation.
- Out-of-order responses.
- Request timeout.
- Mid-response disconnection.
- Reconnection.
- Operation reconciliation.
- Authorization errors.

### 33.13 Rendering Tests

Where practical, render views using Ratatui's test backend.

Verify:

- Expected headings.
- Status labels.
- Table selection.
- Modal placement.
- Narrow terminal behavior.
- Missing-value representation.
- Error presentation.

Tests should not require a real interactive terminal.

### 33.14 Historical TUI Compatibility

Changes to shared widgets, theme helpers, or terminal infrastructure must preserve existing historical processing, merging, and conversion interfaces.

The Live TUI must not introduce regressions into previously implemented MarketForge terminal workflows.

### 33.15 Testing Invariant

    TUI Tests
    Must Verify
    Presentation And
    Control Behavior
    Without Requiring
    Live Exchange Connections.

---

## 34. Fault Injection and Property-Based Testing

The TUI must remain correct under arbitrary valid sequences of input events, daemon responses, and runtime changes.

### 34.1 Generated Events

Property-based tests may generate:

    NavigateView

    SelectRow

    ChangeFilter

    ChangeSort

    OpenDialog

    EditFormField

    ValidateDraft

    ApplyDraft

    CancelDialog

    SubmitControlRequest

    ReceiveControlResponse

    ReceiveNotification

    DisconnectControl

    ReconnectControl

    RemoveSelectedStream

    ChangeConfigurationRevision

    CompleteOperation

    FailOperation

    ResizeTerminal

### 34.2 Selection Safety

For every generated sequence:

    SelectedResourceId

must refer to the intended resource, regardless of table sorting, filtering, or refresh.

A stale row index must never redirect a destructive operation.

### 34.3 Draft Safety

A local configuration draft must not modify daemon state before submission.

Cancellation must leave daemon configuration unchanged.

### 34.4 Revision Safety

A draft based on an obsolete configuration revision must not be submitted as though it were current.

The server remains authoritative for final conflict detection.

### 34.5 Response Safety

An obsolete asynchronous response must not overwrite newer applicable state.

The TUI must distinguish:

    RequestId

    ResourceGeneration

    ConfigurationRevision

    SnapshotEpoch

### 34.6 Focus Safety

One keypress must not trigger conflicting actions through multiple focus handlers.

A modal confirmation must not allow an unrelated global shortcut to activate another resource operation.

### 34.7 Failure Safety

Daemon disconnection must not:

- Discard unsent drafts unexpectedly.
- Trigger uncontrolled mutation retries.
- Display stale health as current.
- Corrupt navigation state.
- Crash the TUI unnecessarily.

### 34.8 Rendering Safety

Arbitrary terminal sizes must not cause invalid layout dimensions or out-of-bounds rendering.

The application must remain responsive after returning to a supported terminal size.

### 34.9 Property-Based Invariant

    Arbitrary UI Events
    Must Not Cause
    Unauthorized Or
    Misidentified
    Runtime Mutations.

---

## 35. Performance and Resource Requirements

The Ratatui interface should remain lightweight.

Its workload consists primarily of rendering bounded operational snapshots and processing low-frequency management interactions.

### 35.1 Performance Principles

The TUI should:

- Render from cached state.
- Avoid blocking socket calls.
- Avoid unnecessary full-screen redraws.
- Limit snapshot history.
- Reuse existing widgets.
- Minimize repeated formatting.
- Avoid unbounded notification queues.
- Avoid expensive dependency-graph reconstruction.
- Preserve responsiveness during long-running operations.

### 35.2 Rendering Performance

Measure:

    FrameRenderDuration

    FrameRefreshRate

    TerminalWriteDuration

    ViewUpdateDuration

The TUI does not require extremely high frame rates.

Rendering should occur when state changes or a scheduled refresh is necessary.

### 35.3 Snapshot Processing

Snapshot updates should avoid cloning large collections unnecessarily.

Where practical, the client may use immutable shared snapshot objects.

Derived presentation data should be cached when beneficial.

### 35.4 Table Performance

Large tables should render only visible rows.

Sorting and filtering may operate on bounded local summaries.

For larger registries, server-side pagination and filtering may be used.

### 35.5 Dependency Inspector Performance

Dependency graphs must be rendered through bounded, collapsible structures.

The TUI must not recursively expand an entire large graph on every frame.

### 35.6 Notification Performance

Notifications must use bounded local storage.

Repeated identical conditions may be consolidated.

Old notifications should be removed according to the configured local policy.

### 35.7 Control Request Efficiency

The TUI should coalesce compatible monitoring requests.

It must not request every individual metric separately on each frame.

### 35.8 Resource Usage

Measure:

    TuiResidentMemory

    SnapshotCacheMemory

    PendingRequestCount

    NotificationQueueSize

    ControlConnectionMemory

Concrete limits should be established through representative testing.

### 35.9 Daemon Isolation

TUI performance must not affect daemon event-processing throughput.

A slow terminal, blocked renderer, or crashed TUI must not interrupt exchange acquisition.

### 35.10 Performance Invariant

    TUI Rendering
    Must Remain
    Independent Of
    Canonical Market-Data
    Processing Performance.

---

## 36. Final Acceptance Criteria

The Ratatui Management Interface is complete when the following requirements are satisfied.

### 36.1 Architecture

- The TUI runs independently of the Live daemon.
- It uses the versioned Control API.
- It does not access mutable daemon internals.
- Existing Ratatui/Crossterm infrastructure is reused where compatible.
- The Tokyo Night theme is preserved.
- Rendering and control communication remain separate.
- Closing the TUI does not stop MarketForge Live.

### 36.2 Navigation

- All five primary views are available.
- Keyboard navigation is consistent.
- Focus ownership is explicit.
- Table selections use stable resource identities.
- Filters and sorting preserve selection safely.
- Dialog focus is restored correctly.
- Contextual help is available.

### 36.3 Dashboard

- Global runtime state is visible.
- Exchange connectivity is summarized.
- Source and derived stream counts are available.
- Throughput and latency are displayed correctly.
- Stream health is summarized.
- Active recovery operations are visible.
- Recording and retention status are visible.
- Significant operational incidents are accessible.

### 36.4 Streams View

- Source streams can be listed and inspected.
- Exchange, market, and channel filtering work.
- Trade continuity is visible.
- L2 synchronization is visible.
- Stream activation is supported.
- Stream deactivation is supported.
- Stream removal is supported.
- Dependency conflicts are displayed.
- Recording state is independently visible.
- Asynchronous operation progress is available.

### 36.5 Derived Streams View

- Derived streams can be listed and inspected.
- Merge modes are visible.
- Source participation is visible.
- Watermark and ordering state are visible.
- Reorder buffer measurements are visible.
- Conversion paths are inspectable.
- Reference freshness is visible.
- Derived streams can be created and removed.
- Semantic configuration changes preserve generation boundaries.
- Dependency graphs can be inspected.

### 36.6 Recording and Retention

- PostgreSQL connection status is visible.
- Recording throughput is visible.
- Recording backlog is visible.
- Commit-frontier information is available.
- Recording gaps remain inspectable.
- Recording can be enabled and disabled.
- Flush operations can be requested.
- Retention memory and disk usage are visible.
- Protected retention intervals are inspectable.
- Recording and retention health remain independent.

### 36.7 Settings

- Desired configuration is visible.
- Effective configuration is visible.
- Applied values are distinguishable.
- Supported settings are editable.
- Inherited values are identifiable.
- Overrides can be reset.
- Configuration drafts can be validated.
- Semantic diffs can be inspected.
- Revision conflicts are handled.
- Partial runtime convergence is visible.
- Restart-required settings are identified.
- Optional Prometheus exporter configuration is visible.

### 36.8 Control Client

- Control connection establishment works.
- Protocol compatibility is validated.
- Requests and responses are correlated.
- Asynchronous operations can be tracked.
- Obsolete responses are rejected.
- Reconnection is supported.
- Ambiguous mutation outcomes are reconciled.
- Client-side resource usage remains bounded.

### 36.9 Failure Handling

- Daemon unavailability is displayed correctly.
- Control disconnections do not crash the UI.
- Cached snapshots are marked stale.
- Unsent drafts are preserved.
- Unsupported mutations are disabled.
- Authorization failures are handled.
- Terminal resize is safe.
- Terminal state is restored on exit.

### 36.10 Testing

- Navigation tests pass.
- Keyboard focus tests pass.
- Table tests pass.
- Dialog tests pass.
- Configuration form tests pass.
- Source-management tests pass.
- Derived-management tests pass.
- Recording and retention tests pass.
- Snapshot-cache tests pass.
- Control-client tests pass.
- Rendering tests pass.
- Fault-injection tests pass.
- Property-based tests pass.
- Historical TUI regression tests pass.
- Performance benchmarks establish acceptable overhead.

---

## 37. Final Architectural Contract

The Ratatui Management Interface provides a unified terminal management experience for MarketForge Live.

It combines operational monitoring, stream management, derived-stream configuration, recording controls, retention inspection, and runtime settings through the existing Control API.

The final architecture is:

    +---------------------------------------------------+
    |                 MARKETFORGE LIVE                  |
    |                                                   |
    |  Exchange Connections                             |
    |  Stream Registry                                  |
    |  Trade Processing / BookStore                     |
    |  Derived Streams                                  |
    |  Publisher                                        |
    |  Recording / Retention                            |
    |  Recovery Coordinator                             |
    |  Metrics Registry / HealthEvaluator               |
    |                                                   |
    |             Runtime Control API                   |
    +------------------------+--------------------------+
                             |
                             |
                      Unix Control Socket
                             |
                             |
    +------------------------v--------------------------+
    |                RATATUI CLIENT                     |
    |                                                   |
    |  Async Control Client                             |
    |          |                                        |
    |          v                                        |
    |  Snapshot Cache                                   |
    |          |                                        |
    |          v                                        |
    |  Application State                                |
    |          |                                        |
    |          +-------------------------------+        |
    |          |                               |        |
    |          v                               v        |
    |    Input Dispatcher                 Renderer      |
    |          |                               |        |
    |          v                               v        |
    |    Control Commands                Terminal       |
    |                                                   |
    |  Views:                                           |
    |    Dashboard                                      |
    |    Streams                                        |
    |    Derived Streams                                |
    |    Recording                                      |
    |    Settings                                       |
    +---------------------------------------------------+

The Ratatui application guarantees:

- Independent client execution.
- Consistent keyboard navigation.
- Shared Tokyo Night styling.
- Stable resource selection.
- Read-only runtime snapshot caching.
- Validated management interactions.
- Explicit asynchronous operation progress.
- Safe configuration editing.
- Structured operational diagnostics.
- Graceful control-plane disconnection handling.
- Bounded rendering and client resources.
- Compatibility with existing MarketForge TUI infrastructure.

It does not guarantee:

- Ownership of runtime processing.
- Independent market-data integrity validation.
- Complete historical event coverage.
- Immediate configuration convergence.
- Automatic recovery merely because the TUI is connected.
- Unlimited operational history.
- Application processing confirmation from socket-write success.
- Availability of external Prometheus or Grafana services.

### 37.1 Presentation Invariant

    Ratatui Displays
    Authoritative Runtime
    Information.

    It Does Not
    Reconstruct Runtime
    Correctness Independently.

### 37.2 Control Invariant

    UI Actions
         |
         v
    Validated Control Commands
         |
         v
    Configuration Manager
         |
         v
    Runtime Reconciler
         |
         v
    Authoritative Component Owners

The TUI must not bypass this model for persistent runtime configuration changes.

### 37.3 Independence Invariant

    MarketForge Live
    Continues Operating
    When Ratatui Is Closed,
    Disconnected,
    Or Unavailable.

### 37.4 Final Completion Rule

The Ratatui Management Interface is architecturally complete when:

    One Shared Control Protocol
               |
               v
    One Read-Only Snapshot Cache
               |
               v
    Consistent Application State
               |
               v
    Reusable Ratatui Components
               |
               v
    Five Operational Views
               |
               v
    Safe Management Interactions

All runtime commands must remain subject to the same authorization, configuration validation, and resource-ownership rules as commands submitted by Python or other management clients.

---

**End of Part 5 — Keyboard Interaction, Dialogs, Async Control Client, Refresh Strategy, Failure Handling, Testing, and Final Acceptance Criteria.**

**End of 14 — Ratatui Management Interface.**