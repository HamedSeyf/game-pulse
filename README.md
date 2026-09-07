# GamePulse

GamePulse is a C++23 game telemetry project that simulates player activity, processes events through a concurrent pipeline, and builds snapshots of player health and position. It explores the systems work behind an event-driven application: coordinating producers and consumers, controlling buffering, and managing ownership and worker lifetimes.

This is a personal learning and portfolio project that aims to cover and demonstrate modern C++ features, particularly C++20 and selected C++23 additions, in a cohesive application. It is under active development.

## Architecture

The core data flow is **Simulators → Queue → Pipeline → Analytics → Reporting**. Reporting periodically reads analytics snapshots and logs each player's health and position.

| Module | Responsibility |
| --- | --- |
| [`game_pulse.domain`](src/modules/game_pulse.domain.ixx) | Defines spawn, movement, and shot events, player snapshots, configuration, atomic ID generation, and the shared tick clock. |
| [`game_pulse.simulator`](src/modules/game_pulse.simulator.ixx) | Generates weighted random player events on worker threads using seeded random engines, and reports progress through each simulation tick. |
| [`game_pulse.queue`](src/modules/game_pulse.queue.ixx) | Buffers events in a fixed-capacity ring queue, coordinates blocking producers and consumers, and tracks producer watermarks to determine which ticks are ready for consumption. |
| [`game_pulse.pipeline`](src/modules/game_pulse.pipeline.ixx) | Consumes batches on a worker thread, sorts each batch by tick and event ID, and dispatches it synchronously to registered processors through a common interface. |
| [`game_pulse.analytics`](src/modules/game_pulse.analytics.ixx) | Applies events to player health and position, and exposes snapshots protected by a reader/writer mutex. |
| [`game_pulse.reporting`](src/modules/game_pulse.reporting.ixx) | Runs a dedicated worker that reads analytics snapshots and logs player health and position, with a configurable interval between reports. |

[`main.cpp`](src/main.cpp) parses configuration, sets up file logging, creates the components, and starts the simulators, processing pipeline, and reporting worker.

## C++ and engineering focus

Features used across the application and its `hamed_common` dependency include:

| Standard | Feature | Use in this project |
| --- | --- | --- |
| C++20 | Named modules and header-unit imports | Separate interfaces and implementation units, and import standard-library headers. |
| C++20 | Concepts, `requires` expressions, and `requires` clauses | Express type requirements through `ChronoDuration`, `NothrowQueuePayload`, and constrained templates in `hamed_common`. Simple, type, and compound requirements check supported operations and their result types. |
| C++20 | Standard-library concepts | Use constraints such as `std::destructible`, `std::constructible_from`, `std::predicate`, `std::same_as`, and `std::convertible_to` in payload and shared utility contracts. |
| C++20 | Lambdas with explicit template parameter lists | Share command-line value parsing between numeric values and chrono durations. |
| C++20 | Designated initializers | Construct event payloads and event-generation settings with named fields. |
| C++20 | `std::jthread`, `std::stop_token`, and interruptible condition-variable waits | Manage workers and cooperative cancellation, including queue waits, waits for the next simulation tick, and waits between snapshot reports. |
| C++20 | `std::span` | Pass non-owning views of player IDs and event batches across component boundaries. |
| C++20 | `std::erase` and `std::string_view::starts_with` | Build each player's target list and recognize command-line options. |
| C++20 | `std::remove_cvref_t` | Normalize types before checking smart-pointer constraints in `hamed_common`. |
| C++20 | `[[no_unique_address]]` | Annotate the ring queue's allocator member in `hamed_common` to permit storage overlap where supported. |
| C++23 | `std::expected` and `std::unexpected` | Return either a successful result or an explicit error from queue and processor-registration operations. |
| C++23 | `std::to_underlying` | Convert state enums to their underlying numeric values for logging. |

The project also uses earlier modern C++ facilities:

- **C++17 `std::optional` and `std::nullopt`** represent potentially absent events, registration handles, and producer watermarks.
- **C++17 `std::variant` and `std::visit`** store spawn, move, and shot payloads in a type-safe event representation and dispatch analytics processing by payload type.
- **C++17 `std::string_view` and `std::from_chars`** provide non-owning argument views and numeric parsing.
- **C++17 `if constexpr`, structured bindings, and initializer statements in `if`** support generic parsing, event dispatch, and scoped result handling.
- **C++17 class template argument deduction and `[[nodiscard]]`** simplify lock declarations and flag discarded results.
- **RAII, smart pointers, move semantics, type traits, and `static_assert`** express ownership, resource management, and compile-time contracts.
- **`std::chrono`, atomics, and scoped reader/writer locks** support tick timing, shared IDs, and synchronized snapshots.

The source provides concrete examples of bounded buffering and backpressure, batch processing, extensible processor registration, and state-driven component lifecycles.


## Build setup

The current development environment is Windows/MSVC. The project requires CMake 3.28 or newer and a C++23 toolchain with support for the module and standard-library header imports used in the source.

Dependencies are included as Git submodules: `hamed_common` for shared utilities and `spdlog` for logging.

From a checkout with a compatible Visual Studio C++ toolchain installed:

```powershell
git submodule update --init --recursive
cmake -S . -B build
cmake --build build --config Debug
```

## Runtime configuration

Command-line parameters let you change queue capacity, processing batch size, the number of simulated players, and the snapshot reporting interval without rebuilding. Both `--option value` and `--option=value` are supported.

| Parameter | Default | Purpose and current status |
| --- | --- | --- |
| `--queue-size` | `200` | Maximum number of buffered events. |
| `--batch-size` | `10` | Maximum number of events consumed in a processing batch. |
| `--player-count` | `5` | Number of simulated players. The current simulator requires at least two players. |
| `--snapshot-interval` | `500` | Wait interval in milliseconds between snapshot reports; must be positive. |
| `--shutdown-gracefully` | `true` | Intended shutdown policy; accepts `true`/`false` or `1`/`0`, with application-level integration still pending. |

For example, using the Visual Studio Debug build:

```powershell
.\build\Debug\game_pulse.exe --queue-size 1000 --batch-size 50 --player-count 10
```

Use positive queue and batch sizes and a positive snapshot interval, with the batch size no larger than the queue capacity. Tick duration, event-generation weights, and random seeds are currently configured in source, so full command-line configurability is still a development goal.


## Current status

The simulation, analytics, and reporting path is implemented. Reporting periodically logs player health and position from analytics snapshots to `gamepulse.log`, alongside diagnostic output. Application-level shutdown orchestration is still in progress; `--shutdown-gracefully` is parsed but is not yet connected to the shutdown flow.
