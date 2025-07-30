# Minidote Implementation and Enhancement Plan

This document outlines the necessary steps to bring the `DistributedDataStore` project into full compliance with the requirements specified in `ex-final.pdf`, including the implementation of the "Crash Recovery and Log Pruning" bonus feature.

## Implementation Guidelines

To ensure a structured and robust development process, the following guidelines will be adhered to during implementation:

*   **Feature-by-Feature Implementation**: Features will be implemented one at a time.
*   **Test After Each Change**: After implementing or changing a feature, `mix test` must be run. This is absolutely necessary. The application will be thoroughly tested to ensure all existing tests pass. Existing test cases will **not** be modified.
*   **Git Commit**: A Git commit will be created after each successful feature implementation/change and successful testing.
*   **No Test Case Modification**: Existing test cases are considered final and will not be altered.

## 1. Codebase Refactoring and Naming Conventions

*   **Objective**: Align project and module names with the `Minidote` specification.
*   **Tasks Status**:
    *   **Not Completed**: Renaming of the main application module from `DistributedDataStore` to `Minidote`. The `mix.exs`, `lib/minidote.ex`, `lib/minidote_server.ex`, and `test/minidote_test.exs` still use `DistributedDataStore` in module definitions and internal references.
    *   **Not Completed**: Renaming of `DistributedDataStore.Service` to `MinidoteServer`.
    *   **Not Completed**: Updating `mix.exs` to reflect the new application name.
    *   **Not Completed**: Renaming API functions in `Minidote` from `retrieve_data_items` to `read_objects` and `modify_data_items` to `update_objects`. These functions are still named `retrieve_data_items` and `modify_data_items` in `lib/minidote.ex` and `lib/minidote_server.ex`.
    *   **Not Completed**: Updating all internal references to these changed names and functions across the codebase.

## 2. CRDT Implementation

*   **Objective**: Implement the remaining CRDTs as required.
*   **Tasks Status**:
    *   **Not Completed**: The specific CRDT files (`lib/g_counter.ex`, `lib/or_set.ex`, `lib/lww_register.ex`, `lib/lww_e_set.ex`) were not found in the `lib/` directory.
    *   **Not Completed**: `lib/crdt.ex`'s `is_supported?` guard only includes `AddWinsSet` and `PositiveNegativeCounter`, not the CRDTs mentioned in the plan.
    *   **Not Completed**: `lib/minidote.ex`'s `type_atom_to_crdt_impl` function does not include mappings for the mentioned CRDTs beyond `PositiveNegativeCounter` and `Set_AW_OB`.
    *   **Not Verified**: Documentation for new CRDT modules (`@moduledoc`) could not be verified as the files were not found.

## 3. Crash Recovery and Log Pruning (Bonus Feature)

*   **Objective**: Implement persistent storage for CRDT states and operations to enable crash recovery.
*   **Tasks Status**:
    *   **Not Completed**: No implementation for persistent logging using `dets` (`:op_log`) was found in `lib/minidote_server.ex`.
    *   **Not Completed**: No implementation for state snapshots using `dets` (`:crdt_snapshots`) or a `MinidoteServer.take_snapshot/1` function was found in `lib/minidote_server.ex`.
    *   **Not Completed**: No log pruning strategy was found.
    *   **Not Completed**: No recovery mechanism upon node startup (rebuilding state from snapshot or replaying logs) was found in `MinidoteServer.init/1`.
    *   **Not Completed**: Integration into `MinidoteServer`'s `init` function and `handle_info` callbacks for crash recovery was not found.

## 4. Testing and Documentation

*   **Objective**: Ensure the new features are well-tested and documented.
*   **Tasks Status**:
    *   **Not Completed**: The mentioned unit and integration test files for CRDTs (`test/g_counter_test.exs`, `test/or_set_test.exs`, `test/lww_register_test.exs`, `test/lww_e_set_test.exs`) were not found.
    *   **Not Completed**: The test file for crash recovery (`test/crash_recovery_test.exs`) was not found.
    *   **Not Verified**: The `README.md` file was not checked, but given the missing features, it's unlikely to be fully updated as described.
    *   **Not Verified**: `lib/test_setup.ex` was not checked for `dets` cleanup, but the `dets` implementation itself is missing.

## 5. Future Work / Remaining Issues

Based on the current state of the project, the following significant work remains:

*   **Codebase Refactoring and Naming Conventions**: The project currently uses `DistributedDataStore` for module and application names. A full refactoring to `Minidote` as per the specification is still required across all relevant files (`mix.exs`, `lib/minidote.ex`, `lib/minidote_server.ex`, `test/minidote_test.exs`). API functions `retrieve_data_items` and `modify_data_items` also need to be renamed.

*   **CRDT Implementation**:
    *   The CRDTs listed in the original plan (`G-Counter`, `OR-Set`, `LWW-Register`, `LWW-Element-Set`) are not present in the `lib/` directory. These need to be implemented.
    *   The `is_supported?` guard in `lib/crdt.ex` and the `type_atom_to_crdt_impl` mapping in `lib/minidote.ex` need to be updated to include any newly implemented CRDTs.

*   **Crash Recovery and Log Pruning (Bonus Feature)**: This feature is entirely missing.
    *   Implementation of a persistent log (e.g., using `dets`) for update operations.
    *   Implementation of state snapshots to persistent storage.
    *   A mechanism for log pruning after successful snapshots.
    *   A recovery mechanism in `MinidoteServer.init/1` to rebuild state from snapshots and replay logs.
    *   Integration of snapshotting and recovery into `MinidoteServer`'s `handle_call` and `handle_info` callbacks.

*   **Testing**:
    *   Unit and integration tests for the CRDTs mentioned in the original plan are missing, as their respective files (`test/g_counter_test.exs`, `test/or_set_test.exs`, `test/lww_register_test.exs`, `test/lww_e_set_test.exs`) were not found. These tests need to be written once the CRDTs are implemented.
    *   Tests for the crash recovery and log pruning mechanism are also missing (`test/crash_recovery_test.exs` was not found). These tests need to be written once the feature is implemented.
    *   The existing `MinidoteTest` (originally `DistributedDataStoreTest`) still includes commented-out tests for distributed node setup. A more robust testing environment for distributed systems would be beneficial for future integration testing.

*   **Documentation**: The `README.md` and other documentation should be updated to accurately reflect the implemented features and usage instructions.

*   **Periodic Snapshots**: Once crash recovery is implemented, a mechanism for periodic, automatic snapshots (e.g., using `Process.send_after/3` or a separate supervisor) should be considered for a production-ready system.

*   **Robust Log Pruning**: The current log pruning strategy (if implemented) is a simplification. A more robust solution involving distributed consensus for log acknowledgment is needed for multi-node setups.

This concludes the assessment of the `DistributedDataStore` project against the `implementation_plan.md`. The core features mentioned in the plan are largely not implemented, and the plan requires significant updates to accurately reflect the project's current state and remaining work.