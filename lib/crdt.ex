defmodule ConflictFreeReplicatedDataType do
  @moduledoc """
  Defines the behaviour for Conflict-Free Replicated Data Types (CRDTs).

  This module defines the `@behaviour` for CRDTs, along with types, callbacks, and functions
  that ensure only valid CRDTs are created and used within the system. It also outlines the
  process for generating and applying updates to CRDTs.

  The module emphasizes that new updates are created by local downstream operations and, upon
  being received, applied as updates. Additionally, it explains that the `requires_state_for_effect`
  callback indicates whether the CRDT's local state is necessary to create the downstream
  effect or update.

  Naming pattern for CRDTs: <type>_<semantics>_<OB|SB>

  CRDT provided:
  Counter_PN_OB: PN-Counter aka Positive Negative Counter
  Set_AW_OB: Add-wins set, to be completed, see ex6
  """

@type crdt_module :: module()
@type crdt_state :: term
@type crdt_value :: term
@type crdt_operation :: {atom(), term()}
@type crdt_propagation_effect :: term
@type error_reason :: term

@doc """
Callback for creating a new, empty instance of the CRDT.
Returns the initial internal representation of the CRDT.
"""
@callback new() :: crdt_state()

@doc """
Callback for retrieving the current logical value of the CRDT.
Given the internal representation, returns the user-facing value.
"""
@callback retrieve_value(crdt_state()) :: crdt_value()

@doc """
Callback for computing the propagation effect of a local operation.
This function takes an operation payload and the current state of the CRDT,
and returns the effect that needs to be propagated to other replicas.
Does not modify the local state.
"""
@callback generate_effect(crdt_operation(), crdt_state()) ::
            {:ok, crdt_propagation_effect()} | {:error, error_reason()}

@doc """
Callback for applying a propagation effect to the local CRDT state.
This function takes a propagation effect (received from a local `generate_effect`
call or from a remote replica) and the current state, and returns the new state.
"""
@callback apply_effect(crdt_propagation_effect(), crdt_state()) ::
            {:ok, crdt_state()} | {:error, error_reason()}

@doc """
Callback to determine if an operation requires the current state to generate its effect.
Some operations (e.g., state-based CRDTs) might not need the current state to compute
their effect, while others (e.g., operation-based CRDTs) might.
"""
@callback requires_state_for_effect(crdt_operation()) :: boolean()

@doc """
Callback for checking if two CRDT states are logically equal.
This is used for testing and comparison purposes.
"""
@callback are_equal(crdt_state(), crdt_state()) :: boolean()

  @doc """
  Guard to check if a given type is a supported CRDT.
  """
  defguard is_supported?(type)
           when type == AddWinsSet or # Add-wins set
                  type == PositiveNegativeCounter or # PN-Counter
                  type == GCounter or # Grow-only counter
                  type == ORSet or # Observed-remove set
                  type == LWWRegister or # Last-write-wins register
                  type == LWWEWSet # Last-write-wins enable-wins set

  @doc """
  Creates a new instance of the given CRDT type module.

  This function delegates to the `new/0` callback defined in the specific CRDT module.
  The returned value is the initial internal state of the CRDT.

  ## Parameters
    - `type`: The CRDT module (e.g., `PositiveNegativeCounter`, `AddWinsSet`).

  ## Returns
    - The initial internal representation (`crdt_state()`) of the CRDT.
  """
  @spec create_new(crdt_module()) :: crdt_state()
  def create_new(type) when is_supported?(type) do
    type.new()
  end

  @doc """
  Retrieves the current logical value of the CRDT from its internal state.

  This function delegates to the `retrieve_value/1` callback defined in the specific CRDT module.
  It transforms the internal representation into a user-friendly value.

  ## Parameters
    - `type`: The CRDT module.
    - `state`: The current internal state (`crdt_state()`) of the CRDT.

  ## Returns
    - The current logical value (`crdt_value()`) of the CRDT.
  """
  @spec get_current_value(crdt_module(), crdt_state()) :: crdt_value()
  def get_current_value(type, state) do
    type.retrieve_value(state)
  end

  @doc """
  Computes the propagation effect of a local operation on the CRDT.

  This function delegates to the `generate_effect/2` callback defined in the specific CRDT module.
  It takes an operation and the current state, and produces an effect that can be applied
  locally or propagated to other replicas. This function does not modify the CRDT's state.

  ## Parameters
    - `type`: The CRDT module.
    - `operation`: The operation to perform, typically a tuple like `{atom, args}` (`crdt_operation()`).
    - `state`: The current internal state (`crdt_state()`) of the CRDT.

  ## Returns
    - `{:ok, effect}`: The propagation effect (`crdt_propagation_effect()`) if successful.
    - `{:error, reason}`: An error tuple if the effect cannot be generated.
  """
  @spec compute_propagation_effect(crdt_module(), crdt_operation(), crdt_state()) ::
            {:ok, crdt_propagation_effect()} | {:error, error_reason()}
  def compute_propagation_effect(type, operation, state) do
    type.generate_effect(operation, state)
  end

  @doc """
  Applies a propagation effect to the CRDT's internal state.

  This function delegates to the `apply_effect/2` callback defined in the specific CRDT module.
  It takes a propagation effect (which could be from a local operation or a remote replica)
  and the current state, and returns the new state after applying the effect.

  ## Parameters
    - `type`: The CRDT module.
    - `effect`: The propagation effect (`crdt_propagation_effect()`) to apply.
    - `state`: The current internal state (`crdt_state()`) of the CRDT.

  ## Returns
    - `{:ok, new_state}`: The new internal state (`crdt_state()`) after applying the effect.
    - `{:error, reason}`: An error tuple if the effect cannot be applied.
  """
  @spec apply_propagation_effect(crdt_module(), crdt_propagation_effect(), crdt_state()) ::
            {:ok, crdt_state()} | {:error, error_reason()}
  def apply_propagation_effect(type, effect, state) do
    type.apply_effect(effect, state)
  end

  @doc """
  Checks if a given CRDT operation requires the current state to compute its effect.

  This function delegates to the `requires_state_for_effect/1` callback defined in the
  specific CRDT module. Some CRDTs (e.g., state-based) might not need the current state
  to generate their effects, while others (e.g., operation-based CRDTs) might.

  ## Parameters
    - `type`: The CRDT module.
    - `operation`: The operation (`crdt_operation()`) in question.

  ## Returns
    - `boolean()`: `true` if the state is required, `false` otherwise.
  """
  @spec check_state_requirement_for_effect(crdt_module(), crdt_operation()) :: boolean()
  def check_state_requirement_for_effect(type, operation) do
    type.requires_state_for_effect(operation)
  end

  @doc """
  Checks if two internal CRDT states are logically equal.

  This function delegates to the `are_equal/2` callback defined in the specific CRDT module.
  It is primarily used for testing and comparison purposes, ensuring that two CRDT instances
  represent the same logical value, even if their internal representations differ slightly.

  ## Parameters
    - `type`: The CRDT module.
    - `state1`: The first internal state (`crdt_state()`).
    - `state2`: The second internal state (`crdt_state()`).

  ## Returns
    - `boolean()`: `true` if the states are equal, `false` otherwise.
  """
  @spec check_equality(crdt_module(), crdt_state(), crdt_state()) :: boolean()
  def check_equality(type, state1, state2) do
    type.are_equal(state1, state2)
  end

  @doc """
  Converts an internal representation of a CRDT to a binary format.

  This function uses Erlang's `term_to_binary/1` for serialization, allowing CRDT states
  to be stored persistently or transmitted across the network.

  ## Parameters
    - `term`: The internal representation (`crdt_state()`) or any Elixir term to serialize.

  ## Returns
    - `binary()`: The binary representation of the term.
  """
  @spec to_binary_representation(term()) :: binary()
  def to_binary_representation(term) do
    :erlang.term_to_binary(term)
  end

  @doc """
  Converts a binary representation back to an internal representation of a CRDT.

  This function uses Erlang's `binary_to_term/1` for deserialization. It is used to
  reconstruct CRDT states from persistent storage or network transmissions.

  ## Parameters
    - `binary`: The binary data to deserialize.

  ## Returns
    - `{:ok, term}`: The reconstructed Elixir term (`crdt_state()` or any term) if successful.
    - `{:error, :invalid_binary_format}`: An error tuple if the binary data is not a valid Erlang term.
  """
  @spec from_binary_representation(binary()) :: {:ok, term()} | {:error, error_reason()}
  def from_binary_representation(binary) do
    {:ok, :erlang.binary_to_term(binary)}
  rescue
    _ -> {:error, :invalid_binary_format}
  end
end
