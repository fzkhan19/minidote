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

  # ToDo: Improve type spec

  @type crdt_type_definition :: Set_AW_OB.t() | Counter_PN_OB.t()
  @type crdt_instance :: crdt_type_definition
  @type operation_payload :: {atom, term}
  @type propagation_effect :: term
  @type current_value :: term
  @type error_reason :: term

  @type internal_representation :: term
  @type internal_effect_representation :: term

  @callback new() :: internal_representation()
  @callback retrieve_value(internal_value :: internal_representation) :: current_value()
  @callback generate_effect(operation_payload(), internal_representation()) :: {:ok, propagation_effect()} | {:error, error_reason}
  @callback apply_effect(propagation_effect(), internal_representation()) :: {:ok, internal_representation()}
  # @callback requires_state_for_effect(operation_payload :: operation_payload()) :: {:ok, internal_representation()}
  @callback requires_state_for_effect(operation_payload :: operation_payload()) :: boolean()
  @callback are_equal(internal_representation(), internal_representation()) :: boolean()

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
  Creates a new instance of the given CRDT type.
  """
  def create_new(type) when is_supported?(type) do
    type.new() # Calls the new/0 function defined in the specific CRDT module
  end

  @doc """
  Retrieves the current value of the CRDT.
  """
  def get_current_value(type, state) do
    type.retrieve_value(state) # Calls the retrieve_value/1 function defined in the specific CRDT module
  end

  @doc """
  Computes the propagation effect of an operation on the CRDT.
  """
  def compute_propagation_effect(type, operation, state) do
    type.generate_effect(operation, state) # Calls the generate_effect/2 function defined in the specific CRDT module
  end

  @doc """
  Applies a propagation effect to the CRDT's state.
  """
  def apply_propagation_effect(type, effect, state) do
    type.apply_effect(effect, state) # Calls the apply_effect/2 function defined in the specific CRDT module
  end

  @doc """
  Checks if the CRDT requires its state to compute the effect of an operation.
  """
  def check_state_requirement_for_effect(type, operation) do
    type.requires_state_for_effect(operation) # Calls the requires_state_for_effect/1 function defined in the specific CRDT module
  end

  @doc """
  Checks if two CRDT states are equal.
  """
  def check_equality(type, state1, state2) do
    type.are_equal(state1, state2) # Calls the are_equal/2 function defined in the specific CRDT module
  end

  @spec to_binary_representation(internal_representation()) :: binary()
  @doc """
  Converts an internal representation of a CRDT to a binary format.
  """
  def to_binary_representation(term) do
    :erlang.term_to_binary(term) # Uses Erlang's term_to_binary for serialization
  end

  @spec from_binary_representation(binary()) :: {:ok, internal_representation()} | {:error, error_reason()}
  @doc """
  Converts a binary representation back to an internal representation of a CRDT.
  """
  def from_binary_representation(binary) do
    {:ok, :erlang.binary_to_term(binary)} # Uses Erlang's binary_to_term for deserialization
  rescue
    _ -> {:error, :invalid_binary_format} # Returns an error if the binary format is invalid
  end
end
