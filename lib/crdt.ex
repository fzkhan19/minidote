defmodule ConflictFreeReplicatedDataType do
  @moduledoc """
  Documentation for `CRDT`.

  This module defines types, callbacks for behaviours and the functions that use them.
  It ensures only valid CRDTs are created.
  New updates are created by local downstream operations and upon being received applied as updates.
  The require_state_downstream callback states if the crdt's local state is needed to create the downstream effect / update or not.

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

  @callback initialize() :: internal_representation()
  @callback retrieve_value(internal_value :: internal_representation) :: current_value()
  @callback generate_effect(operation_payload(), internal_representation()) :: {:ok, propagation_effect()} | {:error, error_reason}
  @callback apply_effect(propagation_effect(), internal_representation()) :: {:ok, internal_representation()}
  # @callback requires_state_for_effect(operation_payload :: operation_payload()) :: {:ok, internal_representation()}
  @callback requires_state_for_effect(operation_payload :: operation_payload()) :: boolean()
  @callback are_equal(internal_representation(), internal_representation()) :: boolean()

  # ToDo: Add new types as needed
  defguard is_supported?(type)
           when type == AddWinsSet or
                  type == PositiveNegativeCounter

  def create_new(type) when is_supported?(type) do
    type.initialize()
  end

  def get_current_value(type, state) do
    type.retrieve_value(state)
  end

  def compute_propagation_effect(type, operation, state) do
    type.generate_effect(operation, state)
  end

  def apply_propagation_effect(type, effect, state) do
    type.apply_effect(effect, state)
  end

  def check_state_requirement_for_effect(type, operation) do
    type.requires_state_for_effect(operation)
  end

  def check_equality(type, state1, state2) do
    type.are_equal(state1, state2)
  end

  @spec to_binary_representation(internal_representation()) :: binary()
  def to_binary_representation(term) do
    :erlang.term_to_binary(term)
  end

  @spec from_binary_representation(binary()) :: {:ok, internal_representation()} | {:error, error_reason()}
  def from_binary_representation(binary) do
    {:ok, :erlang.binary_to_term(binary)}
  rescue
    _ -> {:error, :invalid_binary_format}
  end
end
