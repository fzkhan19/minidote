defmodule CRDT do
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

  @type t :: Set_AW_OB.t() | Counter_PN_OB.t()
  @type crdt :: t
  @type update :: {atom, term}
  @type effect :: term
  @type value :: term
  @type reason :: term

  @type internal_crdt :: term
  @type internal_effect :: term

  @callback new() :: internal_crdt()
  @callback value(internal_value :: internal_crdt) :: value()
  @callback downstream(update(), internal_crdt()) :: {:ok, internal_effect()} | {:error, reason}
  @callback update(internal_effect(), internal_crdt()) :: {:ok, internal_crdt()}
  # @callback require_state_downstream(update :: update()) :: {:ok, internal_crdt()}
  @callback require_state_downstream(update :: update()) :: boolean()
  @callback equal(internal_crdt(), internal_crdt()) :: boolean()

  # ToDo: Add new types as needed
  defguard valid?(type)
           when type == Set_AW_OB or
                  type == Counter_PN_OB

  def new(type) when valid?(type) do
    type.new()
  end

  def value(type, state) do
    type.value(state)
  end

  def downstream(type, update, state) do
    type.downstream(update, state)
  end

  def update(type, effect, state) do
    type.update(effect, state)
  end

  def require_state_downstream(type, update) do
    type.require_state_downstream(update)
  end

  @spec to_binary(internal_crdt()) :: binary()
  def to_binary(term) do
    :erlang.term_to_binary(term)
  end

  @spec from_binary(binary()) :: {:ok, internal_crdt()} | {:error, reason()}
  def from_binary(binary) do
    {:ok, :erlang.binary_to_term(binary)}
  rescue
    _ -> {:error, :invalid_binary}
  end
end
