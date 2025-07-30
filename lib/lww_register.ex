defmodule LWWRegister do
  @behaviour ConflictFreeReplicatedDataType
  @moduledoc """
  `LWWRegister` (Last-Write-Wins Register) is a Conflict-Free Replicated Data Type (CRDT)
  that stores a single value. Conflicts are resolved by comparing timestamps:
  the value associated with the latest timestamp wins. If timestamps are equal,
  a tie-breaking rule (e.g., lexicographical comparison of values, or node ID)
  can be applied. This implementation uses a simple tie-breaking rule that
  favors the existing value if timestamps are equal.
  """

  @type t :: %{value: any(), timestamp: non_neg_integer()}
  @type operation_payload :: {:update, any(), non_neg_integer()}
  @type propagation_effect :: {any(), non_neg_integer()} # {value, timestamp}

  @impl ConflictFreeReplicatedDataType
  @doc """
  Creates a new, empty `LWWRegister`.

  The initial state has a `nil` value and a timestamp of 0.

  ## Returns
    - `t()`: A new LWWRegister with an initial state.
  """
  @spec new() :: t()
  def new do
    %{value: nil, timestamp: 0} # Default empty state
  end

  @doc """
  Creates a new `LWWRegister` with an initial value and timestamp.

  This function provides a way to initialize an LWW-Register with a specific
  value and timestamp, outside of the CRDT behavior callbacks.

  ## Parameters
    - `value`: The initial value for the register.
    - `timestamp`: The initial timestamp for the value.

  ## Returns
    - `t()`: A new LWWRegister with the specified value and timestamp.
  """
  @spec create(any(), non_neg_integer()) :: t()
  def create(value, timestamp) do
    %{value: value, timestamp: timestamp}
  end

  @doc """
  Updates the `LWWRegister` with a new value and timestamp.

  This function applies the Last-Write-Wins rule: if the `new_timestamp` is
  greater than the register's current timestamp, the register is updated.
  Otherwise, the current register state is retained.

  ## Parameters
    - `register`: The current state of the `LWWRegister` (`t()`).
    - `new_value`: The new value to set.
    - `new_timestamp`: The timestamp associated with the new value.

  ## Returns
    - `t()`: The updated LWWRegister state or the original state if the new timestamp is not greater.
  """
  @spec update(t(), any(), non_neg_integer()) :: t()
  def update(register, new_value, new_timestamp) do
    if new_timestamp > register.timestamp do
      %{value: new_value, timestamp: new_timestamp}
    else
      register
    end
  end

  @doc """
  Returns the current value of the `LWWRegister`.

  ## Parameters
    - `register`: The current state of the `LWWRegister` (`t()`).

  ## Returns
    - `any()`: The current value stored in the register.
  """
  @spec value(t()) :: any()
  def value(register) do
    register.value
  end

  @doc """
  Merges two `LWWRegister`s.

  The register with the later timestamp wins. If timestamps are equal,
  the value from `register1` is chosen (arbitrary tie-breaking).

  ## Parameters
    - `register1`: The first `LWWRegister` state (`t()`).
    - `register2`: The second `LWWRegister` state (`t()`).

  ## Returns
    - `t()`: The merged `LWWRegister` state.
  """
  @spec merge(t(), t()) :: t()
  def merge(register1, register2) do
    if register1.timestamp > register2.timestamp do
      register1
    else if register2.timestamp > register1.timestamp do
      register2
    else
      # Tie-breaking: if timestamps are equal, prefer register1's value.
      # A more robust tie-breaking could be lexicographical comparison of values
      # or node ID.
      register1
    end
    end
  end
  @impl ConflictFreeReplicatedDataType
  @doc """
  Applies an update effect to the `LWWRegister`.

  This function compares the `new_timestamp` from the effect with the register's
  current timestamp. If the `new_timestamp` is later, the register's value and
  timestamp are updated. If the timestamps are equal or the `new_timestamp` is
  older, the existing register state is retained (last-write-wins).

  ## Parameters
    - `{{new_value, new_timestamp}, _sender_node}`: The update effect containing the new value and its timestamp.
      - `new_value`: The new value to set.
      - `new_timestamp`: The timestamp associated with the new value.
    - `register`: The current state of the `LWWRegister` (`t()`).

  ## Returns
    - `{:ok, t()}`: The updated LWWRegister state.
  """
  @spec apply_effect({propagation_effect(), node()}, t()) :: {:ok, t()}
  def apply_effect({{new_value, new_timestamp}, _sender_node}, register) do
    if new_timestamp > register.timestamp do
      {:ok, %{value: new_value, timestamp: new_timestamp}}
    else
      {:ok, register}
    end
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Retrieves the current value stored in the `LWWRegister`.

  ## Parameters
    - `register`: The current state of the `LWWRegister` (`t()`).

  ## Returns
    - `any()`: The current value of the register.
  """
  @spec retrieve_value(t()) :: any()
  def retrieve_value(register) do
    register.value
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Generates an update effect for the `LWWRegister`.

  The effect consists of the new value and its associated timestamp.

  ## Parameters
    - `{:update, new_value, new_timestamp}`: The update operation.
      - `new_value`: The value to set.
      - `new_timestamp`: The timestamp of the update.
    - `_register`: The current state of the `LWWRegister` (not used for effect generation).

  ## Returns
    - `{:ok, propagation_effect()}`: A tuple containing the generated update effect.
  """
  @spec generate_effect({:update, any(), non_neg_integer()}, t()) :: {:ok, propagation_effect()}
  def generate_effect({:update, new_value, new_timestamp}, _register) do
    {:ok, {new_value, new_timestamp}}
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Indicates that the `LWWRegister` does not require its current state to generate an effect.

  The effect of an update operation on an LWW-Register is simply the new value
  and its timestamp, which can be determined without knowing the register's
  current state.

  ## Parameters
    - `{:update, _new_value, _new_timestamp}`: The operation payload.

  ## Returns
    - `boolean()`: `false` because the state is not required for effect generation.
  """
  @spec requires_state_for_effect(operation_payload()) :: boolean()
  def requires_state_for_effect({:update, _new_value, _new_timestamp}) do
    false
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Checks if two `LWWRegister` states are equal.

  Equality is determined by comparing both their `value` and `timestamp` fields.

  ## Parameters
    - `register1`: The first `LWWRegister` state (`t()`).
    - `register2`: The second `LWWRegister` state (`t()`).

  ## Returns
    - `boolean()`: `true` if both value and timestamp are identical, `false` otherwise.
  """
  @spec are_equal(t(), t()) :: boolean()
  def are_equal(register1, register2) do
    register1 == register2
  end
end
