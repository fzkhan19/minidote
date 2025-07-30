defmodule LWWRegister do
  @behaviour ConflictFreeReplicatedDataType
  @moduledoc """
  `LWWRegister` (Last-Write-Wins Register) is a CRDT that stores a single value,
  where conflicts are resolved by comparing timestamps. The value with the
  latest timestamp wins. If timestamps are equal, a tie-breaking rule (e.g.,
  lexicographical comparison of values) can be applied.
  """

  @doc """
  Creates a new, empty `LWWRegister`.
  """
  @impl ConflictFreeReplicatedDataType
  def new do
    %{value: nil, timestamp: 0} # Default empty state
  end

  @doc """
  Creates a new `LWWRegister` with an initial value and timestamp.
  """
  def create(value, timestamp) do
    %{value: value, timestamp: timestamp}
  end

  @doc """
  Updates the `LWWRegister` with a new value and timestamp.
  """
  def update(register, new_value, new_timestamp) do
    if new_timestamp > register.timestamp do
      %{value: new_value, timestamp: new_timestamp}
    else
      register
    end
  end

  @doc """
  Returns the current value of the `LWWRegister`.
  """
  def value(register) do
    register.value
  end

  @doc """
  Merges two `LWWRegister`s.
  The register with the later timestamp wins. If timestamps are equal,
  the value from `register1` is chosen (arbitrary tie-breaking).
  """
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
  def apply_effect({{new_value, new_timestamp}, _sender_node}, register) do
    if new_timestamp > register.timestamp do
      {:ok, %{value: new_value, timestamp: new_timestamp}}
    else
      {:ok, register}
    end
  end

  @impl ConflictFreeReplicatedDataType
  def retrieve_value(register) do
    register.value
  end

  @impl ConflictFreeReplicatedDataType
  def generate_effect({:update, new_value, new_timestamp}, _register) do
    {:ok, {new_value, new_timestamp}}
  end

  @impl ConflictFreeReplicatedDataType
  def requires_state_for_effect({:update, _new_value, _new_timestamp}) do
    false
  end

  @impl ConflictFreeReplicatedDataType
  def are_equal(register1, register2) do
    register1 == register2
  end
end