defmodule Minidote do
  require Logger
  require CRDT

  @moduledoc """
  Documentation for `Minidote`.

  Minidote is a causally consistent CRDT database that provides a key-value store
  where each key is a 3-tuple consisting of:
  - Key: binary() - the main identifier
  - Type: CRDT.t() - the CRDT type (e.g., Counter_PN_OB, Set_AW_OB)
  - Bucket: binary() - the namespace

  The API provides two main functions:
  - read_objects/2: Read multiple objects atomically
  - update_objects/2: Update multiple objects atomically

  Both functions support session guarantees through vector clocks.
  """

  @type key :: {binary(), CRDT.t(), binary()}
  @type clock :: map() | :ignore
  @type value :: any()
  @type operation :: atom()
  @type args :: any()

  @doc """
  Simple hello function for basic testing.
  """
  def hello do
    :world
  end

  def start_link(server_name) do
    Minidote.Server.start_link(server_name)
  end

  @doc """
  Read multiple objects from the database.

  Parameters:
  - objects: List of keys to read
  - clock: Vector clock for session guarantees, or :ignore

  Returns:
  - {:ok, results, new_clock} where results is a list of {key, value} tuples
  - {:error, reason} on failure

  If a key doesn't exist, the initial value for the CRDT type is returned.
  The clock parameter ensures session guarantees - if provided from a previous
  operation, this read will observe a state at least as recent as that operation.
  """
  @spec read_objects([key()], clock()) ::
          {:ok, [{key(), value()}], clock()} | {:error, any()}
  def read_objects(objects, clock) do
    Logger.notice("#{node()}: read_objects(#{inspect(objects)}, #{inspect(clock)})")

    # Validate input
    case validate_keys(objects) do
      :ok ->
        # Forward the call to the named GenServer
        GenServer.call(Minidote.Server, {:read_objects, objects, clock})

      {:error, reason} ->
        Logger.warning("#{node()}: Invalid keys in read_objects: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Update multiple objects atomically.

  Parameters:
  - updates: List of {key, operation, args} tuples
  - clock: Vector clock for session guarantees, or :ignore

  Returns:
  - {:ok, new_clock} on success
  - {:error, reason} on failure

  All updates are applied atomically. If multiple updates target the same key,
  they are applied sequentially from left to right.
  The clock parameter ensures session guarantees - if provided from a previous
  operation, this update will be applied on a state at least as recent as that operation.
  """
  @spec update_objects([{key(), operation(), args()}], clock()) ::
          {:ok, clock()} | {:error, any()}
  def update_objects(updates, clock) do
    Logger.notice("#{node()}: update_objects(#{inspect(updates)}, #{inspect(clock)})")

    # Validate input
    case validate_updates(updates) do
      :ok ->
        # Forward the call to the named GenServer
        GenServer.call(Minidote.Server, {:update_objects, updates, clock})

      {:error, reason} ->
        Logger.warning("#{node()}: Invalid updates in update_objects: #{inspect(reason)}")
        {:error, reason}
    end
  end

  # Private helper functions for input validation

  defp validate_keys(objects) when is_list(objects) do
    case Enum.all?(objects, &valid_key?/1) do
      true -> :ok
      false -> {:error, :invalid_key_format}
    end
  end

  defp validate_keys(_), do: {:error, :keys_not_list}

  defp validate_updates(updates) when is_list(updates) do
    case Enum.all?(updates, &valid_update?/1) do
      true -> :ok
      false -> {:error, :invalid_update_format}
    end
  end

  defp validate_updates(_), do: {:error, :updates_not_list}

  defp valid_key?({key, type, bucket})
       when is_binary(key) and is_atom(type) and is_binary(bucket) do
    # Convert atom type to module and check if it's valid
    case atom_to_crdt_module(type) do
      {:ok, module} -> CRDT.valid?(module)
      :error -> false
    end
  end

  defp valid_key?(_), do: false

  defp valid_update?({{key, type, bucket}, operation, _args})
       when is_binary(key) and is_atom(type) and is_binary(bucket) and is_atom(operation) do
    # Convert atom type to module and check if it's valid
    case atom_to_crdt_module(type) do
      {:ok, module} -> CRDT.valid?(module)
      :error -> false
    end
  end

  defp valid_update?(_), do: false

  # Map atom representations to actual CRDT modules
  defp atom_to_crdt_module(:counter_pn_ob), do: {:ok, Counter_PN_OB}
  # Fixed naming
  defp atom_to_crdt_module(:set_aw_ob), do: {:ok, Set_AW_OB}
  # Support both variants
  defp atom_to_crdt_module(:set_aw_op), do: {:ok, Set_AW_OB}
  # Add more mappings as you implement more CRDTs
  # defp atom_to_crdt_module(:counter_pn_sb), do: {:ok, Counter_PN_SB}
  # defp atom_to_crdt_module(:mvregister_sb), do: {:ok, MVRegister_SB}
  defp atom_to_crdt_module(_), do: :error

  # Helper function to get the actual CRDT module from atom type
  def get_crdt_module(type_atom) do
    case atom_to_crdt_module(type_atom) do
      {:ok, module} -> module
      :error -> raise ArgumentError, "Unknown CRDT type: #{inspect(type_atom)}"
    end
  end
end
