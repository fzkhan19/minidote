defmodule Minidote do
  require Logger
  require ConflictFreeReplicatedDataType

  @moduledoc """
  Documentation for `DistributedDataStore`.

  Minidote is a causally consistent CRDT database that provides a key-value store
  where each key is a 3-tuple consisting of:
  - Key: binary() - the main identifier
  - Type: ConflictFreeReplicatedDataType.crdt_type_definition() - the CRDT type (e.g., Counter_PN_OB, Set_AW_OB)
  - Bucket: binary() - the namespace

  The API provides two main functions:
  - read_objects/2: Retrieve multiple objects atomically
  - update_objects/2: Modify multiple objects atomically

  Both functions support session guarantees through version tokens.
  """

  @type data_key :: {binary(), ConflictFreeReplicatedDataType.crdt_type_definition(), binary()}
  @type version_token :: map() | :ignore
  @type item_value :: any()
  @type item_operation :: atom()
  @type operation_args :: any()

  @doc """
  Simple ping function for basic testing.
  """
  def ping do
    :world
  end

  def start_service_link(service_name) do
    MinidoteServer.start_link(service_name)
  end

  @doc """
  Retrieve multiple objects from the database.

  Parameters:
  - data_items: List of keys to retrieve
  - version_token: Version token for session guarantees, or :ignore

  Returns:
  - {:ok, results, new_version_token} where results is a list of {data_key, item_value} tuples
  - {:error, reason} on failure

  If a data_key doesn't exist, the initial value for the CRDT type is returned.
  The version_token parameter ensures session guarantees - if provided from a previous
  operation, this retrieval will observe a state at least as recent as that operation.
  """
  @spec read_objects([data_key()], version_token()) ::
          {:ok, [{data_key(), item_value()}], version_token()} | {:error, any()}
  def read_objects(data_items, version_token) do
    Logger.notice("#{node()}: read_objects(#{inspect(data_items)}, #{inspect(version_token)})")

    # Validate input
    case validate_data_keys(data_items) do
      :ok ->
        # Forward the call to the named GenServer
        GenServer.call(MinidoteServer, {:read_objects, data_items, version_token})

      {:error, reason} ->
        Logger.warning("#{node()}: Invalid data keys in read_objects: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Modify multiple objects atomically.

  Parameters:
  - modifications: List of {data_key, item_operation, operation_args} tuples
  - version_token: Version token for session guarantees, or :ignore

  Returns:
  - {:ok, new_version_token} on success
  - {:error, reason} on failure

  All modifications are applied atomically. If multiple modifications target the same data_key,
  they are applied sequentially from left to right.
  The version_token parameter ensures session guarantees - if provided from a previous
  operation, this modification will be applied on a state at least as recent as that operation.
  """
  @spec update_objects([{data_key(), item_operation(), operation_args()}], version_token()) ::
          {:ok, version_token()} | {:error, any()}
  def update_objects(modifications, version_token) do
    Logger.notice("#{node()}: update_objects(#{inspect(modifications)}, #{inspect(version_token)})")

    # Validate input
    case validate_item_modifications(modifications) do
      :ok ->
        # Forward the call to the named GenServer
        GenServer.call(MinidoteServer, {:update_objects, modifications, version_token})

      {:error, reason} ->
        Logger.warning("#{node()}: Invalid modifications in update_objects: #{inspect(reason)}")
        {:error, reason}
    end
  end

  # Private helper functions for input validation

  defp validate_data_keys(data_items) when is_list(data_items) do
    case Enum.all?(data_items, &is_valid_data_key?/1) do
      true -> :ok
      false -> {:error, :invalid_data_key_format}
    end
  end

  defp validate_data_keys(_), do: {:error, :data_keys_not_list}

  defp validate_item_modifications(modifications) when is_list(modifications) do
    case Enum.all?(modifications, &is_valid_modification?/1) do
      true -> :ok
      false -> {:error, :invalid_modification_format}
    end
  end

  defp validate_item_modifications(_), do: {:error, :modifications_not_list}

  defp is_valid_data_key?({key, type, bucket})
       when is_binary(key) and is_atom(type) and is_binary(bucket) do
    # Convert atom type to module and check if it's valid
    case type_atom_to_crdt_impl(type) do
      {:ok, module} -> ConflictFreeReplicatedDataType.is_supported?(module)
      :error -> false
    end
  end

  defp is_valid_data_key?(_), do: false

  defp is_valid_modification?({{key, type, bucket}, operation, _args})
       when is_binary(key) and is_atom(type) and is_binary(bucket) and is_atom(operation) do
    # Convert atom type to module and check if it's valid
    case type_atom_to_crdt_impl(type) do
      {:ok, module} -> ConflictFreeReplicatedDataType.is_supported?(module)
      :error -> false
    end
  end

  defp is_valid_modification?(_), do: false

  # Map atom representations to actual CRDT modules
  defp type_atom_to_crdt_impl(:counter_pn_ob), do: {:ok, PositiveNegativeCounter}
  # Fixed naming
  defp type_atom_to_crdt_impl(:set_aw_ob), do: {:ok, Set_AW_OB}
  # Support both variants
  defp type_atom_to_crdt_impl(:set_aw_op), do: {:ok, Set_AW_OB}
  # Add more mappings as you implement more CRDTs
  # defp type_atom_to_crdt_impl(:counter_pn_sb), do: {:ok, Counter_PN_SB}
  # defp type_atom_to_crdt_impl(:mvregister_sb), do: {:ok, MVRegister_SB}
  defp type_atom_to_crdt_impl(_), do: :error

  # Helper function to get the actual CRDT module from atom type
  def get_crdt_implementation(type_atom) do
    case type_atom_to_crdt_impl(type_atom) do
      {:ok, module} -> module
      :error -> raise ArgumentError, "Unknown CRDT type: #{inspect(type_atom)}"
    end
  end
end
