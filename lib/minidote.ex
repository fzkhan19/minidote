defmodule Minidote do
  require Logger
  require ConflictFreeReplicatedDataType

  @moduledoc """
  The `Minidote` module provides the public API for the Minidote key-CRDT database.

  Minidote is a causally consistent CRDT database that provides a key-value store
  where each key is a 3-tuple as defined by `key()` type:
  - `Key`: A binary (string) representing the main identifier for the data item.
  - `Type`: An atom representing the CRDT type (e.g., `:counter_pn_ob`, `:set_aw_op`),
            which corresponds to a specific CRDT module implementing the `ConflictFreeReplicatedDataType` behaviour.
  - `Bucket`: A binary (string) representing the namespace or bucket for the data item.

  The API consists of two primary functions: `read_objects/2` (implemented as `retrieve_data_items/2`)
  and `update_objects/2` (implemented as `modify_data_items/2`). Both functions support
  session guarantees through version tokens, ensuring that subsequent operations observe
  a state at least as recent as previous ones.
  """

  @type data_key :: {binary(), atom(), binary()}
  @type version_token :: map() | :ignore
  @type item_value :: any()
  @type item_operation :: atom()
  @type operation_args :: any()

  @doc """
  A simple ping function for basic connectivity testing.
  Returns `:world`.
  """
  @spec ping() :: :world
  def ping do
    :world
  end

  @doc """
  Starts the MinidoteServer as a linked GenServer process.

  This function is typically called during application startup to initialize the
  Minidote database server. The server will be registered globally under the
  provided `service_name`.

  ## Parameters
    - `service_name`: The name to register the MinidoteServer process under (e.g., `:minidote_server`).

  ## Returns
    - `{:ok, pid}`: If the server starts successfully, where `pid` is the process ID of the server.
    - `{:error, reason}`: If the server fails to start.
  """
  @spec start_service_link(atom()) :: {:ok, pid()} | {:error, any()}
  def start_service_link(service_name) do
    MinidoteServer.start_link(service_name)
  end

  @doc """
  Retrieves the current values of one or more CRDT objects from the database.

  This function corresponds to the `read_objects` API call described in the project
  specification. It takes a list of `data_key` tuples and an optional `version_token`
  for session guarantees.

  If a `data_key` does not exist in the database, the initial value for its specified
  CRDT type is returned.

  ## Session Guarantees
  The `version_token` parameter provides session guarantees. If a `version_token`
  from a previous `retrieve_data_items/2` or `modify_data_items/2` call is provided,
  this read operation is guaranteed to observe a state that is not older than the
  state after the previous call. If `:ignore` is provided, no session guarantee is enforced.

  ## Parameters
    - `data_items`: A list of `data_key()` tuples representing the objects to retrieve.
    - `version_token`: A `version_token()` (either a map representing a vector clock or `:ignore`).

  ## Returns
    - `{:ok, results, new_version_token}`: On success, where `results` is a list of
      `{data_key(), item_value()}` tuples, and `new_version_token` is the updated
      version token reflecting the state after this read.
    - `{:error, reason}`: On failure, with a descriptive `reason`.
  """
  @spec retrieve_data_items([data_key()], version_token()) ::
          {:ok, [{data_key(), item_value()}], version_token()} | {:error, any()}
  def retrieve_data_items(data_items, version_token) do
    Logger.notice("#{node()}: retrieve_data_items(#{inspect(data_items)}, #{inspect(version_token)})")

    # Validate input
    case validate_data_keys(data_items) do
      :ok ->
        # Forward the call to the named GenServer
        GenServer.call(MinidoteServer, {:retrieve_data_items, data_items, version_token})

      {:error, reason} ->
        Logger.warning("#{node()}: Invalid data keys in retrieve_data_items: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Modifies one or more CRDT objects atomically in the database.

  This function corresponds to the `update_objects` API call described in the project
  specification. It takes a list of `modifications` and an optional `version_token`
  for session guarantees.

  All modifications provided in a single call are applied atomically. If multiple
  modifications target the same `data_key`, they are performed sequentially from
  left to right as they appear in the `modifications` list.

  ## Session Guarantees
  The `version_token` parameter provides session guarantees. If a `version_token`
  from a previous `retrieve_data_items/2` or `modify_data_items/2` call is provided,
  this update operation is guaranteed to be applied on a state that is not older
  than the state after the previous call. If `:ignore` is provided, no session
  guarantee is enforced.

  ## Parameters
    - `modifications`: A list of tuples, where each tuple is
      `{data_key(), item_operation(), operation_args()}`.
      - `data_key`: The key of the CRDT to modify.
      - `item_operation`: An atom representing the operation to perform (e.g., `:increment`, `:add`).
      - `operation_args`: Arguments for the operation.
    - `version_token`: A `version_token()` (either a map representing a vector clock or `:ignore`).

  ## Returns
    - `{:ok, new_version_token}`: On success, where `new_version_token` is the updated
      version token reflecting the state after this modification.
    - `{:error, reason}`: On failure, with a descriptive `reason`.
  """
  @spec modify_data_items([{data_key(), item_operation(), operation_args()}], version_token()) ::
          {:ok, version_token()} | {:error, any()}
  def modify_data_items(modifications, version_token) do
    Logger.notice("#{node()}: modify_data_items(#{inspect(modifications)}, #{inspect(version_token)})")

    # Validate input
    case validate_item_modifications(modifications) do
      :ok ->
        # Forward the call to the named GenServer
        GenServer.call(MinidoteServer, {:modify_data_items, modifications, version_token})

      {:error, reason} ->
        Logger.warning("#{node()}: Invalid modifications in modify_data_items: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Validates a list of data keys.

  Ensures that all elements in the provided list are valid `data_key()` tuples.
  """
  @spec validate_data_keys([data_key()]) :: :ok | {:error, :invalid_data_key_format | :data_keys_not_list}
  defp validate_data_keys(data_items) when is_list(data_items) do
    case Enum.all?(data_items, &is_valid_data_key?/1) do
      true -> :ok
      false -> {:error, :invalid_data_key_format}
    end
  end

  defp validate_data_keys(_), do: {:error, :data_keys_not_list}

  @doc """
  Validates a list of item modifications.

  Ensures that all elements in the provided list are valid modification tuples
  (`{data_key, item_operation, operation_args}`).
  """
  @spec validate_item_modifications([
          {data_key(), item_operation(), operation_args()}
        ]) :: :ok | {:error, :invalid_modification_format | :modifications_not_list}
  defp validate_item_modifications(modifications) when is_list(modifications) do
    case Enum.all?(modifications, &is_valid_modification?/1) do
      true -> :ok
      false -> {:error, :invalid_modification_format}
    end
  end

  defp validate_item_modifications(_), do: {:error, :modifications_not_list}

  @doc """
  Checks if a single `data_key` tuple is valid.

  A `data_key` is considered valid if it's a 3-tuple `{key, type, bucket}` where:
  - `key` is a binary.
  - `type` is an atom that maps to a supported CRDT module.
  - `bucket` is a binary.
  """
  @spec is_valid_data_key?(data_key()) :: boolean()
  defp is_valid_data_key?({key, type, bucket})
       when is_binary(key) and is_atom(type) and is_binary(bucket) do
    # Convert atom type to module and check if it's valid
    case type_atom_to_crdt_impl(type) do
      {:ok, module} -> ConflictFreeReplicatedDataType.is_supported?(module)
      :error -> false
    end
  end

  defp is_valid_data_key?(_), do: false

  @doc """
  Checks if a single modification tuple is valid.

  A modification tuple is considered valid if it's of the form
  `{data_key, operation, args}` where:
  - `data_key` is a valid `data_key()` tuple.
  - `operation` is an atom.
  """
  @spec is_valid_modification?({data_key(), item_operation(), operation_args()}) :: boolean()
  defp is_valid_modification?({{key, type, bucket}, operation, _args})
       when is_binary(key) and is_atom(type) and is_binary(bucket) and is_atom(operation) do
    # Convert atom type to module and check if it's valid
    case type_atom_to_crdt_impl(type) do
      {:ok, module} -> ConflictFreeReplicatedDataType.is_supported?(module)
      :error -> false
    end
  end

  defp is_valid_modification?(_), do: false

  @doc """
  Maps a CRDT type atom to its corresponding CRDT module.

  This function centralizes the mapping from the atom representation of a CRDT type
  (used in `data_key` tuples) to the actual module that implements the
  `ConflictFreeReplicatedDataType` behaviour.

  Add new mappings here as new CRDTs are implemented.
  """
  @spec type_atom_to_crdt_impl(atom()) :: {:ok, ConflictFreeReplicatedDataType.crdt_module()} | :error
  defp type_atom_to_crdt_impl(:counter_pn_ob), do: {:ok, PositiveNegativeCounter}
  # Fixed naming
  defp type_atom_to_crdt_impl(:set_aw_ob), do: {:ok, Set_AW_OB}
  # Support both variants
  defp type_atom_to_crdt_impl(:set_aw_op), do: {:ok, Set_AW_OB}
  defp type_atom_to_crdt_impl(:g_counter), do: {:ok, GCounter}
  defp type_atom_to_crdt_impl(:or_set), do: {:ok, ORSet}
  defp type_atom_to_crdt_impl(:lww_register), do: {:ok, LWWRegister}
  defp type_atom_to_crdt_impl(:lww_e_set), do: {:ok, LWWEWSet}
  # Add more mappings as you implement more CRDTs
  # defp type_atom_to_crdt_impl(:counter_pn_sb), do: {:ok, Counter_PN_SB}
  # defp type_atom_to_crdt_impl(:mvregister_sb), do: {:ok, MVRegister_SB}
  defp type_atom_to_crdt_impl(_), do: :error

  @doc """
  Retrieves the actual CRDT module from its atom representation.

  This is a public helper function that wraps `type_atom_to_crdt_impl/1` and
  raises an `ArgumentError` if an unknown CRDT type atom is provided.

  ## Parameters
    - `type_atom`: An atom representing the CRDT type (e.g., `:counter_pn_ob`).

  ## Returns
    - The CRDT module (`ConflictFreeReplicatedDataType.crdt_module()`).

  ## Raises
    - `ArgumentError`: If `type_atom` does not map to a known CRDT module.
  """
  @spec get_crdt_implementation(atom()) :: ConflictFreeReplicatedDataType.crdt_module()
  def get_crdt_implementation(type_atom) do
    case type_atom_to_crdt_impl(type_atom) do
      {:ok, module} -> module
      :error -> raise ArgumentError, "Unknown CRDT type: #{inspect(type_atom)}"
    end
  end

  @doc """
  Converts a CRDT module to its atom representation.

  This helper function extracts the last part of the module's name (e.g., `PositiveNegativeCounter`
  becomes `:positivenegativecounter`) and converts it to a lowercase atom. This is primarily
  used for internal mapping and representation.

  ## Parameters
    - `module`: The CRDT module (e.g., `PositiveNegativeCounter`).

  ## Returns
    - The atom representation of the CRDT module (e.g., `:positivenegativecounter`).
  """
  @spec get_crdt_atom(ConflictFreeReplicatedDataType.crdt_module()) :: atom()
  def get_crdt_atom(module) do
    Atom.to_string(module)
    |> String.split(".")
    |> List.last()
    |> String.to_atom()
    |> String.downcase()
    |> String.to_atom()
  end
end
