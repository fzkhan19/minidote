defmodule DistributedDataStore.Service do
  use GenServer
  require Logger
  require ConflictFreeReplicatedDataType

  @moduledoc """
  The API documentation for `DistributedDataStore.Service`.
  A distributed key-value store that works with CausalBroadcast and CRDTs.
  """

  # Public API
  def start_link(service_name) do
    GenServer.start_link(__MODULE__, [], name: service_name)
  end

  def retrieve(service, key) do
    GenServer.call(service, {:retrieve, key})
  end

  def store(service, key, value) do
    GenServer.call(service, {:store, key, value})
  end

  # GenServer Callbacks
  @impl true
  def init(_) do
    # Start the causal broadcast layer
    {:ok, bcast_pid} = CausalBroadcast.start_link(owner: self())

    initial_state = %{
      # Simple key-value store for CRDT objects
      db: %{},
      # Reference to broadcast layer
      bcast: bcast_pid,
      # Vector clock for causality
      clock: Vector_Clock.new(),
      # Queue for requests waiting for session guarantees
      waiting_requests: []
    }

    Logger.info("Distributed data store service initiated")
    {:ok, initial_state}
  end

  # Handle CRDT read_objects requests
  @impl true
  def handle_call({:retrieve_data_items, objects, client_clock}, from, state) do
    # Check session guarantees
    if client_clock == :ignore or Vector_Clock.leq(client_clock, state.clock) do
      # Session guarantee satisfied, process immediately
      results =
        Enum.map(objects, fn {_key, crdt_type_atom, _bucket} = full_key ->
          # Convert atom to actual CRDT module
          crdt_type = DistributedDataStore.get_crdt_implementation(crdt_type_atom)

          case Map.get(state.db, full_key) do
            nil ->
              # Object doesn't exist, create empty CRDT
              empty_crdt = ConflictFreeReplicatedDataType.create_new(crdt_type)
              value = ConflictFreeReplicatedDataType.get_current_value(crdt_type, empty_crdt)
              {full_key, value}

            crdt_state ->
              # Object exists, get its value
              value = ConflictFreeReplicatedDataType.get_current_value(crdt_type, crdt_state)
              {full_key, value}
          end
        end)

      {:reply, {:ok, results, state.clock}, state}
    else
      # Session guarantee not met, queue the request
      new_waiting = [{:retrieve_data_items, objects, client_clock, from} | state.waiting_requests]
      {:noreply, %{state | waiting_requests: new_waiting}}
    end
  end

  # Handle CRDT update_objects requests
  @impl true
  def handle_call({:modify_data_items, updates, client_clock}, from, state) do
    # Check session guarantees
    if client_clock == :ignore or Vector_Clock.leq(client_clock, state.clock) do
      # Session guarantee satisfied, process immediately
      process_item_modifications(updates, state, from)
    else
      # Session guarantee not met, queue the request
      new_waiting = [{:modify_data_items, updates, client_clock, from} | state.waiting_requests]
      {:noreply, %{state | waiting_requests: new_waiting}}
    end
  end

  # Handle read requests (original simple API)
  @impl true
  def handle_call({:retrieve, key}, _from, state) do
    value = Map.get(state.db, key, :not_found)
    Logger.debug("Retrieved key=#{key}, value=#{inspect(value)}")
    {:reply, {:ok, value}, state}
  end

  # Handle store requests (original simple API)
  @impl true
  def handle_call({:store, key, value}, _from, state) do
    # Update local database
    new_db = Map.put(state.db, key, value)
    new_state = %{state | db: new_db}

    # Broadcast the change to other nodes
    CausalBroadcast.broadcast({:store, key, value})

    Logger.debug("Stored key=#{key}, value=#{inspect(value)}")
    {:reply, :ok, new_state}
  end

  # Catch-all for other calls
  @impl true
  def handle_call(msg, _from, state) do
    Logger.warning("Unhandled call: #{inspect(msg)}")
    {:reply, {:error, :not_implemented}, state}
  end

  # Handle CRDT updates delivered from other nodes
  @impl true
  def handle_info(
        {:deliver,
         {:crdt_update, full_key, _crdt_type_atom, crdt_type, effect, sender_clock, sender_node}},
        state
      ) do
    # Get current CRDT state or create new one
    current_crdt = Map.get(state.db, full_key, ConflictFreeReplicatedDataType.create_new(crdt_type))

    # Apply the remote effect
    case ConflictFreeReplicatedDataType.apply_propagation_effect(crdt_type, {effect, sender_node}, current_crdt) do
      {:ok, new_crdt} ->
        # Update database and merge clocks
        new_db = Map.put(state.db, full_key, new_crdt)
        new_clock = Vector_Clock.merge(state.clock, sender_clock)
        new_state = %{state | db: new_db, clock: new_clock}

        Logger.debug("Applied remote CRDT update: key=#{inspect(full_key)}")

        # Check waiting requests after clock update
        final_state = check_waiting_requests(new_state)
        {:noreply, final_state}

      {:error, reason} ->
        Logger.error("Failed to apply remote CRDT update: #{inspect(reason)}")
        {:noreply, state}
    end
  end

  # Handle older format for backward compatibility
  @impl true
  def handle_info(
        {:deliver, {:crdt_update, full_key, crdt_type, effect, sender_clock, sender_node}},
        state
      ) do
    # Assume crdt_type is already the module (backward compatibility)
    current_crdt = Map.get(state.db, full_key, ConflictFreeReplicatedDataType.create_new(crdt_type))

    case ConflictFreeReplicatedDataType.apply_propagation_effect(crdt_type, {effect, sender_node}, current_crdt) do
      {:ok, new_crdt} ->
        new_db = Map.put(state.db, full_key, new_crdt)
        new_clock = Vector_Clock.merge(state.clock, sender_clock)
        new_state = %{state | db: new_db, clock: new_clock}

        Logger.debug("Applied remote CRDT update (old format): key=#{inspect(full_key)}")

        final_state = check_waiting_requests(new_state)
        {:noreply, final_state}

      {:error, reason} ->
        Logger.error("Failed to apply remote CRDT update: #{inspect(reason)}")
        {:noreply, state}
    end
  end

  # Handle writes delivered from other nodes via CausalBroadcast (original simple API)
  @impl true
  def handle_info({:deliver, {:store, key, value}}, state) do
    # Apply the remote store to our database
    new_db = Map.put(state.db, key, value)
    new_state = %{state | db: new_db}

    Logger.debug("Applied remote store: key=#{key}, value=#{inspect(value)}")
    {:noreply, new_state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Unhandled info message: #{inspect(msg)}")
    {:noreply, state}
  end

  # Private helper functions

  # Process update_objects request
  defp process_item_modifications(modifications, state, from) do
    # Increment local clock for this update operation
    new_clock = Vector_Clock.increment(state.clock, node())

    # Process each update atomically
    {new_db, effects} =
      Enum.reduce(modifications, {state.db, []}, fn {{_key, crdt_type_atom, _bucket} = full_key,
                                                       operation, args},
                                                      {db_acc, effects_acc} ->
        # Convert atom to actual CRDT module
        crdt_type = DistributedDataStore.get_crdt_implementation(crdt_type_atom)

        # Get current CRDT state or create new one
        current_crdt = Map.get(db_acc, full_key, ConflictFreeReplicatedDataType.create_new(crdt_type))

        # Create the update operation
        update_op = {operation, args}

        # Apply the operation
        case ConflictFreeReplicatedDataType.compute_propagation_effect(crdt_type, update_op, current_crdt) do
          {:ok, effect} ->
            case ConflictFreeReplicatedDataType.apply_propagation_effect(crdt_type, {effect, node()}, current_crdt) do
              {:ok, new_crdt} ->
                # Update database locally
                new_db = Map.put(db_acc, full_key, new_crdt)
                # Store effect for broadcasting (include both atom and module for compatibility)
                new_effects = [{full_key, crdt_type_atom, crdt_type, effect} | effects_acc]
                {new_db, new_effects}

              {:error, reason} ->
                Logger.warning("Failed to apply CRDT update locally: #{inspect(reason)}")
                {db_acc, effects_acc}
            end

          {:error, reason} ->
            Logger.warning("Failed to create downstream effect: #{inspect(reason)}")
            {db_acc, effects_acc}
        end
      end)

    # Broadcast all effects
    for {full_key, crdt_type_atom, crdt_type, effect} <- effects do
      CausalBroadcast.broadcast(
        {:crdt_update, full_key, crdt_type_atom, crdt_type, effect, new_clock, node()}
      )
    end

    new_state = %{state | db: new_db, clock: new_clock}

    # Check waiting requests after clock update
    final_state = check_waiting_requests(new_state)

    # Reply to the caller
    GenServer.reply(from, {:ok, new_clock})
    {:noreply, final_state}
  end

  # Check if any waiting requests can now be processed
  defp check_waiting_requests(state) do
    {ready_requests, still_waiting} =
      Enum.split_with(state.waiting_requests, fn
        {_op, _data, client_clock, _from} ->
          client_clock == :ignore or Vector_Clock.leq(client_clock, state.clock)
      end)

    # Process ready requests
    final_state =
      Enum.reduce(ready_requests, %{state | waiting_requests: still_waiting}, fn request,
                                                                                 acc_state ->
        case request do
          {:retrieve_data_items, objects, _client_clock, from} ->
            results =
              Enum.map(objects, fn {_key, crdt_type_atom, _bucket} = full_key ->
                crdt_type = DistributedDataStore.get_crdt_implementation(crdt_type_atom)

                case Map.get(acc_state.db, full_key) do
                  nil ->
                    empty_crdt = ConflictFreeReplicatedDataType.create_new(crdt_type)
                    value = ConflictFreeReplicatedDataType.get_current_value(crdt_type, empty_crdt)
                    {full_key, value}

                  crdt_state ->
                    value = ConflictFreeReplicatedDataType.get_current_value(crdt_type, crdt_state)
                    {full_key, value}
                end
              end)

            GenServer.reply(from, {:ok, results, acc_state.clock})
            acc_state

          {:modify_data_items, updates, _client_clock, from} ->
            # Process the modification (this will handle the reply internally)
            {:noreply, new_state} = process_item_modifications(updates, acc_state, from)
            new_state
        end
      end)

    final_state
  end
end

# Simple example
defmodule DistributedDataStore.SampleUsage do
  def demonstration do
    # Start service
    {:ok, _process_id} = DistributedDataStore.Service.start_link(:my_service)

    # Store some information
    :ok = DistributedDataStore.Service.store(:my_service, "entity_name", "Bob")

    # Retrieve it
    {:ok, retrieved_value} = DistributedDataStore.Service.retrieve(:my_service, "entity_name")
    IO.puts("Retrieved: #{retrieved_value}")
  end

  def crdt_sample do
    # Example using the CRDT API directly through the service
    # This assumes the DistributedDataStore.Service is registered as DistributedDataStore.Service
    version_token = Vector_Clock.new()

    # Define a set key - use the atom representation
    set_identifier = {"my_unique_set", :set_aw_op, "my_specific_domain"}

    # Perform modifications using GenServer.call directly
    case GenServer.call(
           DistributedDataStore.Service,
           {:modify_data_items,
            [
              {set_identifier, :add_element, {"component_A", :marker1}}
            ], version_token}
         ) do
      {:ok, updated_token} ->
        # Retrieve the set
        case GenServer.call(DistributedDataStore.Service, {:retrieve_data_items, [set_identifier], updated_token}) do
          {:ok, results_data, _retrieval_token} ->
            IO.inspect(results_data, label: "Set contents")
            :ok

          {:error, issue} ->
            IO.puts("Failed to retrieve data items: #{inspect(issue)}")
            {:error, issue}
        end

      {:error, issue} ->
        IO.puts("Failed to modify data items: #{inspect(issue)}")
        {:error, issue}
    end
  end
end
