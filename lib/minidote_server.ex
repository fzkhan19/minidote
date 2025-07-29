defmodule Minidote.Server do
  use GenServer
  require Logger

  @moduledoc """
  The API documentation for `Minidote.Server`.
  A simple key-value store that works with CausalBroadcast and CRDTs.
  """

  # Public API
  def start_link(server_name) do
    GenServer.start_link(__MODULE__, [], name: server_name)
  end

  def read(server, key) do
    GenServer.call(server, {:read, key})
  end

  def write(server, key, value) do
    GenServer.call(server, {:write, key, value})
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

    Logger.info("Minidote server started")
    {:ok, initial_state}
  end

  # Handle CRDT read_objects requests
  @impl true
  def handle_call({:read_objects, objects, client_clock}, from, state) do
    # Check session guarantees
    if client_clock == :ignore or Vector_Clock.leq(client_clock, state.clock) do
      # Session guarantee satisfied, process immediately
      results =
        Enum.map(objects, fn {_key, crdt_type_atom, _bucket} = full_key ->
          # Convert atom to actual CRDT module
          crdt_type = Minidote.get_crdt_module(crdt_type_atom)

          case Map.get(state.db, full_key) do
            nil ->
              # Object doesn't exist, create empty CRDT
              empty_crdt = CRDT.new(crdt_type)
              value = CRDT.value(crdt_type, empty_crdt)
              {full_key, value}

            crdt_state ->
              # Object exists, get its value
              value = CRDT.value(crdt_type, crdt_state)
              {full_key, value}
          end
        end)

      {:reply, {:ok, results, state.clock}, state}
    else
      # Session guarantee not met, queue the request
      new_waiting = [{:read_objects, objects, client_clock, from} | state.waiting_requests]
      {:noreply, %{state | waiting_requests: new_waiting}}
    end
  end

  # Handle CRDT update_objects requests
  @impl true
  def handle_call({:update_objects, updates, client_clock}, from, state) do
    # Check session guarantees
    if client_clock == :ignore or Vector_Clock.leq(client_clock, state.clock) do
      # Session guarantee satisfied, process immediately
      process_update_objects(updates, state, from)
    else
      # Session guarantee not met, queue the request
      new_waiting = [{:update_objects, updates, client_clock, from} | state.waiting_requests]
      {:noreply, %{state | waiting_requests: new_waiting}}
    end
  end

  # Handle read requests (original simple API)
  @impl true
  def handle_call({:read, key}, _from, state) do
    value = Map.get(state.db, key, :not_found)
    Logger.debug("Read key=#{key}, value=#{inspect(value)}")
    {:reply, {:ok, value}, state}
  end

  # Handle write requests (original simple API)
  @impl true
  def handle_call({:write, key, value}, _from, state) do
    # Update local database
    new_db = Map.put(state.db, key, value)
    new_state = %{state | db: new_db}

    # Broadcast the change to other nodes
    CausalBroadcast.broadcast({:write, key, value})

    Logger.debug("Write key=#{key}, value=#{inspect(value)}")
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
    current_crdt = Map.get(state.db, full_key, CRDT.new(crdt_type))

    # Apply the remote effect
    case CRDT.update(crdt_type, {effect, sender_node}, current_crdt) do
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
    current_crdt = Map.get(state.db, full_key, CRDT.new(crdt_type))

    case CRDT.update(crdt_type, {effect, sender_node}, current_crdt) do
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
  def handle_info({:deliver, {:write, key, value}}, state) do
    # Apply the remote write to our database
    new_db = Map.put(state.db, key, value)
    new_state = %{state | db: new_db}

    Logger.debug("Applied remote write: key=#{key}, value=#{inspect(value)}")
    {:noreply, new_state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Unhandled info message: #{inspect(msg)}")
    {:noreply, state}
  end

  # Private helper functions

  # Process update_objects request
  defp process_update_objects(updates, state, from) do
    # Increment local clock for this update operation
    new_clock = Vector_Clock.increment(state.clock, node())

    # Process each update atomically
    {new_db, effects} =
      Enum.reduce(updates, {state.db, []}, fn {{_key, crdt_type_atom, _bucket} = full_key,
                                               operation, args},
                                              {db_acc, effects_acc} ->
        # Convert atom to actual CRDT module
        crdt_type = Minidote.get_crdt_module(crdt_type_atom)

        # Get current CRDT state or create new one
        current_crdt = Map.get(db_acc, full_key, CRDT.new(crdt_type))

        # Create the update operation
        update_op = {operation, args}

        # Apply the operation
        case CRDT.downstream(crdt_type, update_op, current_crdt) do
          {:ok, effect} ->
            case CRDT.update(crdt_type, {effect, node()}, current_crdt) do
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
          {:read_objects, objects, _client_clock, from} ->
            results =
              Enum.map(objects, fn {_key, crdt_type_atom, _bucket} = full_key ->
                crdt_type = Minidote.get_crdt_module(crdt_type_atom)

                case Map.get(acc_state.db, full_key) do
                  nil ->
                    empty_crdt = CRDT.new(crdt_type)
                    value = CRDT.value(crdt_type, empty_crdt)
                    {full_key, value}

                  crdt_state ->
                    value = CRDT.value(crdt_type, crdt_state)
                    {full_key, value}
                end
              end)

            GenServer.reply(from, {:ok, results, acc_state.clock})
            acc_state

          {:update_objects, updates, _client_clock, from} ->
            # Process the update (this will handle the reply internally)
            {:noreply, new_state} = process_update_objects(updates, acc_state, from)
            new_state
        end
      end)

    final_state
  end
end

# Simple example
defmodule Minidote.Example do
  def demo do
    # Start server
    {:ok, _pid} = Minidote.Server.start_link(:my_server)

    # Write some data
    :ok = Minidote.Server.write(:my_server, "name", "Alice")

    # Read it back
    {:ok, value} = Minidote.Server.read(:my_server, "name")
    IO.puts("Read: #{value}")
  end

  def crdt_demo do
    # Example using the CRDT API directly through the server
    # This assumes the Minidote.Server is registered as Minidote.Server
    clock = Vector_Clock.new()

    # Create a set key - use the atom representation
    set_key = {"my_set", :set_aw_op, "my_bucket"}

    # Add some elements using GenServer.call directly
    case GenServer.call(
           Minidote.Server,
           {:update_objects,
            [
              {set_key, :add, {"element1", :tag1}}
            ], clock}
         ) do
      {:ok, new_clock} ->
        # Read the set
        case GenServer.call(Minidote.Server, {:read_objects, [set_key], new_clock}) do
          {:ok, results, _read_clock} ->
            IO.inspect(results, label: "Set contents")
            :ok

          {:error, reason} ->
            IO.puts("Failed to read objects: #{inspect(reason)}")
            {:error, reason}
        end

      {:error, reason} ->
        IO.puts("Failed to update objects: #{inspect(reason)}")
        {:error, reason}
    end
  end
end
