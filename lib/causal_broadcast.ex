defmodule CausalBroadcast do
  use GenServer

  # -- Public API --

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def broadcast(message) do
    GenServer.cast(__MODULE__, {:broadcast, message})
  end

  # -- GenServer Callbacks --

  @impl true
  def init(opts) do
    # Get the owner PID from options
    owner_pid = Keyword.get(opts, :owner, self())

    # Start the link layer to connect to other nodes
    {:ok, link_layer_pid} = LinkLayer.start_link(:distributed_data_store)

    # Register self to receive messages from the link layer
    LinkLayer.register(link_layer_pid, self())

    # Initialize state
    initial_state = %{
      link_layer: link_layer_pid,
      vector_clock: Vector_Clock.new(),
      buffer: [],
      owner: owner_pid
    }

    {:ok, initial_state}
  end

  # Handle broadcast requests
  @impl true
  def handle_cast({:broadcast, message}, state) do
    # Get this node's name
    this_node = node()

    # Increment our clock
    new_clock = Vector_Clock.increment(state.vector_clock, this_node)

    # Create tagged message with explicit sender
    tagged_message = {message, new_clock, this_node}

    # Send to all other nodes - handle the {:ok, nodes} return value
    case LinkLayer.other_nodes(state.link_layer) do
      {:ok, other_nodes} ->
        for node_pid <- other_nodes do
          LinkLayer.send(state.link_layer, tagged_message, node_pid)
        end

      {:error, _reason} ->
        # Log error but continue
        require Logger
        Logger.warning("Failed to get other nodes for broadcast")
    end



    # Update state
    {:noreply, %{state | vector_clock: new_clock}}
  end

  # Handle remote messages
  @impl true
  def handle_info({:remote, {message, sender_clock, sender_node}}, state) do
    if message_is_ready?(sender_clock, sender_node, state.vector_clock) do
      # Message is ready - deliver it
      send(state.owner, {:deliver, message})

      # Merge clocks
      merged_clock = Vector_Clock.merge(state.vector_clock, sender_clock)
      new_state = %{state | vector_clock: merged_clock}

      # Check if any buffered messages are now ready
      process_buffer(new_state)
    else
      # Not ready - add to buffer
      new_buffer = [{message, sender_clock, sender_node} | state.buffer]
      {:noreply, %{state | buffer: new_buffer}}
    end
  end

  # Fallback for old 2-tuple format (backward compatibility)
  @impl true
  def handle_info({:remote, {message, sender_clock}}, state) do
    # Try to infer sender from clock
    sender_node = infer_sender_from_clock(sender_clock, state.vector_clock)

    if sender_node != :unknown and
         message_is_ready?(sender_clock, sender_node, state.vector_clock) do
      send(state.owner, {:deliver, message})
      merged_clock = Vector_Clock.merge(state.vector_clock, sender_clock)
      new_state = %{state | vector_clock: merged_clock}
      process_buffer(new_state)
    else
      new_buffer = [{message, sender_clock, sender_node} | state.buffer]
      {:noreply, %{state | buffer: new_buffer}}
    end
  end

  # -- Helper Functions --

  # Check if a message is causally ready to be delivered
  defp message_is_ready?(sender_clock, sender_node, local_clock) do
    # For causal ordering, a message from sender S with clock C is ready if:
    # 1. For the sender node: C[S] = local_clock[S] + 1
    # 2. For all other nodes N: C[N] <= local_clock[N]

    # Check condition 1: sender's clock should be exactly local + 1
    sender_value = Vector_Clock.get(sender_clock, sender_node)
    local_sender_value = Vector_Clock.get(local_clock, sender_node)

    if sender_value != local_sender_value + 1 do
      false
    else
      # Check condition 2: all other nodes should be <= local
      Enum.all?(sender_clock, fn {node, value} ->
        if node == sender_node do
          # Already checked above
          true
        else
          local_value = Vector_Clock.get(local_clock, node)
          value <= local_value
        end
      end)
    end
  end

  # Fallback method to infer sender from clock differences
  defp infer_sender_from_clock(sender_clock, local_clock) do
    # Find the node whose clock value is exactly local + 1
    Enum.find_value(sender_clock, fn {node, value} ->
      local_value = Vector_Clock.get(local_clock, node)
      if value == local_value + 1, do: node, else: nil
    end) || :unknown
  end

  # Process buffered messages to see if any are now ready
  defp process_buffer(state) do
    {ready_messages, remaining_buffer} =
      Enum.split_with(state.buffer, fn
        {_msg, clock, sender} ->
          message_is_ready?(clock, sender, state.vector_clock)

        {_msg, clock} ->
          sender = infer_sender_from_clock(clock, state.vector_clock)
          sender != :unknown and message_is_ready?(clock, sender, state.vector_clock)
      end)

    if ready_messages == [] do
      # No messages ready
      {:noreply, %{state | buffer: remaining_buffer}}
    else
      # Deliver ready messages and update clock
      final_state = deliver_buffered_messages(ready_messages, state)
      final_state = %{final_state | buffer: remaining_buffer}

      # Recursively check buffer again in case more messages are now ready
      process_buffer(final_state)
    end
  end

  # Deliver buffered messages and update clock
  defp deliver_buffered_messages(messages, state) do
    Enum.reduce(messages, state, fn
      {message, sender_clock, _sender}, acc_state ->
        # Deliver message
        send(acc_state.owner, {:deliver, message})
        # Merge clock
        merged_clock = Vector_Clock.merge(acc_state.vector_clock, sender_clock)
        %{acc_state | vector_clock: merged_clock}

      {message, sender_clock}, acc_state ->
        # Handle old format
        send(acc_state.owner, {:deliver, message})
        merged_clock = Vector_Clock.merge(acc_state.vector_clock, sender_clock)
        %{acc_state | vector_clock: merged_clock}
    end)
  end
end
