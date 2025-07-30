defmodule CausalBroadcast do
  use GenServer

  @moduledoc """
  Implements a causal broadcast mechanism using vector clocks and a link layer for communication.

  This module ensures that messages are delivered in causal order, meaning that if message A
  happened before message B, then A will be delivered before B at all nodes.
  """

  # -- Public API --

  @doc """
  Starts the CausalBroadcast GenServer.

  `name` - The registered name for the GenServer.
  `opts` - Options for the GenServer, including:
    - `:owner` (required): The PID of the process that will receive delivered messages.
    - `:link_group_name` (optional): The name of the link group used by the LinkLayer.
  """
  @spec start_link(atom(), Keyword.t()) :: {:ok, pid()} | {:error, any()}
  def start_link(name, opts) do
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Broadcasts a message to all other nodes in the cluster.

  The message is tagged with the sender's current vector clock and node ID to ensure
  causal ordering. The broadcast is asynchronous and uses the underlying LinkLayer
  for message transmission.

  ## Parameters
    - `name`: The registered name of the CausalBroadcast GenServer.
    - `message`: The message to broadcast. Can be any Elixir term.
  """
  @spec broadcast(atom(), any()) :: :ok
  def broadcast(name, message) do
    GenServer.cast(name, {:broadcast, message})
  end

  # -- GenServer Callbacks --

  @impl true
  @doc """
  Initializes the CausalBroadcast GenServer.

  Starts the LinkLayer, registers the CausalBroadcast process with the LinkLayer,
  and initializes the state with the LinkLayer PID, a new VectorClock, an empty buffer,
  the owner PID, and the GenServer name.
  """
  @spec init(Keyword.t()) :: {:ok, map()}
  def init(opts) do
    # Get the owner PID from options (the process that will receive delivered messages)
    owner_pid = Keyword.get(opts, :owner, self())
    name = Keyword.get(opts, :name) # Retrieve the name passed to start_link, so the LinkLayer knows who sent the message
    group_name = Keyword.get(opts, :link_group_name, String.to_atom("#{Atom.to_string(name)}_link_layer")) # Use passed group_name or default, for the LinkLayer to identify other nodes

    # Start the link layer to connect to other nodes using the group_name
    {:ok, link_layer_pid} = LinkLayer.start_link(group_name)

    # Register self to receive messages from the link layer, so we can receive messages from other nodes
    LinkLayer.register(link_layer_pid, self())

    # Initialize state
    initial_state = %{
      link_layer: link_layer_pid,
      vector_clock: Vector_Clock.new(),
      buffer: [],
      owner: owner_pid,
      name: name # Store the name in the state
    }

    {:ok, initial_state}
  end

  @impl true
  @doc """
  Handles the `:broadcast` cast message.

  Increments the local vector clock for the current node, tags the message with
  this new clock and the sender's node ID, and then sends the tagged message
  to all other nodes in the cluster via the LinkLayer.

  ## Parameters
    - `{:broadcast, message}`: The cast message containing the payload to broadcast.
    - `state`: The current state of the GenServer.

  ## Returns
    - `{:noreply, new_state}`: The updated state with the incremented vector clock.
  """
  @spec handle_cast({:broadcast, any()}, map()) :: {:noreply, map()}
  def handle_cast({:broadcast, message}, state) do
    # Get this node's name (Elixir node name)
    this_node = node()

    # Increment our clock
    new_clock = Vector_Clock.increment(state.vector_clock, this_node)

    # Create tagged message with explicit sender
    tagged_message = {message, new_clock, this_node}

    # Send to all other nodes using the LinkLayer - handle the {:ok, nodes} return value
    case LinkLayer.other_nodes(state.link_layer) do
      {:ok, other_nodes} ->
        for node_pid <- other_nodes do
          # Pass the broadcast module's name to LinkLayer.send so the LinkLayer knows who sent the message
          LinkLayer.send(state.link_layer, tagged_message, node_pid, state.name)
        end

      {:error, _reason} ->
        # Log error but continue
        require Logger
        Logger.warning("Failed to get other nodes for broadcast")
    end

    # Update state
    {:noreply, %{state | vector_clock: new_clock}}
  end

  @impl true
  @doc """
  Handles incoming messages from the LinkLayer that include sender clock and node.

  If the message is causally ready (as determined by `message_is_ready?/3`), it is
  delivered to the owner process, the local vector clock is merged with the sender's
  clock, and the buffer is processed to see if any buffered messages are now ready.

  If the message is not causally ready, it is added to the buffer for later processing.

  ## Parameters
    - `{:remote, {message, sender_clock, sender_node}}`: The incoming message from the LinkLayer.
      - `message`: The actual payload of the broadcast message.
      - `sender_clock`: The vector clock of the sender at the time of broadcast.
      - `sender_node`: The node from which the message originated.
    - `state`: The current state of the GenServer.

  ## Returns
    - `{:noreply, new_state}`: The updated state of the GenServer.
  """
  @spec handle_info({:remote, {any(), map(), node()}}, map()) :: {:noreply, map()}
  def handle_info({:remote, {message, sender_clock, sender_node}}, state) do
    if message_is_ready?(sender_clock, sender_node, state.vector_clock) do
      # Message is causally ready - deliver it to the owner
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

  @impl true
  @doc """
  Handles incoming messages from the LinkLayer (fallback for old 2-tuple format).

  This function is for backward compatibility with older versions of the system. It attempts
  to infer the sender node from the clock. If successful and the message is causally ready,
  it delivers the message and merges the clocks. Otherwise, it buffers the message.

  ## Parameters
    - `{:remote, {message, sender_clock}}`: The incoming message from the LinkLayer.
      - `message`: The actual payload of the broadcast message.
      - `sender_clock`: The vector clock of the sender at the time of broadcast.
    - `state`: The current state of the GenServer.

  ## Returns
    - `{:noreply, new_state}`: The updated state of the GenServer.
  """
  @spec handle_info({:remote, {any(), map()}}, map()) :: {:noreply, map()}
  def handle_info({:remote, {message, sender_clock}}, state) do
    # Try to infer sender from clock, since older versions didn't send the sender node
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

  # Checks if a message is causally ready to be delivered.
  #
  # A message from sender S with clock C is causally ready if:
  # 1.  C[S] = local_clock[S] + 1 (The sender's clock value for the sender node is one greater than the local clock's value for the sender node.)
  # 2.  For all other nodes N: C[N] <= local_clock[N] (The sender's clock value for all other nodes is less than or equal to the local clock's value for those nodes.)
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

  # Inferred the sender node from the clock differences (fallback method).
  #
  # This method is used when the sender node is not explicitly included in the message
  # (for backward compatibility). It finds the node whose clock value in the sender's
  # clock is exactly one greater than its value in the local clock.
  defp infer_sender_from_clock(sender_clock, local_clock) do
    # Find the node whose clock value is exactly local + 1
    Enum.find_value(sender_clock, fn {node, value} ->
      local_value = Vector_Clock.get(local_clock, node)
      if value == local_value + 1, do: node, else: nil
    end) || :unknown
  end

  # Processes the message buffer to see if any buffered messages are now ready for delivery.
  #
  # This function iterates through the buffer and checks if each message is causally ready.
  # Ready messages are delivered, and the vector clock is updated. The function then
  # recursively calls itself to process the remaining buffer in case delivering the
  # ready messages has made other messages ready as well.
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

  # Delivers a list of buffered messages and updates the vector clock accordingly.
  #
  # This function iterates through the list of messages, delivers each message to the
  # owner process, and merges the sender's vector clock into the local vector clock.
  defp deliver_buffered_messages(messages, state) do
    Enum.reduce(messages, state, fn
      {message, sender_clock, _sender}, acc_state ->
        # Deliver message to the owner process
        send(acc_state.owner, {:deliver, message})
        # Merge sender's clock into the local clock
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
