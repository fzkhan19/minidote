defmodule LinkLayerDistr do
  @moduledoc """
  An implementation of the link layer using distributed Erlang and the `pg` process group library.

  This module provides a distributed link layer for connecting nodes in a cluster. It leverages
  Erlang's distributed capabilities and the `pg` library for process group management. The link layer
  is responsible for discovering other nodes, sending messages to specific nodes, and registering
  processes to receive messages.
  """

  use GenServer
  require Logger

  defstruct group_name: nil, respond_to: :none

  # Public API
  @doc """
  Starts the LinkLayerDistr GenServer.

  ## Parameters
    - `group_name`: The name of the process group to join. This is used to identify other nodes
                    in the cluster.

  ## Returns
    - `{:ok, pid}`: If the GenServer starts successfully.
    - `{:error, reason}`: If the GenServer fails to start.
  """
  @spec start_link(atom()) :: {:ok, pid()} | {:error, any()}
  def start_link(group_name) when is_atom(group_name) do
    GenServer.start_link(__MODULE__, [group_name], [])
  end

  @doc """
  Stops the LinkLayerDistr GenServer.

  ## Parameters
    - `pid`: The PID of the LinkLayerDistr process to stop.

  ## Returns
    - `:ok`: On successful termination.
  """
  @spec stop(pid()) :: :ok
  def stop(pid) do
    GenServer.stop(pid)
  end

  # GenServer Callbacks
  @impl true
  @doc """
  Initializes the LinkLayerDistr GenServer.

  This function is called when the GenServer is started. It performs the following actions:
    - Spawns a linked process to find other Erlang nodes based on `MINIDOTE_NODES` environment variable.
    - Starts the `:pg` process group library.
    - Joins the specified process group.
    - Registers the GenServer process with the group name for easy lookup.

  ## Parameters
    - `[group_name]`: A list containing the name of the process group to join.

  ## Returns
    - `{:ok, state}`: The initial state of the GenServer.
  """
  @spec init([atom()]) :: {:ok, map()}
  def init([group_name]) do
    # Initially, try to connect with other erlang nodes using the environment variable MINIDOTE_NODES
    spawn_link(&find_other_nodes/0)
    # Ensure :pg is started (OTP 23+)
    :pg.start_link()
    :pg.join(group_name, self()) # Join the process group using the given group_name
    Process.register(self(), group_name) # Register the process with the group_name for easy lookup
    {:ok, %__MODULE__{group_name: group_name, respond_to: :none}}
  end

  @impl true
  @doc """
  Terminates the LinkLayerDistr GenServer.

  This function is called when the GenServer is stopped. It leaves the process group.

  ## Parameters
    - `_reason`: The reason for termination (not used).
    - `state`: The current state of the GenServer.

  ## Returns
    - `:ok`: Always `:ok`.
  """
  @spec terminate(any(), map()) :: :ok
  def terminate(_reason, state) do
    :pg.leave(state.group_name, self()) # Leave the process group
  end

  @impl true
  @doc """
  Handles the `:send` call to send data to a specific node.

  This function is called when a process wants to send a message to a specific node.
  It sends the message to the process group associated with the target node.

  ## Parameters
    - `{:send, data, node, broadcast_name}`: The call message.
      - `data`: The actual data to send.
      - `node`: The target node (atom name) to send the message to.
      - `broadcast_name`: The registered name of the `CausalBroadcast` GenServer that initiated the send.
    - `_from`: The PID of the process making the call (not used).
    - `state`: The current state of the GenServer.

  ## Returns
    - `{:reply, :ok, state}`: Always `:ok` on successful message sending.
  """
  @spec handle_call({:send, any(), node(), atom()}, GenServer.from(), map()) :: {:reply, :ok, map()}
  def handle_call({:send, data, node, broadcast_name}, _from, state) do
    GenServer.cast({state.group_name, node}, {:remote, data, broadcast_name}) # Send the message to the target node's process group
    {:reply, :ok, state}
  end

  @impl true
  @doc """
  Handles the `:register` call to register a receiver process.

  This function is called when a process wants to register itself to receive messages
  from the link layer. The PID of the registered process is stored in the GenServer's state.

  ## Parameters
    - `{:register, receiver}`: The call message containing the receiver's PID.
      - `receiver`: The PID of the process (`pid()`) to register.
    - `_from`: The PID of the process making the call (not used).
    - `state`: The current state of the GenServer.

  ## Returns
    - `{:reply, :ok, new_state}`: The updated state with the registered receiver.
  """
  @spec handle_call({:register, pid()}, GenServer.from(), map()) :: {:reply, :ok, map()}
  def handle_call({:register, r}, _from, state) do
    {:reply, :ok, %{state | respond_to: r}} # Update the state to store the PID of the registered process
  end

  @impl true
  @doc """
  Handles the `:all_nodes` call to retrieve all nodes in the process group.

  ## Parameters
    - `:all_nodes`: The call message.
    - `_from`: The PID of the process making the call (not used).
    - `state`: The current state of the GenServer.

  ## Returns
    - `{:reply, {:ok, [node()]}, state}`: A list of all connected node names.
  """
  @spec handle_call(:all_nodes, GenServer.from(), map()) :: {:reply, {:ok, [node()]}, map()}
  def handle_call(:all_nodes, _from, state) do
    members = :pg.get_members(state.group_name) # Get all members of the process group
    nodes = for m <- members, do: node(m) # Extract the node name from each member
    {:reply, {:ok, nodes}, state}
  end

  @impl true
  @doc """
  Handles the `:other_nodes` call to retrieve all nodes in the process group, excluding the current node.

  ## Parameters
    - `:other_nodes`: The call message.
    - `_from`: The PID of the process making the call (not used).
    - `state`: The current state of the GenServer.

  ## Returns
    - `{:reply, {:ok, [node()]}, state}`: A list of other connected node names.
  """
  @spec handle_call(:other_nodes, GenServer.from(), map()) :: {:reply, {:ok, [node()]}, map()}
  def handle_call(:other_nodes, _from, state) do
    members = :pg.get_members(state.group_name) # Get all members of the process group
    self_pid = self() # Get the PID of the current process
    other_members = for m <- members, m != self_pid, do: node(m) # Extract the node name from each member, excluding the current process
    {:reply, {:ok, other_members}, state}
  end

  @impl true
  @doc """
  Handles the `:this_node` call to retrieve the name of the current node.

  ## Parameters
    - `:this_node`: The call message.
    - `_from`: The PID of the process making the call (not used).
    - `state`: The current state of the GenServer.

  ## Returns
    - `{:reply, {:ok, node()}, state}`: The name of the current node.
  """
  @spec handle_call(:this_node, GenServer.from(), map()) :: {:reply, {:ok, node()}, map()}
  def handle_call(:this_node, _from, state) do
    {:reply, {:ok, node()}, state} # Return the current node name
  end

  @impl true
  @doc """
  Handles the `{:remote, msg, broadcast_name}` cast message received from another node.

  This function forwards the received message to the `CausalBroadcast` GenServer
  that is registered to receive remote messages.

  ## Parameters
    - `{:remote, msg, broadcast_name}`: The cast message.
      - `msg`: The actual message payload.
      - `broadcast_name`: The registered name of the `CausalBroadcast` GenServer to forward the message to.
    - `state`: The current state of the GenServer.

  ## Returns
    - `{:noreply, state}`: The unchanged state.
  """
  @spec handle_cast({:remote, any(), atom()}, map()) :: {:noreply, map()}
  def handle_cast({:remote, msg, broadcast_name}, state) do
    send(broadcast_name, {:remote, msg}) # Send the message to the registered process
    {:noreply, state}
  end

  @impl true
  @doc """
  Handles the `{:remote, msg}` cast message (for backward compatibility).

  This function is called when a remote message is received from another node,
  using an older message format that does not include the `broadcast_name`.
  It forwards the message to the registered `respond_to` process.

  ## Parameters
    - `{:remote, msg}`: The cast message.
      - `msg`: The actual message payload.
    - `state`: The current state of the GenServer.

  ## Returns
    - `{:noreply, state}`: The unchanged state.
  """
  @spec handle_cast({:remote, any()}, map()) :: {:noreply, map()}
  def handle_cast({:remote, msg}, state) do
    # This clause is for backward compatibility with older message formats
    respond_to = state.respond_to # Get the PID of the registered process
    send(respond_to, {:remote, msg}) # Send the message to the registered process
    {:noreply, state}
  end

  @impl true
  @doc """
  Handles any other incoming `GenServer.handle_info` messages.

  This function is a fallback for handling any other incoming information that is not
  explicitly handled by other functions. It simply ignores the message.

  ## Parameters
    - `_msg`: The incoming message (not used).
    - `state`: The current state of the GenServer.

  ## Returns
    - `{:noreply, state}`: The unchanged state.
  """
  @spec handle_info(any(), map()) :: {:noreply, map()}
  def handle_info(_msg, state) do
    {:noreply, state} # Ignore the message
  end

  @impl true
  @doc """
  Handles code changes (hot code reloading).

  This function is called when the module is recompiled or upgraded. It simply returns the
  current state without modification, allowing for seamless code updates in a running system.

  ## Parameters
    - `_old_vsn`: The old version of the code (not used).
    - `state`: The current state of the GenServer.
    - `_extra`: Extra information (not used).

  ## Returns
    - `{:ok, state}`: The current state without modification.
  """
  @spec code_change(any(), map(), any()) :: {:ok, map()}
  def code_change(_old_vsn, state, _extra) do
    {:ok, state} # Return the current state without modification
  end

  # Private functions

  @doc """
  Connects to other Erlang nodes using the environment variable `MINIDOTE_NODES`.

  This function retrieves the value of the `MINIDOTE_NODES` environment variable, which is expected
  to be a comma-separated list of Erlang node names. It then attempts to connect to each of these nodes.
  """
  @spec find_other_nodes() :: :ok
  defp find_other_nodes do
    nodes = :string.tokens(:os.getenv(~c"MINIDOTE_NODES", ~c""), ~c",") # Get the list of nodes from the environment variable
    nodes2 = for n <- nodes, do: :erlang.list_to_atom(n) # Convert the node names to atoms
    Logger.info("Connecting #{inspect(node())} to other nodes: #{inspect(nodes2)}")
    try_connect(nodes2, 500)
  end

  @doc """
  Attempts to connect to a list of nodes recursively.

  This function attempts to connect to each node in the given list. It uses `:net_adm.ping/1` to check
  if a node is reachable. If a node is reachable, it is logged. Otherwise, it is
  retried with an increased timeout.

  ## Parameters
    - `nodes`: A list of Erlang node names (`[node()]`) to connect to.
    - `t`: The current timeout value in milliseconds (`non_neg_integer()`).
  """
  @spec try_connect([node()], non_neg_integer()) :: :ok
  defp try_connect(nodes, t) do
    {ping, pong} = :lists.partition(fn n -> :pong == :net_adm.ping(n) end, nodes) # Ping each node and partition into reachable and unreachable

    for n <- ping do
      Logger.info("Connected to node #{inspect(n)}")
    end

    case t > 1000 do # If the timeout is greater than 1 second, log the failed connections
      true ->
        for n <- pong do
          Logger.info("Failed to connect #{inspect(node())} to node #{inspect(n)}")
        end

      _ ->
        :ok
    end

    case pong do # If there are no unreachable nodes, log that we're connected to all nodes
      [] ->
        Logger.info("Connected to all nodes")

      _ ->
        :timer.sleep(t) # Sleep for the timeout period
        try_connect(pong, 2 * t) # Recursively try to connect to the unreachable nodes with an increased timeout
    end
  end
end
