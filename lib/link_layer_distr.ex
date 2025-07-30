defmodule LinkLayerDistr do
  @moduledoc """
  An implementation of the link layer using distributed Erlang
  and the pg process group library (translated from Erlang)
  """

  use GenServer
  require Logger

  defstruct group_name: nil, respond_to: :none

  # Public API
  def start_link(group_name) when is_atom(group_name) do
    GenServer.start_link(__MODULE__, [group_name], [])
  end

  def stop(pid) do
    GenServer.stop(pid)
  end

  # GenServer Callbacks
  @impl true
  def init([group_name]) do
    # Initially, try to connect with other erlang nodes
    spawn_link(&find_other_nodes/0)
    # Ensure :pg is started (OTP 23+)
    :pg.start_link()
    :pg.join(group_name, self())
    Process.register(self(), group_name)
    {:ok, %__MODULE__{group_name: group_name, respond_to: :none}}
  end

  @impl true
  def terminate(_reason, state) do
    :pg.leave(state.group_name, self())
  end

  @impl true
  def handle_call({:send, data, node, broadcast_name}, _from, state) do
    GenServer.cast({state.group_name, node}, {:remote, data, broadcast_name})
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:register, r}, _from, state) do
    {:reply, :ok, %{state | respond_to: r}}
  end

  @impl true
  def handle_call(:all_nodes, _from, state) do
    members = :pg.get_members(state.group_name)
    nodes = for m <- members, do: node(m)
    {:reply, {:ok, nodes}, state}
  end

  @impl true
  def handle_call(:other_nodes, _from, state) do
    members = :pg.get_members(state.group_name)
    self_pid = self()
    other_members = for m <- members, m != self_pid, do: node(m)
    {:reply, {:ok, other_members}, state}
  end

  @impl true
  def handle_call(:this_node, _from, state) do
    {:reply, {:ok, node()}, state}
  end

  @impl true
  def handle_cast({:remote, msg, broadcast_name}, state) do
    send(broadcast_name, {:remote, msg})
    {:noreply, state}
  end

  @impl true
  def handle_cast({:remote, msg}, state) do
    # This clause is for backward compatibility with older message formats
    respond_to = state.respond_to
    send(respond_to, {:remote, msg})
    {:noreply, state}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def code_change(_old_vsn, state, _extra) do
    {:ok, state}
  end

  # Private functions
  # Connect to other erlang nodes using the environment variable MINIDOTE_NODES
  defp find_other_nodes do
    nodes = :string.tokens(:os.getenv(~c"MINIDOTE_NODES", ~c""), ~c",")
    nodes2 = for n <- nodes, do: :erlang.list_to_atom(n)
    Logger.info("Connecting #{inspect(node())} to other nodes: #{inspect(nodes2)}")
    try_connect(nodes2, 500)
  end

  defp try_connect(nodes, t) do
    {ping, pong} = :lists.partition(fn n -> :pong == :net_adm.ping(n) end, nodes)

    for n <- ping do
      Logger.info("Connected to node #{inspect(n)}")
    end

    case t > 1000 do
      true ->
        for n <- pong do
          Logger.info("Failed to connect #{inspect(node())} to node #{inspect(n)}")
        end

      _ ->
        :ok
    end

    case pong do
      [] ->
        Logger.info("Connected to all nodes")

      _ ->
        :timer.sleep(t)
        try_connect(pong, 2 * t)
    end
  end
end
