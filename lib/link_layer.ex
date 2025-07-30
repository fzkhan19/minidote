defmodule LinkLayer do
  @moduledoc """
  Provides an abstraction for the link layer implementation.

  This module defines the API for interacting with the link layer, which is responsible
  for communication between nodes in the distributed system. It delegates the actual
  implementation to a specific link layer module (e.g., `LinkLayerDistr`).
  """

  # API

  @doc """
  Starts the link layer.

  Delegates to `LinkLayerDistr.start_link/1`.
  """
  def start_link(group_name) do
    LinkLayerDistr.start_link(group_name)
  end

  @doc """
  Sends data to a specific node.

  Delegates to `GenServer.call/2` with the `:send` message.
  """
  def send(ll, data, node, broadcast_name) do
    GenServer.call(ll, {:send, data, node, broadcast_name})
  end

  @doc """
  Registers a receiver process with the link layer.

  Delegates to `GenServer.call/2` with the `:register` message.
  """
  def register(ll, receiver) do
    GenServer.call(ll, {:register, receiver})
  end

  @doc """
  Retrieves a list of all nodes in the link layer.

  Delegates to `GenServer.call/2` with the `:all_nodes` message.
  """
  def all_nodes(ll) do
    GenServer.call(ll, :all_nodes)
  end

  @doc """
  Retrieves a list of all nodes in the link layer, excluding the current node.

  Delegates to `GenServer.call/2` with the `:other_nodes` message.
  """
  def other_nodes(ll) do
    GenServer.call(ll, :other_nodes)
  end

  @doc """
  Retrieves the current node.

  Delegates to `GenServer.call/2` with the `:this_node` message.
  """
  def this_node(ll) do
    GenServer.call(ll, :this_node)
  end
end
