defmodule Minidote.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc """
  The main application module for Minidote.

  This module defines the application's supervision tree and is responsible for
  starting and stopping the core components of the Minidote distributed database.
  It also handles the configuration of the Erlang cookie for distributed node
  communication.
  """
  use Application
  require Logger

  @doc """
  Starts the Minidote application.

  This function is the entry point for the Minidote application. It configures
  the Erlang cookie if specified via the `ERLANG_COOKIE` environment variable,
  and then starts the supervision tree for the application's children, including
  the `MinidoteServer`.

  ## Parameters
    - `_type`: The application type (e.g., `:permanent`, `:transient`).
    - `_args`: A list of arguments passed to the application.

  ## Returns
    - `{:ok, pid}`: If the application starts successfully, where `pid` is the PID of the supervisor.
    - `{:error, reason}`: If the application fails to start.
  """
  @impl true
  @spec start(:normal | :permanent | :transient, list()) :: {:ok, pid()} | {:error, any()}
  def start(_type, _args) do
    Logger.notice("#{node()}: Starting Minidote application")
    # Change the secret Elixir cookie if given as environment variable:
    change_cookie()

    children = [
      # Starts a worker by calling: MinidoteServer.start_link(MinidoteServer)
      # The MinidoteServer will then be locally available under the name MinidoteServer
      {MinidoteServer, MinidoteServer}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Minidote.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @doc """
  Changes the Erlang cookie for the current node.

  This function checks for the `ERLANG_COOKIE` environment variable. If it is set,
  the Erlang cookie for the current node is updated to the value of this
  environment variable. This is crucial for enabling secure communication between
  distributed Erlang nodes.

  ## Returns
    - `:ok`: Always returns `:ok`.
  """
  @spec change_cookie() :: :ok
  def change_cookie do
    case :os.getenv(~c"ERLANG_COOKIE") do
      false -> :ok
      cookie -> :erlang.set_cookie(node(), :erlang.list_to_atom(cookie))
    end
  end
end
