defmodule Minidote.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    Logger.notice("#{node()}: Starting Minidote application")
    # Change the secret Elixir cookie if given as environment variable:
    change_cookie()

    children = [
      # Starts a worker by calling: Minidote.Worker.start_link(MinidoteServer)
      # The Minidote service will then be locally available under the name MinidoteServer
      # Example call:
      # GenServer.call(MinidoteServer, :do_something)
      {MinidoteServer, MinidoteServer}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Minidote.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def change_cookie do
    case :os.getenv(~c"ERLANG_COOKIE") do
      false -> :ok
      cookie -> :erlang.set_cookie(node(), :erlang.list_to_atom(cookie))
    end
  end
end
