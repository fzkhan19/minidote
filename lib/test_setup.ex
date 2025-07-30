# FIXME put TestSetup module under test/ folder
# don't know why test_helper.exs does not recognize this module if put under test/
defmodule TestSetup do
  require Logger

  def init() do
    Logger.notice("Starting distributed setup")
    {:ok, _} = :net_kernel.start([:"master@127.0.0.1"])
    {:ok, _} = :erl_boot_server.start([{127,0,0,1}])
    Logger.notice("Finished distributed setup")
  end

  # assumes that the node is down!
  def start_node(name) do
    Logger.notice("Booting distributed node #{name}")
    cookie = :erlang.get_cookie()
    {:ok, node} =
      :slave.start_link(
        ~c"127.0.0.1",
        :"#{name}",
        ~c"-loader inet -hosts 127.0.0.1 -setcookie \"#{cookie}\""
      )
    Logger.notice("Started node #{node}")

    # initialize environment
    rpc = &(_ = :rpc.call(node, &1, &2, &3))
    rpc.(:code, :add_paths, [:code.get_path()])
    rpc.(Application, :ensure_all_started, [:mix])
    rpc.(Application, :ensure_all_started, [:logger])
    rpc.(Logger, :configure, [[level: Logger.level()]])
    rpc.(Mix, :env, [Mix.env()])
    Logger.notice("Starting minidote on node: #{inspect node}")
    rpc.(Application, :ensure_all_started, [:minidote])

    node
  end

  # # helper function to start multiple nodes at once, uncomment if needed
  # @spec start(integer()) :: list()
  # def start(num_nodes) do
  #   Logger.notice("Booting #{num_nodes} distributed nodes with prefix :minidote")
  #   name = :minidote
  #   cookie = :erlang.get_cookie()
  #   nodes = Enum.map(1..num_nodes, fn i ->
  #       {:ok, name} =
  #         :slave.start_link(
  #           ~c"127.0.0.1",
  #           :"#{name}#{i}",
  #           ~c"-loader inet -hosts 127.0.0.1 -setcookie \"#{cookie}\""
  #         )

  #       name
  #     end)
  #   Logger.notice("Started nodes #{inspect nodes}")
  #   nodes
  # end

  @spec stop_node(atom) :: :ok
  def stop_node(node) do
    :slave.stop(node)
  end

  @spec stop_nodes([atom]) :: :ok
  def stop_nodes(nodes) do
    Enum.each(nodes, &:slave.stop/1)
  end

  def mock_link_layer(nodes, options) do
    IO.puts("Mocking link layer for  #{inspect nodes}")
    for node <- nodes do :rpc.cast(node, __MODULE__, :add_delay_r, [self(), node(), options]) end
    IO.puts("Waiting delay for #{inspect nodes}")
    for node <- nodes do receive do {:add_delay_r_done, ^node} -> :ok end end
    IO.puts("Added delay for #{inspect nodes}")
    :ok
  end

  def add_delay_r(requester, _this_node, options) do
    :application.ensure_all_started(:meck)
    :ok = :meck.new(:"Elixir.LinkLayer", [:passthrough])
    :ok = :meck.expect(:"Elixir.LinkLayer", :send, fn(ll, data, receiver) ->
      spawn(fn() ->
        case :maps.find(:delay, options) do
          {:ok, ms} when is_number(ms) ->
            :timer.sleep(ms)
          {:ok, f} when is_function(f) ->
            ms = f.(node(), receiver)
            :timer.sleep(ms)
          :error -> :ok
        end
        case :maps.get(:debug, options, false) do
          true ->
            :ok #TODO use this_node
            # :rpc.call(this_node, :ct, :pal, ["Sending message from ~p to ~p:~n ~180p", [node(), Receiver, Data]])
          false ->
            :ok
        end
        apply(:meck_util.original_name(:"Elixir.LinkLayer"), :send, [ll, data, receiver])
      end)
    end)
    # answer to requester
    send(requester, {:add_delay_r_done, node()})
    # wait until monitor is done:
    monitor_ref = :erlang.monitor(:process, requester)
    receive do
      {:DOWN, ^monitor_ref, _type, _object, _info} ->
        :ok
    end
  end


  def counter() do
    spawn_link(fn() -> counter(0) end)
  end

  def counter(n) do
    receive do
      {:get, sender} ->
        send(sender, {self(), n})
        counter(n)
      {:increment, x} ->
        counter(n+x)
    end
  end

  def counter_get(c) do
    send(c, {:get, self()})
    receive do
      {^c, n} -> n
    end
  end

  def counter_inc(c, n) do
    send(c, {:increment, n})
  end

end
