defmodule MinidoteTest do
  use ExUnit.Case
  doctest Minidote

  test "greets the world" do
    # ToDo: This fails, so feel free to improve the tests :)
    assert Minidote.hello() == :world
  end

  test "setup nodes" do
    # start minidote1, minidote2
    [dc1, dc2] = [TestSetup.start_node(:minidote1), TestSetup.start_node(:minidote2)]
    # start minidote3
    dc3 = TestSetup.start_node(:minidote3)
    # crash a node
    TestSetup.stop_node(dc2)
    # restart a node
    dc2 = TestSetup.start_node(:minidote2)

    # tear down all nodes
    [TestSetup.stop_node(dc1), TestSetup.stop_node(dc2), TestSetup.stop_node(dc3)]
  end

  test "setup nodes in other test" do
    # note: using the same name affects other tests if some state is persisted
    [dc1, dc2] = [TestSetup.start_node(:minidote1), TestSetup.start_node(:minidote2)]
    dc3 = TestSetup.start_node(:minidote3)

    # tear down all nodes
    [TestSetup.stop_node(dc1), TestSetup.stop_node(dc2), TestSetup.stop_node(dc3)]
  end

  test "simple counter replication" do
    (nodes = [dc1, dc2]) = [TestSetup.start_node(:t1_minidote1), TestSetup.start_node(:t1_minidote2)]
    # debug messages:
    TestSetup.mock_link_layer(nodes, %{:debug => true})

    # increment counter by 42
    # When using Erlang rpc calls, the module name needs to be specified. Elixir modules are converted into Erlang modules via a $ModuleName -> :"Elixir.ModuleName" transformation
    {:ok, vc} = :rpc.call(dc1, :"Elixir.Minidote", :update_objects, [[{{"key", :counter_pn_ob, "simple counter replication"}, :increment, 42}], :ignore])
    # reading on the same replica returns 42
    {:ok, [{{"key", :counter_pn_ob, "simple counter replication"}, 42}], _vc2} = :rpc.call(dc1, :"Elixir.Minidote", :read_objects, [[{"key", :counter_pn_ob, "simple counter replication"}], vc])
    # reading on the other replica returns 42
    {:ok, [{{"key", :counter_pn_ob, "simple counter replication"}, 42}], _vc2} = :rpc.call(dc2, :"Elixir.Minidote", :read_objects, [[{"key", :counter_pn_ob, "simple counter replication"}], vc])

    # tear down all nodes
    [TestSetup.stop_node(dc1), TestSetup.stop_node(dc2)]
  end
end
