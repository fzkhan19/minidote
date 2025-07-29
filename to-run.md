`iex --name kevin@127.0.0.1 --cookie ERLANG_COOKIE -S mix`
`iex --name daniel@127.0.0.1 --cookie ERLANG_COOKIE -S mix`
`iex --name goveas@127.0.0.1 --cookie ERLANG_COOKIE -S mix`
MINIDOTE_NODES="node1@127.0.0.1,node3@127.0.0.1" iex --name node2@127.0.0.1 -S mix
MINIDOTE_NODES="node1@127.0.0.1,node2@127.0.0.1" iex --name node3@127.0.0.1 -S mix
MINIDOTE_NODES="node2@127.0.0.1,node3@127.0.0.1" iex --name node1@127.0.0.1 -S mix

NODE 1

# Create a new counter with a unique name

counter_key = {"distributed_test", Counter_PN_OB, "test_bucket"}
{:ok, clock1} = Minidote.update_objects([{counter_key, :increment, 10}], :ignore)
IO.puts("Node1: Incremented by 10")

NODE 2
counter_key = {"distributed_test", Counter_PN_OB, "test_bucket"}
:timer.sleep(1000)
{:ok, results, clock2} = Minidote.read_objects([counter_key], :ignore)
IO.inspect(results, label: "Node2 read")

# Now increment from node2

{:ok, clock3} = Minidote.update_objects([{counter_key, :increment, 20}], clock2)
IO.puts("Node2: Incremented by 20")

NODE 3
counter_key = {"distributed_test", Counter_PN_OB, "test_bucket"}
:timer.sleep(1000)
{:ok, results, \_clock} = Minidote.read_objects([counter_key], :ignore)
IO.inspect(results, label: "Node3 final read")

# Should show 30 (10 + 20)

TEST

# Copy this into any node and run:

MinidoteQuickTest.run_all_tests()
