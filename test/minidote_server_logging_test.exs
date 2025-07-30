defmodule MinidoteServerLoggingTest do
  use ExUnit.Case, async: true
  alias MinidoteServer
  alias ConflictFreeReplicatedDataType
  alias Vector_Clock
  require GCounter

  setup do
    # Ensure a clean dets state for each test by deleting the files
    File.rm("op_log")
    File.rm("crdt_snapshots")
    :ok
  end

  test "local operations are logged to op_log" do
    {:ok, pid} = MinidoteServer.start_link(:test_server_local_op)
    # Wait for init to complete and dets tables to open
    Process.sleep(100)

    # Simulate a local modification
    full_key = {"test_key", :g_counter, "default"}
    updates = [{full_key, :increment, 5}]
    {:ok, _clock} = GenServer.call(pid, {:modify_data_items, updates, :ignore})

    # Verify that the operation is logged
    {:ok, op_log_ref} = :dets.open_file(:op_log, [type: :set])
    entries = :dets.foldl(fn entry, acc -> [entry | acc] end, [], op_log_ref)
    :dets.close(op_log_ref)

    assert length(entries) == 1
    {logged_clock, logged_full_key, logged_crdt_type_atom, logged_crdt_type, logged_effect, logged_sender_node} = List.first(entries)

    assert logged_full_key == full_key
    assert logged_crdt_type == GCounter
    assert logged_effect == {:increment, 5}
    assert logged_sender_node == node()
    # assert Vector_Clock.is_vector_clock?(logged_clock) # No public API for this
  end

  test "remote operations are logged to op_log" do
    {:ok, pid} = MinidoteServer.start_link(:test_server_remote_op)
    # Wait for init to complete and dets tables to open
    Process.sleep(100)

    # Simulate a remote CRDT update delivery
    full_key = {"remote_key", :g_counter, "remote_bucket"}
    crdt_type_atom = :g_counter
    crdt_type = GCounter
    effect = {:increment, 10}
    sender_clock = Vector_Clock.new() |> Vector_Clock.increment(:node_remote)
    sender_node = :node_remote

    send(pid, {:deliver, {:crdt_update, full_key, crdt_type_atom, crdt_type, effect, sender_clock, sender_node}})
    Process.sleep(100) # Give time for handle_info to process

    # Verify that the operation is logged
    {:ok, op_log_ref} = :dets.open_file(:op_log, [type: :set])
    entries = :dets.foldl(fn entry, acc -> [entry | acc] end, [], op_log_ref)
    :dets.close(op_log_ref)

    assert length(entries) == 1
    {logged_clock, logged_full_key, logged_crdt_type_atom, logged_crdt_type, logged_effect, logged_sender_node} = List.first(entries)

    assert logged_full_key == full_key
    assert logged_crdt_type == GCounter
    assert logged_effect == effect
    assert logged_sender_node == sender_node
    # assert Vector_Clock.is_vector_clock?(logged_clock) # No public API for this
  end
end