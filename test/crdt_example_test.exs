defmodule CrdtTest do
  use ExUnit.Case

  test "simple local counter" do
    typ = Counter_PN_OB

    # this data type is opaque
    # don't pattern match against the raw crdt type!
    counter_crdt = CRDT.new(typ)

    # to get the value, use value/2
    counter_value = CRDT.value(typ, counter_crdt)
    ^counter_value = 0

    # prepare an update operation
    # here, increment the crdt by 9
    # operations of counter_pn include:
    # {increment, integer()}
    # {decrement, integer()}
    # you can see which operations are supported
    # in the corresponding file,
    # i.e. in antidote_crdt_counter_pn.erl
    # in the '-type op()' specification

    # first, we need to generate the downstream effect
    # for simple counters, the current state of the CRDT is not important
    # we can supply :ignore as the state
    {:ok, downstream} = CRDT.downstream(typ, {:increment, 9}, :ignore)
    # otherwise, we would need to use counter_crdt as defined above
    # {:ok, downstream} = CRDT.downstream(typ, {:increment, 9}, counter_crdt)

    # this downstream effect should be broadcasted to all nodes
    # we assume the downstream effect arrived locally
    # to generate a new value, we apply the downstream effect to the current state
    {:ok, counter_crdt} = CRDT.update(typ, downstream, counter_crdt) #local state!

    # we have overwritten the old state
    # to get the new value, use value again
    counter_value = CRDT.value(typ, counter_crdt)
    ^counter_value = 9

    IO.inspect(counter_value)
  end

end
