defmodule LWWRegisterTest do
  use ExUnit.Case, async: true
  doctest LWWRegister

  test "new/2 creates a new LWWRegister with the given value and timestamp" do
    register = LWWRegister.new("initial_value", 100)
    assert register == %{value: "initial_value", timestamp: 100}
  end

  test "update/3 updates the register if the new timestamp is greater" do
    register = LWWRegister.new("old_value", 100)
    updated_register = LWWRegister.update(register, "new_value", 200)
    assert updated_register == %{value: "new_value", timestamp: 200}
  end

  test "update/3 does not update the register if the new timestamp is not greater" do
    register = LWWRegister.new("old_value", 100)
    updated_register_equal = LWWRegister.update(register, "new_value", 100)
    assert updated_register_equal == register

    updated_register_less = LWWRegister.update(register, "new_value", 50)
    assert updated_register_less == register
  end

  test "value/1 returns the current value of the LWWRegister" do
    register = LWWRegister.new("some_value", 100)
    assert LWWRegister.value(register) == "some_value"

    register = LWWRegister.update(register, "updated_value", 200)
    assert LWWRegister.value(register) == "updated_value"
  end

  test "merge/2 returns register with later timestamp" do
    register1 = LWWRegister.new("value1", 100)
    register2 = LWWRegister.new("value2", 200)
    assert LWWRegister.merge(register1, register2) == register2
    assert LWWRegister.merge(register2, register1) == register2
  end

  test "merge/2 handles equal timestamps by preferring register1's value" do
    register1 = LWWRegister.new("value1", 100)
    register2 = LWWRegister.new("value2", 100) # Same timestamp

    # register1 should win due to tie-breaking rule
    assert LWWRegister.merge(register1, register2) == register1
    assert LWWRegister.merge(register2, register1) == register2 # Because register2 is now register1
  end

  test "merge/2 is idempotent" do
    register1 = LWWRegister.new("value1", 100)
    register2 = LWWRegister.new("value2", 200)
    merged = LWWRegister.merge(register1, register2)
    assert LWWRegister.merge(merged, register1) == merged
    assert LWWRegister.merge(merged, register2) == merged
  end

  test "merge/2 with self returns self" do
    register = LWWRegister.new("value", 100)
    assert LWWRegister.merge(register, register) == register
  end
end