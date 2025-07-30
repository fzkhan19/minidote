defmodule TPSetTest do
  use ExUnit.Case
  doctest TPSet

  test "new/0" do
    {pid, return} = TPSet.new()
    assert MapSet.new() == return
    assert is_pid(pid)
  end

  test "new/1" do
    initial_set = MapSet.new([1,2,3,:abc,5])
    {pid, return} = TPSet.new(initial_set)
    assert initial_set == return
    assert is_pid(pid)
  end

  test "update empty" do
    add_set = MapSet.new([1,2,3,10,20,30])
    del_set = MapSet.new([1,2,3,:abc,5])
    result_set = MapSet.new([10,20,30])

    {pid, _return} = TPSet.new()
    assert result_set == TPSet.update(pid, {add_set, del_set})
  end

  test "add new" do
    {pid, return} = TPSet.new(MapSet.new([1,2,3,10,20,30]))
    update = TPSet.add(pid, 123)
    assert update == {MapSet.new([1,2,3,10,20,30,123]), MapSet.new()}
    assert MapSet.new([1,2,3,10,20,30,123]) == TPSet.update(pid, update)
  end

  test "delete existing" do
    {pid, return} = TPSet.new(MapSet.new([1,2,3,10,20,30]))
    update = TPSet.delete(pid, 30)
    assert update == {MapSet.new([1,2,3,10,20,30]), MapSet.new([30])} || {MapSet.new([1,2,3,10,20]), MapSet.new([30])}
    assert MapSet.new([1,2,3,10,20]) == TPSet.update(pid, update)
  end

  test "delete non existing" do
    {pid, return} = TPSet.new(MapSet.new([1,2,3,10,20,30]))
    update = TPSet.delete(pid, 123)
    assert update == {MapSet.new([1,2,3,10,20,30]), MapSet.new([123])}
    assert MapSet.new([1,2,3,10,20,30]) == TPSet.update(pid, update)
  end

  test "add deleted" do
    {pid, return} = TPSet.new(MapSet.new([1,2,3,10,20,30]))
    update1 = TPSet.delete(pid, 123)
    assert update1 == {MapSet.new([1,2,3,10,20,30]), MapSet.new([123])}

    view = TPSet.update(pid, update1)
    assert MapSet.new([1,2,3,10,20,30]) == view

    update2 = TPSet.add(pid, 123)
    assert update2 == {MapSet.new([1,2,3,10,20,30,123]), MapSet.new([123])}
    assert MapSet.new([1,2,3,10,20,30]) == TPSet.update(pid, update2)
  end

  test "everything everywhere all at once" do
    {pid1, _return} = TPSet.new(MapSet.new([1,2,3,10,20,30]))
    {pid2, _return} = TPSet.new()
    {pid3, _return} = TPSet.new(MapSet.new([1,2,5,6,7,20,30]))

    update1 = TPSet.add(pid1, :a)
    update2a = TPSet.add(pid2, :a)
    update2b = TPSet.add(pid2, :b)
    update3 = TPSet.add(pid3, :c)

    assert update1 == {MapSet.new([1,2,3,10,20,30,:a]), MapSet.new()}
    assert update2a == {MapSet.new([:a]), MapSet.new()}
    assert update2b == {MapSet.new([:b]), MapSet.new()}
    assert update3 == {MapSet.new([1,2,5,6,7,20,30,:c]), MapSet.new()}

    _result2_1 = TPSet.update(pid2, update1)
    _result2_2a = TPSet.update(pid2, update2a)
    _result2_2b = TPSet.update(pid2, update2b)
    result2_3 = TPSet.update(pid2, update3)

    assert result2_3 == MapSet.new([1,2,3,5,6,7,10,20,30,:a,:b,:c])

    _result2_1 = TPSet.update(pid1, update3)
    _result2_2a = TPSet.update(pid1, update2b)
    _result2_2b = TPSet.update(pid1, update2a)
    result1 = TPSet.update(pid1, update1)

    assert result1 == result2_3

    TPSet.update(pid2, TPSet.delete(pid2, 1))
    TPSet.update(pid2, TPSet.delete(pid2, :d))
    update_del = TPSet.delete(pid2, 30)

    assert update_del == {MapSet.new([1,2,3,5,6,7,10,20,30,:a,:b,:c]), MapSet.new([1,:d, 30])} || {MapSet.new([2,3,5,6,7,10,20,:a,:b,:c]), MapSet.new([1,:d, 30])}

    result_a = TPSet.update(pid1, update_del)
    result_b = TPSet.update(pid2, update_del)
    result_c = TPSet.update(pid3, update_del)

    assert result_a == result_b
    assert result_b == result_c
    assert result_c == MapSet.new([2,3,5,6,7,10,20,:a,:b,:c])

    _result = TPSet.update(pid3, update1)
    _result = TPSet.update(pid3, update3)
    _result = TPSet.update(pid3, update2a)
    result = TPSet.update(pid3, update2b)

    assert result_c == result
  end

end
