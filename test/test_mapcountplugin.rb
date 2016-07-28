require 'test_helper'
require 'roma/client'
require 'roma/client/plugin/mapcount'

Roma::Client::RomaClient.class_eval{
  def init_sync_routing_proc
  end
}

class MapCountPluginTest < Test::Unit::TestCase
  self.test_order = :defined
  include RomaTestUtils

  DATE_FORMAT = "%Y-%m-%dT%H:%M:%S +00"

  def setup
    start_roma
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"], [Roma::Client::Plugin::MapCount])
    @k = "key1"
    @sk = "subkey1"
    @lt = Time.parse(Time.now.strftime(DATE_FORMAT))
    @ret = {"last_updated_date"=>@lt, @sk=>1}
  end

  def teardown
    stop_roma
  end

  def check_roma_value(h, res)
    h.each{|k, v|
      if k == 'last_updated_date'
        assert_equal true, res[k].between?(v, v+1)
      else
        assert_equal v, res[k]
      end
    }
  end

  def test_mapcount_get(n = 5)
    n.times do |i|
      @sk = "subkey#{i}"
      expt = i
      
      retc = {"last_updated_date"=>@lt}
      (i+1).times do |j|
        retc["subkey#{j}"] = 1
      end
      check_roma_value(retc, @rc.mapcount_countup(@k, @sk, 0))
      
      retg = {"last_updated_date"=>@lt}
      retg["subkey#{i}"] = 1
      check_roma_value(retg, @rc.mapcount_get(@k, @sk))
      check_roma_value(retc, @rc.mapcount_get(@k))
    end

    ret2 = {"last_updated_date"=>@lt, "subkey0"=>1, "subkey1"=>1}
    check_roma_value(ret2, @rc.mapcount_get(@k, 'subkey0,subkey1'))
  end

  def test_mapcount_countup_array
    assert_equal @ret, @rc.mapcount_countup(@k, [@sk])
  end

  def test_mapcount_countup_array_multi
    ret2 = {"last_updated_date"=>@lt, "subkey0"=>1, "subkey1"=>1}
    check_roma_value(ret2, @rc.mapcount_countup(@k, ["subkey0", "subkey1"]))

    ret2 = {"last_updated_date"=>@lt, "subkey0"=>1, "subkey1"=>2}
    @rc.mapcount_countup(@k, ["subkey1"])
    check_roma_value(ret2, @rc.mapcount_get(@k))
  end

  def test_mapcount_countup_hash
    ret2 = {"last_updated_date"=>@lt, "subkey0"=>1, "subkey1"=>1}
    check_roma_value(ret2, @rc.mapcount_countup(@k, ["subkey0", "subkey1"]))

    ret2 = {"last_updated_date"=>@lt, "subkey0"=>4, "subkey1"=>1}
    @rc.mapcount_countup(@k, {"subkey0" => 3})
    check_roma_value(ret2, @rc.mapcount_get(@k))

    ret2 = {"last_updated_date"=>@lt, "subkey0"=>4, "subkey1"=>11}
    @rc.mapcount_countup(@k, {"subkey1" => 10})
    check_roma_value(ret2, @rc.mapcount_get(@k))
  end

  def test_mapcount_countup_degits
    ret2 = {"last_updated_date"=>@lt, "subkey0"=>125, "subkey1"=>30}
    @rc.mapcount_countup(@k, {"subkey0" => 125, "subkey1" => 30})
    check_roma_value(ret2, @rc.mapcount_get(@k))
  end

  def test_mapcount_countup_expt
    assert_nil @rc.mapcount_get(@k, @sk)
    assert_equal @ret, @rc.mapcount_countup(@k, @sk, 1)
    assert_equal @ret, @rc.mapcount_get(@k, @sk)
    sleep 2
    assert_nil @rc.mapcount_get(@k, @sk)
  end

  def test_mapcount_update
    ret_time = {"last_updated_date"=>@lt}

    assert_nil @rc.mapcount_update(@k)
    check_roma_value(@ret, @rc.mapcount_countup(@k, @sk, 0))
    check_roma_value(@ret, @rc.mapcount_update(@k))
    check_roma_value(ret_time, @rc.mapcount_update(@k, 'subkey2'))
    check_roma_value(@ret, @rc.mapcount_update(@k, nil, 1))
    sleep 2
    assert_nil @rc.mapcount_get(@k)
  end

  def test_mapcount_update_subkeys
    ret = {"last_updated_date"=>@lt, @sk=>1}
    check_roma_value(@ret, @rc.mapcount_countup(@k, @sk, 0))
    check_roma_value(@ret, @rc.mapcount_update(@k, @sk, 1))
    sleep 2
    assert_nil @rc.mapcount_get(@k)
  end

  def test_mapcount_update_time
    ret = {"last_updated_date"=>@lt, @sk=>1}
    check_roma_value(@ret, @rc.mapcount_countup(@k, @sk, 0))
    sleep 2
    lt2 = Time.parse(Time.now.strftime(DATE_FORMAT))
    ret2 = {"last_updated_date"=>lt2, @sk=>1}
    check_roma_value(ret2, @rc.mapcount_update(@k))
  end

  def test_counts
    conn1 = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")
    conn2 = Roma::Messaging::ConPool.instance.get_connection("localhost_11212")

    if conn1 && conn2
      conns = [conn1, conn2]

      ret = get_stat_sum(conns, "stat read_count\n")
      assert_equal 0, ret

      ret = get_stat_sum(conns, "stat write_count\n")
      assert_equal 0, ret

      @rc.mapcount_get('key')
      ret = get_stat_sum(conns, "stat read_count\n")
      assert_equal 1, ret

      @rc.mapcount_countup('key', 'subkey', 0)
      ret = get_stat_sum(conns, "stat write_count\n")
      assert_equal 1, ret

      @rc.mapcount_update('key')
      ret = get_stat_sum(conns, "stat write_count\n")
      assert_equal 2, ret
    else
      flunk(message="Fail to get connection")
    end

    conn1.close
    conn2.close
  end

  def test_mapcount_get_ms(n = 5)
    n.times do |i|
      @sk = "subkey#{i}"
      expt = i
      
      retc = {"last_updated_date"=>@lt}
      (i+1).times do |j|
        retc["subkey#{j}"] = 1
      end
      check_roma_value(retc, @rc.mapcount_countup_ms(@k, @sk, 0))

      retg = {"last_updated_date"=>@lt}
      retg["subkey#{i}"] = 1
      check_roma_value(retg, @rc.mapcount_get_ms(@k, @sk))
      check_roma_value(retc, @rc.mapcount_get_ms(@k))
    end

    ret2 = {"last_updated_date"=>@lt, "subkey0"=>1, "subkey1"=>1}
    check_roma_value(ret2, @rc.mapcount_get_ms(@k, 'subkey0,subkey1'))
  end

  def test_mapcount_countup_ms_array
    assert_equal @ret, @rc.mapcount_countup_ms(@k, [@sk])
  end

  def test_mapcount_countup_ms_array_multi
    ret2 = {"last_updated_date"=>@lt, "subkey0"=>1, "subkey1"=>1}
    check_roma_value(ret2, @rc.mapcount_countup_ms(@k, ["subkey0", "subkey1"]))

    ret2 = {"last_updated_date"=>@lt, "subkey0"=>1, "subkey1"=>2}
    @rc.mapcount_countup_ms(@k, ["subkey1"])
    check_roma_value(ret2, @rc.mapcount_get_ms(@k))
  end

  def test_mapcount_countup_ms_hash
    ret2 = {"last_updated_date"=>@lt, "subkey0"=>1, "subkey1"=>1}
    check_roma_value(ret2, @rc.mapcount_countup_ms(@k, ["subkey0", "subkey1"]))

    ret2 = {"last_updated_date"=>@lt, "subkey0"=>4, "subkey1"=>1}
    @rc.mapcount_countup_ms(@k, {"subkey0" => 3})
    check_roma_value(ret2, @rc.mapcount_get_ms(@k))

    ret2 = {"last_updated_date"=>@lt, "subkey0"=>4, "subkey1"=>11}
    @rc.mapcount_countup_ms(@k, {"subkey1" => 10})
    check_roma_value(ret2, @rc.mapcount_get_ms(@k))
  end

  def test_mapcount_countup_ms_degits
    ret2 = {"last_updated_date"=>@lt, "subkey0"=>125, "subkey1"=>30}
    @rc.mapcount_countup_ms(@k, {"subkey0" => 125, "subkey1" => 30})
    check_roma_value(ret2, @rc.mapcount_get_ms(@k))
  end

  def test_mapcount_countup_ms_expt
    assert_nil @rc.mapcount_get_ms(@k, @sk)
    assert_equal @ret, @rc.mapcount_countup_ms(@k, @sk, 1)
    assert_equal @ret, @rc.mapcount_get_ms(@k, @sk)
    sleep 2
    assert_nil @rc.mapcount_get_ms(@k, @sk)
  end

  def test_mapcount_update_ms
    ret_time = {"last_updated_date"=>@lt}

    assert_nil @rc.mapcount_update_ms(@k)
    check_roma_value(@ret, @rc.mapcount_countup_ms(@k, @sk, 0))
    check_roma_value(@ret, @rc.mapcount_update_ms(@k))
    check_roma_value(ret_time, @rc.mapcount_update_ms(@k, 'subkey2'))
    check_roma_value(@ret, @rc.mapcount_update_ms(@k, nil, 1))
    sleep 2
    assert_nil @rc.mapcount_get_ms(@k)
  end

  def test_mapcount_update_ms_subkeys
    ret = {"last_updated_date"=>@lt, @sk=>1}
    check_roma_value(@ret, @rc.mapcount_countup_ms(@k, @sk, 0))
    check_roma_value(@ret, @rc.mapcount_update_ms(@k, @sk, 1))
    sleep 2
    assert_nil @rc.mapcount_get_ms(@k)
  end

  def test_mapcount_update_ms_time
    ret = {"last_updated_date"=>@lt, @sk=>1}
    check_roma_value(@ret, @rc.mapcount_countup_ms(@k, @sk, 0))
    sleep 2
    lt2 = Time.parse(Time.now.strftime(DATE_FORMAT))
    ret2 = {"last_updated_date"=>lt2, @sk=>1}
    check_roma_value(ret2, @rc.mapcount_update_ms(@k))
  end

  def test_counts_ms
    conn1 = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")
    conn2 = Roma::Messaging::ConPool.instance.get_connection("localhost_11212")

    if conn1 && conn2
      conns = [conn1, conn2]

      ret = get_stat_sum(conns, "stat read_count\n")
      assert_equal 0, ret

      ret = get_stat_sum(conns, "stat write_count\n")
      assert_equal 0, ret

      @rc.mapcount_get_ms('key')
      ret = get_stat_sum(conns, "stat read_count\n")
      assert_equal 1, ret

      @rc.mapcount_countup_ms('key', 'subkey', 0)
      ret = get_stat_sum(conns, "stat write_count\n")
      assert_equal 1, ret

      @rc.mapcount_update_ms('key')
      ret = get_stat_sum(conns, "stat write_count\n")
      assert_equal 2, ret
    else
      flunk(message="Fail to get connection")
    end

    conn1.close
    conn2.close
  end

  private
  def get_stat_sum(conns, msg)
    sum = 0
    conns.each do |conn|
      conn.write(msg)
      r = conn.gets.split(" ")
      sum += r[1].to_i
      conn.gets
    end
    sum
  end

end #MapCountPluginTest
