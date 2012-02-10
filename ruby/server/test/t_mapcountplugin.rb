#!/usr/bin/env ruby

#require 'roma/messaging/con_pool'
#require 'roma/plugin/plugin_mapcount'
require 'roma/client'
require '../../../roma-client-plugin-mapcount/lib/roma/client/plugin/mapcount.rb'

Roma::Client::RomaClient.class_eval{
  def init_sync_routing_proc
  end
}

class MapCountPluginTest < Test::Unit::TestCase
  include RomaTestUtils

  DATE_FORMAT = "%Y-%m-%dT%H:%M:%S +00"

  def setup
    start_roma
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"], [Roma::Client::Plugin::MapCount])
  end

  def teardown
    stop_roma
  end

  def test_mapcount_get(n = 5)
    n.times do |i|
      k = "subkey#{i}"
      expt = i
      lt = Time.parse(Time.now.gmtime.strftime(DATE_FORMAT))

      retc = {"last_updated_date"=>lt}
      (i+1).times do |j|
        retc["subkey#{j}"] = 1
      end
      assert_equal retc, @rc.mapcount_countup('key1', k, 0)

      retg = {"last_updated_date"=>lt}
      retg["subkey#{i}"] = 1
      assert_equal retg, @rc.mapcount_get('key1', k)
      assert_equal retc, @rc.mapcount_get('key1')
    end    

    lt = Time.parse(Time.now.gmtime.strftime(DATE_FORMAT))
    ret = {"last_updated_date"=>lt, "subkey0"=>1, "subkey1"=>1}
    assert_equal ret, @rc.mapcount_get('key1', 'subkey0,subkey1')
  end

  def test_mapcount_countup_expt
    k = "subkey1"
    lt = Time.parse(Time.now.gmtime.strftime(DATE_FORMAT))
    ret = {"last_updated_date"=>lt, k=>1}

    assert_nil @rc.mapcount_get('key1', k)
    assert_equal ret, @rc.mapcount_countup('key1', k, 1)
    assert_equal ret, @rc.mapcount_get('key1', k)
    sleep 2
    assert_nil @rc.mapcount_get('key1', k)
  end

  def test_mapcount_update
    k = "subkey1"
    lt = Time.parse(Time.now.gmtime.strftime(DATE_FORMAT))
    ret_time = {"last_updated_date"=>lt}
    ret_all = {"last_updated_date"=>lt, k=>1}

    assert_nil @rc.mapcount_update('key1')
    assert_equal ret_all, @rc.mapcount_countup('key1', k, 0)
    assert_equal ret_all, @rc.mapcount_update('key1')
    assert_equal ret_time, @rc.mapcount_update('key1', 'subkey2')

    assert_equal ret_all, @rc.mapcount_update('key1', nil, 1)
    sleep 2
    assert_nil @rc.mapcount_get('key1')

    lt = Time.parse(Time.now.gmtime.strftime(DATE_FORMAT))
    ret_all = {"last_updated_date"=>lt, k=>1}
    assert_equal ret_all, @rc.mapcount_countup('key1', k, 0)
    assert_equal ret_all, @rc.mapcount_update('key1', k, 1)
    sleep 2
    assert_nil @rc.mapcount_get('key1')

    lt = Time.parse(Time.now.gmtime.strftime(DATE_FORMAT))
    ret_all = {"last_updated_date"=>lt, k=>1}
    assert_equal ret_all, @rc.mapcount_countup('key1', k, 0)
    sleep 2
    lt = Time.parse(Time.now.gmtime.strftime(DATE_FORMAT))
    ret_all = {"last_updated_date"=>lt, k=>1}
    assert_equal ret_all, @rc.mapcount_update('key1')
  end

  def test_counts
    conn = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")
    if conn
      conn.write "stat read_count\n"
      r = conn.gets.split(" ")
      assert_equal 0, r[1].to_i
      conn.gets

      @rc.mapcount_get('key')
      sleep 10
      conn.write "stat read_count\n"
      r = conn.gets.split(" ")
      assert_equal 1, r[1].to_i
      conn.gets

      conn.write "stat write_count\n"
      r = conn.gets.split(" ")
      assert_equal 0, r[1].to_i
      conn.gets

      @rc.mapcount_countup('key', 'subkey', 0)
      sleep 10
      conn.write "stat write_count\n"
      r = conn.gets.split(" ")
      assert_equal 1, r[1].to_i
      conn.gets

      @rc.mapcount_update('key')
      sleep 10
      conn.write "stat write_count\n"
      r = conn.gets.split(" ")
      assert_equal 2, r[1].to_i
      conn.gets

      conn.close
    end
  end

  def test_mapcount_get_ms(n = 5)
    n.times do |i|
      k = "subkey#{i}"
      expt = i
      lt = Time.parse(Time.now.gmtime.strftime(DATE_FORMAT))

      retc = {"last_updated_date"=>lt}
      (i+1).times do |j|
        retc["subkey#{j}"] = 1
      end
      assert_equal retc, @rc.mapcount_countup_ms('key1', k, 0)

      retg = {"last_updated_date"=>lt}
      retg["subkey#{i}"] = 1
      assert_equal retg, @rc.mapcount_get_ms('key1', k)
      assert_equal retc, @rc.mapcount_get_ms('key1')
    end    

    lt = Time.parse(Time.now.gmtime.strftime(DATE_FORMAT))
    ret = {"last_updated_date"=>lt, "subkey0"=>1, "subkey1"=>1}
    assert_equal ret, @rc.mapcount_get_ms('key1', 'subkey0,subkey1')
  end

  def test_mapcount_countup_ms_expt
    k = "subkey1"
    lt = Time.parse(Time.now.gmtime.strftime(DATE_FORMAT))
    ret = {"last_updated_date"=>lt, k=>1}

    assert_nil @rc.mapcount_get_ms('key1', k)
    assert_equal ret, @rc.mapcount_countup_ms('key1', k, 1)
    assert_equal ret, @rc.mapcount_get_ms('key1', k)
    sleep 2
    assert_nil @rc.mapcount_get_ms('key1', k)
  end

  def test_mapcount_update_ms
    k = "subkey1"
    lt = Time.parse(Time.now.gmtime.strftime(DATE_FORMAT))
    ret_time = {"last_updated_date"=>lt}
    ret_all = {"last_updated_date"=>lt, k=>1}

    assert_nil @rc.mapcount_update_ms('key1')
    assert_equal ret_all, @rc.mapcount_countup_ms('key1', k, 0)
    assert_equal ret_all, @rc.mapcount_update_ms('key1')
    assert_equal ret_time, @rc.mapcount_update_ms('key1', 'subkey2')

    assert_equal ret_all, @rc.mapcount_update_ms('key1', nil, 1)
    sleep 2
    assert_nil @rc.mapcount_get_ms('key1')

    lt = Time.parse(Time.now.gmtime.strftime(DATE_FORMAT))
    ret_all = {"last_updated_date"=>lt, k=>1}
    assert_equal ret_all, @rc.mapcount_countup_ms('key1', k, 0)
    assert_equal ret_all, @rc.mapcount_update_ms('key1', k, 1)
    sleep 2
    assert_nil @rc.mapcount_get_ms('key1')

    lt = Time.parse(Time.now.gmtime.strftime(DATE_FORMAT))
    ret_all = {"last_updated_date"=>lt, k=>1}
    assert_equal ret_all, @rc.mapcount_countup_ms('key1', k, 0)
    sleep 2
    lt = Time.parse(Time.now.gmtime.strftime(DATE_FORMAT))
    ret_all = {"last_updated_date"=>lt, k=>1}
    assert_equal ret_all, @rc.mapcount_update_ms('key1')
  end

  def test_counts_ms
    conn = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")
    if conn
      conn.write "stat read_count\n"
      r = conn.gets.split(" ")
      assert_equal 0, r[1].to_i
      conn.gets

      @rc.mapcount_get_ms('key')
      sleep 10
      conn.write "stat read_count\n"
      r = conn.gets.split(" ")
      assert_equal 1, r[1].to_i
      conn.gets

      conn.write "stat write_count\n"
      r = conn.gets.split(" ")
      assert_equal 0, r[1].to_i
      conn.gets

      @rc.mapcount_countup_ms('key', 'subkey', 0)
      sleep 10
      conn.write "stat write_count\n"
      r = conn.gets.split(" ")
      assert_equal 1, r[1].to_i
      conn.gets

      @rc.mapcount_update_ms('key')
      sleep 10
      conn.write "stat write_count\n"
      r = conn.gets.split(" ")
      assert_equal 2, r[1].to_i
      conn.gets

      conn.close
    end
  end
end # MapCountPluginTest
