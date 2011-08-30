#!/usr/bin/env ruby
require 'roma/client/rclient'
require 'roma/plugin/plugin_map'
# require 'roma/messaging/con_pool'

Roma::Client::RomaClient.class_eval{
  def init_sync_routing_proc
  end
}

class MapPluginTest < Test::Unit::TestCase
  include RomaTestUtils

  def setup
    start_roma
    @rc=Roma::Client::RomaClient.new(
                                     ["localhost_11211","localhost_11212"],
                                     [::Roma::ClientPlugin::PluginMap])
  end

  def teardown
    stop_roma
    Roma::Messaging::ConPool::instance.close_all
  end

  def mk_data(n = 10)
    n.times do |i|
      v = "value#{i}"
      k = "mapkey#{i}"
      assert_equal 'STORED', @rc.map_set('key1', k, v)
      assert_equal v, @rc.map_get('key1', k)
    end    
  end

  def test_map_set
    assert_nil @rc.map_get('key1','mapkey1')
    assert_equal 'STORED', @rc.map_set('key1','mapkey1','value1')
    assert_equal 'value1', @rc.map_get('key1','mapkey1')

    assert_equal 'STORED', @rc.map_set('key1','mapkey1','value2')
    assert_equal 'value2', @rc.map_get('key1','mapkey1')
  end

  def test_map_delete
    mk_data
    assert_equal 'NOT_FOUND', @rc.map_delete('key2', 'key1')
    assert_equal 'NOT_DELETED', @rc.map_delete('key1', 'key1')
    assert_equal 'DELETED', @rc.map_delete('key1', 'mapkey1')
    assert_nil @rc.map_get('key1', 'mapkey1')
  end

  def test_map_clear
    assert_equal 'NOT_FOUND', @rc.map_empty?('key1')
    assert_equal 'NOT_FOUND', @rc.map_size('key1')
    assert_equal 'NOT_FOUND', @rc.map_clear('key1')
    mk_data
    assert !@rc.map_empty?('key1')
    assert_equal 10, @rc.map_size('key1')
    assert_equal 'CLEARED', @rc.map_clear('key1')
    assert @rc.map_empty?('key1')
    assert_equal 0, @rc.map_size('key1')
  end

  def test_map_key?
    assert_equal 'NOT_FOUND', @rc.map_key?('key1', 'key1')
    mk_data
    assert !@rc.map_key?('key1', 'key1')
    assert @rc.map_key?('key1', 'mapkey1')
  end

  def test_map_value?
    assert_equal 'NOT_FOUND', @rc.map_value?('key1', 'value1')
    mk_data
    assert !@rc.map_value?('key1', 'key1')
    assert @rc.map_value?('key1', 'value1')
  end
  
  def test_map_keys
    assert_nil @rc.map_keys('key1')
    mk_data
    v = [10]
    10.times{|i| v << "mapkey#{i}" }
    assert_equal v, @rc.map_keys('key1')
  end

  def test_map_values
    assert_nil @rc.map_values('key1')
    mk_data
    v = [10]
    10.times{|i| v << "value#{i}" }
    assert_equal v, @rc.map_values('key1')
  end

  def test_map_to_s
    assert !@rc.map_to_s('key1')
    mk_data
    h = {}
    10.times do |i|
      v = "value#{i}"
      k = "mapkey#{i}"
      h[k] = v
    end    
    assert_equal h, eval(@rc.map_to_s('key1'))
  end

end # MapPluginTest

class MapPluginTestForceForward < MapPluginTest
  def setup
    super
    @rc.rttable.instance_eval{
      undef search_node

      def search_node(key); search_node2(key); end

      def search_node2(key)
        d = Digest::SHA1.hexdigest(key).hex % @hbits
        @rd.v_idx[d & @search_mask][1]
      end
    }
  end  
end


