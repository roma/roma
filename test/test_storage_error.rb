require 'test_helper'
require 'roma/client/rclient'
require 'roma/messaging/con_pool'
require 'roma/config'
require 'pathname'

Roma::Client::RomaClient.class_eval{
  def init_sync_routing_proc
  end
}

class StorageErrorTest < Test::Unit::TestCase
  self.test_order = :defined
  include RomaTestUtils

  def setup
    base_path = Pathname(__FILE__).dirname.parent.expand_path
    `ln -s #{base_path}/test/storage_error_storage.rb #{base_path}/lib/roma/storage`
    start_roma 'config4storage_error.rb'
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
  end

  def teardown
    base_path = Pathname(__FILE__).dirname.parent.expand_path
    `rm -f #{base_path}/lib/roma/storage/storage_error_storage.rb`
    stop_roma
    Roma::Messaging::ConPool::instance.close_all
   rescue => e
    puts "#{e} #{$@}"
  end

  def test_storage_error_get
    con = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")
    con.write("get key\r\n")
    ret = con.gets
    con.close
    assert( ret.start_with? 'SERVER_ERROR' )
  end

  def test_storage_error_set
    con = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")
    con.write("set key 0 0 5\r\nvalue\r\n")
    ret = con.gets
    con.close
    assert( ret.start_with? 'SERVER_ERROR' )
  end
end

class StorageErrorTestForceForward < StorageErrorTest
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
