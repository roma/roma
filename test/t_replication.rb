#!/usr/bin/env ruby
require 'logger'
require 'roma/write_behind'
require 'roma/client/rclient'
require 'roma/messaging/con_pool'

module StreamWriterTests
  def setup
    start_roma
    @rc = Roma::Client::RomaClient.new(%w(localhost_11211 localhost_11212))

    start_roma_replica
    @rc_replica = Roma::Client::RomaClient.new(%w(localhost_21211 localhost_21212))
  end

  def teardown
    stop_roma
    stop_roma_replica
    Roma::Messaging::ConPool.instance.close_all
  rescue => e
    puts "#{e} #{$ERROR_POSITION}"
  end

  def send_cmd(host, cmd)
    con = Roma::Messaging::ConPool.instance.get_connection(host)
    con.write("#{cmd}\r\n")
    ret = con.gets
    con.close
    ret
  end
end # end of StreamWriterTests module


class StreamWriterTest < Test::Unit::TestCase
  include StreamWriterTests
  include RomaTestUtils

   def test_sw_get_routing_data
     sw = Roma::WriteBehind::StreamWriter.new(Logger.new(nil))

     sw.update_mklhash('localhost_21211')
     pre_mklhash = sw.replica_mklhash
     assert_match(/[\d\w]{40}/, pre_mklhash)

     sw.update_nodelist('localhost_21211')
     pre_nodelist = sw.replica_nodelist
     assert_equal(['localhost_21211', 'localhost_21212'], pre_nodelist)

     sw.update_rttable('localhost_21211')
     pre_rttable = sw.replica_rttable
     assert_kind_of(Roma::Routing::RoutingData, pre_rttable)
   end

   def test_sw_change_mkl_hash?
     sw = Roma::WriteBehind::StreamWriter.new(Logger.new(nil))
     sw.replica_mklhash = 'dummy'
     assert_equal(true, sw.change_mklhash?)
   end


end #  end of StreamWriterTest class

class ClusterReplicationTest < Test::Unit::TestCase
  include StreamWriterTests
  include RomaTestUtils

  def test_rc_status
    ret = send_cmd('localhost_11211', 'stat run_replication')
    assert_equal("write-behind.run_replication false\r\n", ret)

    ret = send_cmd('localhost_11211', 'stat replica_mklhash')
    assert_equal("write-behind.replica_mklhash \r\n", ret)

    ret = send_cmd('localhost_11211', 'stat replica_nodelist')
    assert_equal("write-behind.replica_nodelist []\r\n", ret)
  end

  def test_rc_switch_replication_default
    ret = send_cmd('localhost_11211', 'switch_replication true localhost_21211')
    assert_equal("{\"localhost_11212\"=>\"ACTIVATED\", \"localhost_11211\"=>\"ACTIVATED\"}\r\n", ret)
    ret = send_cmd('localhost_11211', 'stat run_replication')
    assert_equal("write-behind.run_replication true\r\n", ret)
    pre_mklhash = send_cmd('localhost_11211', 'stat replica_mklhash')
    assert_match(/write-behind\.replica_mklhash [\d\w]{40}\r\n/, pre_mklhash)
    ret = send_cmd('localhost_11211', 'stat replica_nodelist')
    assert_equal("write-behind.replica_nodelist [\"localhost_21211\", \"localhost_21212\"]\r\n", ret)

    ret = send_cmd('localhost_11211', 'switch_replication false')
    assert_equal("{\"localhost_11212\"=>\"DEACTIVATED\", \"localhost_11211\"=>\"DEACTIVATED\"}\r\n", ret)
    ret = send_cmd('localhost_11211', 'stat run_replication')
    assert_equal("write-behind.run_replication false\r\n", ret)
    post_mklhash = send_cmd('localhost_11211', 'stat replica_mklhash')
    assert_match("write-behind\.replica_mklhash \r\n", post_mklhash)
    assert_not_equal(pre_mklhash, post_mklhash)
    ret = send_cmd('localhost_11211', 'stat replica_nodelist')
    assert_equal("write-behind.replica_nodelist []\r\n", ret)
  end

  def test_rc_switch_replication_error
    ret = send_cmd('localhost_11211', 'switch_replication true localhost_21211 localhost_21212')
    assert_equal("CLIENT_ERROR number of arguments\r\n", ret)
    ret = send_cmd('localhost_11211', 'stat run_replication')
    assert_equal("write-behind.run_replication false\r\n", ret)

    ret = send_cmd('localhost_11211', 'switch_replication on localhost_21211')
    assert_equal("CLIENT_ERROR value must be true or false\r\n", ret)
    ret = send_cmd('localhost_11211', 'stat run_replication')
    assert_equal("write-behind.run_replication false\r\n", ret)
  end

  def test_rc_store_cmd  # get/set/add/replace/append/prepend
    send_cmd('localhost_11211', 'switch_replication true localhost_21211')

    # set/get
    @rc.set('key1', 'val1')
    assert_equal('val1', @rc_replica.get('key1'))

    # add
    @rc.add('key2', 'val2')
    assert_equal('val2', @rc_replica.get('key2'))
    @rc.add("key2", "val3")
    assert_equal('val2', @rc_replica.get('key2'))

    # replace
    @rc.set('key3', 'val3')
    @rc.replace('key3', 'val4')
    assert_equal('val4', @rc_replica.get('key3'))
    @rc.replace('key4', 'val4')
    assert_nil( @rc_replica.get('key4'))
   
    # append
    @rc.set('key5','value5', 0, true)
    assert_equal('value5', @rc_replica.get('key5', true))
    @rc.append("key5","_end")
    assert_equal("value5_end", @rc_replica.get('key5',true))

    # prepend
    @rc.set('key6','value6', 0, true)
    assert_equal('value6', @rc_replica.get('key6', true))
    @rc.prepend("key6","start_")
    assert_equal("start_value6", @rc_replica.get('key6',true))
  end

  def test_rc_other_store_cmd # delete/incr/decr/set_expt/cas
    send_cmd('localhost_11211', 'switch_replication true localhost_21211')

    # delete
    @rc.set('key1', 'val1')
    assert_equal('val1', @rc_replica.get('key1'))
    @rc.delete('key1')
    assert_nil(@rc_replica.get('key1'))

    # incr
    @rc.set('key2','100',0,true)
    assert_equal('100', @rc_replica.get('key2', true))
    @rc.incr('key2')
    assert_equal('101', @rc_replica.get('key2', true))
    @rc.incr('key2', 10)
    assert_equal('111', @rc_replica.get('key2', true))

    # decr
    @rc.set('key3','100', 0, true)
    assert_equal('100', @rc_replica.get('key3', true))

    @rc.decr('key3')
    assert_equal('99', @rc_replica.get('key3', true))

    @rc.decr('key3', 10)
    assert_equal('89', @rc_replica.get('key3', true))

    # expt
    @rc.set('key4', 'val4', 1)
    assert_equal('val4', @rc_replica.get('key4'))
    sleep 2
    assert_nil( @rc_replica.get('key4') )

    # set_expt
    @rc.set('key5', 'val5')
    assert_equal('val5', @rc_replica.get('key5'))
    send_cmd('localhost_11211', 'set_expt key5 1')
    sleep 2
    assert_nil( @rc_replica.get('key5') )

    # cas
    @rc.set("cnt", 1)
    res = @rc.cas("cnt"){|v|
      assert_equal(1, v)
      v += 1
    }
    assert_equal(2, @rc_replica.get("cnt"))

    res = @rc.cas("cnt"){|v|
      res2 = @rc.cas("cnt"){|v2|
        v += 2
      }
      assert_equal("STORED", res2)
      v += 1
    }
    assert_equal("EXISTS", res)
    assert_equal(4, @rc_replica.get("cnt"))
  end

end #  end of ClusterReplicationTest class
