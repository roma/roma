#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

require 'roma/client/rclient'
require 'roma/messaging/con_pool'
require 'roma/config'

Roma::Client::RomaClient.class_eval{
  def init_sync_routing_proc
  end
}

class RClientTest < Test::Unit::TestCase
  include RomaTestUtils

  def setup
    start_roma
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
  end

  def teardown
    stop_roma
    Roma::Messaging::ConPool::instance.close_all
   rescue => e
    puts "#{e} #{$@}"
  end

  def test_set_expt_and_get
    assert_equal("STORED", @rc.set("abc","value abc", 1))
    assert_equal("value abc", @rc.get("abc"))
    sleep 2
    assert_nil( @rc.get("abc") )
  end

  def test_set_get_delete
    @rc.delete("abc")
    assert_nil( @rc.get("abc") )
    assert_equal("STORED", @rc.set("abc","value abc"))
    assert_equal("value abc", @rc.get("abc"))
    assert_equal("STORED", @rc.set("abc","value abc")) # over write will be success
    assert_equal("DELETED", @rc.delete("abc"))
    assert_nil( @rc.get("abc"))
    assert_equal("NOT_FOUND", @rc.delete("abc"))
  end

  def test_set_get
    10.times{|i|
      s = i.to_s * 1024000
      assert_equal("STORED", @rc.set("abc", s))
      assert(s == @rc.get("abc"))
   }
  end

  def test_cas
    @rc.set("cnt", 1)
    res = @rc.cas("cnt"){|v|
      assert_equal(1, v)
      v += 1
    }
    assert_equal("STORED", res)
    assert_equal(2, @rc.get("cnt"))

    res = @rc.cas("cnt"){|v|
      res2 = @rc.cas("cnt"){|v2|
        v += 2
      }
      assert_equal("STORED", res2)
      v += 1
    }
    assert_equal("EXISTS", res)
    assert_equal(4, @rc.get("cnt"))
  end

  def test_set_gets
    keys = []
    assert_equal(@rc.gets(["key-1","key-2"]).length,0)
    10.times{|i|
      assert_equal("STORED", @rc.set("key-#{i}", "value-#{i}"))
      keys << "key-#{i}"
    }
    ret = @rc.gets(keys)
    assert_equal(ret.length,10)
    ret.each_pair{|k,v|
      assert_equal(k[-1],v[-1])
      assert_equal(k[0..3],"key-")
      assert_equal(v[0..5],"value-")
    }
    keys << "key-99"
    ret = @rc.gets(keys)
    assert_equal(ret.length,10)

    assert_equal("DELETED", @rc.delete("key-5"))
    ret = @rc.gets(keys)
    assert_equal(ret.length,9)
  end

  def test_out
    @rc.out("key-out")
    # will return NOT_DELETED
    assert_equal("NOT_DELETED", @rc.out("key-out"))
    assert_equal("STORED", @rc.set("key-out","value out"))
    assert_equal("DELETED", @rc.out("key-out"))
    assert_equal("STORED", @rc.set("key-out","value out"))
    # create a delete mark
    assert_equal("DELETED", @rc.delete("key-out"))
    # will return DELETED cause for delete mark
    assert_equal("DELETED", @rc.out("key-out"))
  end

  def test_add
    assert_nil( @rc.get("add") )
    assert_equal("STORED", @rc.add("add","value add"))
    assert_equal("NOT_STORED", @rc.add("add","value add")) # will fail
    assert_equal("DELETED", @rc.delete("add"))
    assert_equal("STORED", @rc.add("add","value add")) # will success add after delete
    assert_equal("DELETED", @rc.delete("add"))
  end

  def test_replace
    assert_nil( @rc.get("replace") )
    assert_equal("NOT_STORED", @rc.replace("replace","value replace"))
    assert_nil( @rc.get("replace") )
    assert_equal("STORED", @rc.add("replace","value add"))
    assert_equal("STORED", @rc.replace("replace","value replace"))
    assert_equal("DELETED", @rc.delete("replace"))
  end

  def test_append
    assert_nil( @rc.get("append") )
    assert_equal("NOT_STORED", @rc.append("append","append"))
    assert_equal("STORED", @rc.set("append","set",0,true))
    assert_equal("set", @rc.get("append",true))
    assert_equal("STORED", @rc.append("append","append"))
    assert_equal("setappend", @rc.get("append",true))
    assert_equal("DELETED", @rc.delete("append"))
  end

  def test_prepend
    assert_nil( @rc.get("prepend"))
    assert_equal("NOT_STORED", @rc.prepend("prepend","prepend"))
    assert_equal("STORED", @rc.set("prepend","set",0,true))
    assert_equal("set", @rc.get("prepend",true))
    assert_equal("STORED", @rc.prepend("prepend","prepend"))
    assert_equal("prependset", @rc.get("prepend",true))
    assert_equal("DELETED", @rc.delete("prepend"))
  end

  def test_incr
    assert_nil( @rc.get("incr"))
    assert_equal("NOT_FOUND", @rc.incr("incr"))
    assert_equal("STORED", @rc.set("incr","100",0,true))
    assert_equal(101, @rc.incr("incr"))
    assert_equal(102, @rc.incr("incr"))
    assert_equal("DELETED", @rc.delete("incr"))
  end

  def test_decr
    assert_nil( @rc.get("decr") )
    assert_equal("NOT_FOUND", @rc.decr("decr"))
    assert_equal("STORED", @rc.set("decr","100",0,true))
    assert_equal(99, @rc.decr("decr"))
    assert_equal(98, @rc.decr("decr"))
    assert_equal("DELETED", @rc.delete("decr"))
  end

  def test_routingdump_bin
    con = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")
    con.write("routingdump bin\r\n")
    len = con.gets
    bin = con.read(len.to_i)
    con.gets
    con.close

    magic, ver, dgst_bits, div_bits, rn, nodeslen = bin.unpack('a2nCCCn')
    assert_equal('RT', magic)
    assert_equal(1, ver)
    assert_equal(32, dgst_bits)
    assert_equal(3, div_bits)
    assert_equal(2, rn)
    assert_equal(2, nodeslen)
  end
end

class RClientTestForceForward < RClientTest
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
