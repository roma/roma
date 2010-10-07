#!/usr/bin/env ruby
require 'roma/client/rclient'
require 'roma/messaging/con_pool'
require 'roma/config'

Roma::Client::RomaClient.class_eval{
  def init_sync_routing_proc
  end
}

class MHashTest < Test::Unit::TestCase
  include RomaTestUtils

  def setup
    start_roma 'config4mhash.rb'
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
  end

  def teardown
    stop_roma
    Roma::Messaging::ConPool::instance.close_all
   rescue => e
    puts "#{e} #{$@}"
  end

  def test_createhash
    con = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")
    con.write("hashlist\r\n")
    ret = con.gets
    assert_equal("roma", ret.chomp )

    con.write("createhash test\r\n")
    ret = con.gets
    assert_equal("{\"localhost_11212\"=>\"CREATED\", \"localhost_11211\"=>\"CREATED\"}", ret.chomp )

    # file check
    assert(File.directory? './localhost_11211/test')
    assert(File.directory? './localhost_11212/test')

    con.write("hashlist\r\n")
    ret = con.gets
    assert_equal("roma test", ret.chomp )

    assert_equal("STORED", @rc.set("roma","hname=roma"))
    assert_equal("hname=roma", @rc.get("roma"))
    @rc.default_hash_name='test'
    assert_nil( @rc.get("roma") )
    assert_equal("STORED", @rc.set("roma","hname=test"))
    assert_equal("hname=test", @rc.get("roma"))
    @rc.default_hash_name='roma'
    assert_equal("hname=roma", @rc.get("roma"))
    assert_equal("DELETED", @rc.delete("roma"))

    @rc.default_hash_name='not_exist_hash'
    [:get, :delete, :incr, :decr].each do |m|
      assert_raise(RuntimeError,'SERVER_ERROR not_exist_hash dose not exists.') do
        @rc.send m, "key"
      end
    end

    [:set, :add, :replace, :append, :prepend].each do |m|
      assert_raise(RuntimeError,'SERVER_ERROR not_exist_hash dose not exists.') do
        @rc.send m, "key","value"
      end
    end

    # delete hash
    con.write("deletehash test\r\n")
    ret = con.gets
    assert_equal( "{\"localhost_11212\"=>\"DELETED\", \"localhost_11211\"=>\"DELETED\"}", ret.chomp  )

    con.close

    # file check
    assert( File.directory?('./localhost_11211/test') == false)
    assert( File.directory?('./localhost_11212/test') == false)
  end

  def test_createhash2
    # for file storage
    assert_equal(Roma::Storage::TCStorage, Roma::Config::STORAGE_CLASS)

    # add 'test' hash
    con = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")
    con.write("hashlist\r\n")
    ret = con.gets
    assert_equal("roma", ret.chomp)

    con.write("createhash test\r\n")
    ret = con.gets
    assert_equal("{\"localhost_11212\"=>\"CREATED\", \"localhost_11211\"=>\"CREATED\"}", ret.chomp  )

    assert_equal("STORED", @rc.set("roma","hname=roma"))
    assert_equal("hname=roma", @rc.get("roma"))
    @rc.default_hash_name='test'
    assert_equal("STORED", @rc.set("roma","hname=test"))
    assert_equal("hname=test", @rc.get("roma"))

    # stop roam
    stop_roma

    # restart roma
    sleep 0.5
    do_command_romad 'config4mhash.rb'
    sleep 0.8

    Roma::Messaging::ConPool.instance.close_all
    Roma::Client::ConPool.instance.close_all

    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
        
    @rc.default_hash_name='test'
    con = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")
    con.write("hashlist\r\n")
    ret = con.gets

    assert_equal("hname=test", @rc.get("roma"))
  end
  
  def test_createhash3
    con = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")

    # delete hash to a nothing hash
    con.write("deletehash test\r\n")
    ret = con.gets
    assert_equal("{\"localhost_11212\"=>\"SERVER_ERROR test dose not exists.\", \"localhost_11211\"=>\"SERVER_ERROR test dose not exists.\"}", ret.chomp )
    
    # delete hash to default
    con.write("deletehash roma\r\n")
    ret = con.gets
    assert_equal("{\"localhost_11212\"=>\"SERVER_ERROR default hash can't unmount.\", \"localhost_11211\"=>\"SERVER_ERROR default hash can't unmount.\"}", ret.chomp )
  end

  def test_defhash
    con = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")
    con.write("defhash\r\n")
    ret = con.gets.chomp
    assert_equal("{\"localhost_11212\"=>\"roma\", \"localhost_11211\"=>\"roma\"}", ret) 
    
    con.write("rdefhash not_exist_hash\r\n")
    ret = con.gets.chomp
    assert_equal("CLIENT_ERROR not_exist_hash dose not find.", ret)

    con.write("createhash test\r\n")
    con.gets

    con.write("rdefhash test\r\n")
    ret = con.gets.chomp
    assert_equal("STORED", ret)
  end

  def test_mounthash
    con = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")

    # file check
    assert( File.directory?('./localhost_11211/test') == false)
    assert( File.directory?('./localhost_11212/test') == false)

    # umount
    con.write("umounthash test\r\n")
    ret = con.gets.chomp
    assert_equal("SERVER_ERROR test dose not find.", ret)

    # add 'test' hash
    con.write("createhash test\r\n")
    ret = con.gets.chomp
    assert_equal("{\"localhost_11212\"=>\"CREATED\", \"localhost_11211\"=>\"CREATED\"}", ret )

    # file check
    assert(File.directory? './localhost_11211/test')
    assert(File.directory? './localhost_11212/test')

    # umount
    con.write("umounthash test\r\n")
    ret = con.gets.chomp
    assert_equal("{\"localhost_11212\"=>\"UNMOUNTED\", \"localhost_11211\"=>\"UNMOUNTED\"}", ret)

    @rc.default_hash_name='test'
    assert_raise(RuntimeError,'SERVER_ERROR not_exist_hash dose not exists.') do
      @rc.set "key", "value"
    end

    # mount
    con.write("mounthash test\r\n")
    ret = con.gets.chomp
    assert_equal("{\"localhost_11212\"=>\"MOUNTED\", \"localhost_11211\"=>\"MOUNTED\"}", ret)

    assert_equal("STORED", @rc.set("key", "value"))

  end
end

class MHashTestForward < RClientTest
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
