#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

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

    @rc.default_hash_name='not_exist_hash' # 存在しないハッシュへのアクセス
    begin
      @rc.get("roma")
      assert(false)
    rescue =>e
      assert_equal('SERVER_ERROR not_exist_hash dose not exists.',e.message)
    end

    begin
      @rc.set("roma","hname=roma")
      assert(false)
    rescue =>e
      assert_equal('SERVER_ERROR not_exist_hash dose not exists.',e.message)
    end

    begin
      @rc.delete("roma")
      assert(false)
    rescue =>e
      assert_equal('SERVER_ERROR not_exist_hash dose not exists.',e.message)
    end

    begin
      @rc.add("add","value add")
      assert(false)
    rescue =>e
      assert_equal('SERVER_ERROR not_exist_hash dose not exists.',e.message)
    end

    begin
      @rc.replace("replace","value replace")
      assert(false)
    rescue =>e
      assert_equal('SERVER_ERROR not_exist_hash dose not exists.',e.message)
    end

    begin
      @rc.append("append","append")
      assert(false)
    rescue =>e
      assert_equal('SERVER_ERROR not_exist_hash dose not exists.', e.message)
    end

    begin
      @rc.prepend("prepend","prepend")
      assert(false)
    rescue =>e
      assert_equal('SERVER_ERROR not_exist_hash dose not exists.',e.message)
    end

    begin
      @rc.incr("incr")
      assert(false)
    rescue =>e
      assert_equal('SERVER_ERROR not_exist_hash dose not exists.',e.message)
    end

    begin
      @rc.decr("decr")
      assert(false)
    rescue =>e
      assert_equal('SERVER_ERROR not_exist_hash dose not exists.',e.message)
    end

    con.write("deletehash test\r\n")
    ret = con.gets
    assert_equal( "{\"localhost_11212\"=>\"DELETED\", \"localhost_11211\"=>\"DELETED\"}", ret.chomp  )

    con.close
  end

  def test_createhash2
    #
    # for file storage
    #
    return if Roma::Config::STORAGE_CLASS.to_s != "Roma::Storage::TCStorage"
    # test ハッシュを追加し終了する
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
    con.write("balse\r\n")
    con.gets
    con.write "yes\r\n"
    ret = con.gets
    con.close

    # 再起動
    sh = Shell.new
    sleep 0.5
    sh.system(ruby_path,romad_path,"localhost","-p","11211","-d","--verbose",
              "--disabled_cmd_protect","--config","#{server_test_dir}/config4mhash.rb")
    sh.system(ruby_path,romad_path,"localhost","-p","11212","-d","--verbose",
              "--disabled_cmd_protect","--config","#{server_test_dir}/config4mhash.rb")
    sleep 0.8
    Roma::Messaging::ConPool.instance.close_all
    Roma::Client::ConPool.instance.close_all

    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
        
    @rc.default_hash_name='test'
    con = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")
    con.write("hashlist\r\n")
    ret = con.gets

    # 停止前のデータが残っていることを確認
    assert_equal("hname=test", @rc.get("roma"))
  end
  
  def test_createhash3
    con = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")

    # 存在しないハッシュを削除
    con.write("deletehash test\r\n")
    ret = con.gets
    assert_equal("{\"localhost_11212\"=>\"SERVER_ERROR test dose not exists.\", \"localhost_11211\"=>\"SERVER_ERROR test dose not exists.\"}", ret.chomp )
    
    # デフォルトハッシュを削除
    con.write("deletehash roma\r\n")
    ret = con.gets
    assert_equal("{\"localhost_11212\"=>\"SERVER_ERROR default hash can't unmount.\", \"localhost_11211\"=>\"SERVER_ERROR default hash can't unmount.\"}", ret.chomp )
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
