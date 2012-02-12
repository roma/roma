#!/usr/bin/env ruby
require 'roma/client/rclient'
require 'roma/plugin/plugin_alist'
require 'roma/storage/tc_storage'
require 'roma/messaging/con_pool'
require 'roma/client/plugin/alist'

Roma::Client::RomaClient.class_eval{
  def init_sync_routing_proc
  end
}

class ListPluginTest < Test::Unit::TestCase
  include RomaTestUtils

  def setup
    start_roma
    @rc=Roma::Client::RomaClient.new(
                                     ["localhost_11211","localhost_11212"],
                                     [Roma::Client::Plugin::Alist])
  end

  def teardown
    stop_roma
    Roma::Messaging::ConPool::instance.close_all
  end

  def test_error_case
    @rc.set("aa","123")
    assert_raise(RuntimeError) do
      @rc.alist_to_s("aa")
    end

    assert_raise(RuntimeError) do
      @rc.alist_push("aa","123")
    end
  end

  def test_at
    @rc.alist_clear("aa")
    assert_nil( @rc.alist_at("aa",0) )
    assert_equal('STORED', @rc.alist_push("aa","11"))
    assert_equal('STORED', @rc.alist_push("aa","22"))
    assert_equal('STORED', @rc.alist_push("aa","33"))
    assert_equal('STORED', @rc.alist_push("aa","44"))

    assert_equal('11',@rc.alist_at("aa",0) )
    assert_equal('22', @rc.alist_at("aa",1) )
    assert_equal('33', @rc.alist_at("aa",2) )
    assert_equal('44', @rc.alist_at("aa",3) )
    assert_nil( @rc.alist_at("aa",4) )
  end

  def test_delete
    @rc.alist_clear("aa")
    assert_equal('STORED', @rc.alist_push("aa","11") )
    assert_equal('STORED', @rc.alist_push("aa","22") )
    assert_equal('STORED', @rc.alist_push("aa","33") )
    assert_equal('STORED', @rc.alist_push("aa","44") )

    assert_equal('NOT_FOUND', @rc.alist_delete("bb","11") )
    assert_equal('NOT_DELETED', @rc.alist_delete("aa","55") )

    assert_equal('DELETED', @rc.alist_delete("aa","33"))
    assert_equal(["11","22","44"], @rc.alist_to_s("aa")[1] )
    assert_raise RuntimeError do
      @rc.alist_delete("aa",33)
    end
  end

  def test_delete_at
    @rc.alist_clear("aa")
    assert_equal('STORED', @rc.alist_push("aa","11"))
    assert_equal('STORED', @rc.alist_push("aa","22"))
    assert_equal('STORED', @rc.alist_push("aa","33"))
    assert_equal('STORED', @rc.alist_push("aa","44"))

    assert_equal('NOT_FOUND', @rc.alist_delete_at("bb",1))
    assert_equal('NOT_DELETED',  @rc.alist_delete_at("aa",55))

    assert_equal('DELETED',  @rc.alist_delete_at("aa",2))
    assert_equal(["11","22","44"], @rc.alist_to_s("aa")[1])
  end

  def test_empty?
    @rc.delete("aa")
    assert_equal('NOT_FOUND', @rc.alist_empty?("aa"))
    assert_equal('STORED', @rc.alist_push("aa","11"))
    assert_equal('false', @rc.alist_empty?("aa"))
    @rc.alist_clear("aa")
    assert_equal('true', @rc.alist_empty?("aa"))
  end

  def test_first_last
    @rc.alist_clear("aa")

    assert_nil( @rc.alist_first("aa"))
    assert_nil( @rc.alist_first("bb"))
    assert_nil( @rc.alist_last("aa"))
    assert_nil( @rc.alist_last("bb"))

    assert_equal('STORED', @rc.alist_push("aa","11"))
    assert_equal('11', @rc.alist_first("aa"))
    assert_equal('11', @rc.alist_last("aa"))
    assert_equal('STORED', @rc.alist_push("aa","22"))
    assert_equal('STORED', @rc.alist_push("aa","33"))
    assert_equal('33', @rc.alist_last("aa"))
    assert_equal('STORED', @rc.alist_push("aa","44"))
    assert_equal('11', @rc.alist_first("aa"))
    assert_equal('44', @rc.alist_last("aa"))
  end

  def test_gets
    @rc.alist_clear("aa")
    
    assert_nil( @rc.alist_gets("aa") )
    assert( @rc.alist_push("aa","11")=='STORED' )
    assert( @rc.alist_gets("aa") == [1,"11"] )
    assert( @rc.alist_push("aa","22")=='STORED' )
    assert( @rc.alist_push("aa","33")=='STORED' )
    assert( @rc.alist_gets("aa") == [3,"11","22","33"] )
    assert( @rc.alist_push("aa","44")=='STORED' )
    assert( @rc.alist_push("aa","55")=='STORED' )
    assert( @rc.alist_gets("aa",1..3)==[5, "22", "33", "44"] )
    ret = @rc.alist_gets_with_time("aa")
    assert( ret[0]==5 )
    assert( ret.values_at(1,3,5,7,9) == ["11","22","33","44","55"] )
  end

  def test_include?
    @rc.delete("aa")

    assert_equal('NOT_FOUND', @rc.alist_include?("aa","11"))
    assert_equal('STORED', @rc.alist_push("aa","11"))
    assert_equal('true', @rc.alist_include?("aa","11"))
    assert_equal('false', @rc.alist_include?("aa","22"))
    assert_equal('STORED', @rc.alist_push("aa","22"))
    assert_equal('true', @rc.alist_include?("aa","22"))
  end

  def test_index
    @rc.delete("aa")

    assert_equal('NOT_FOUND', @rc.alist_index("aa","22"))
    assert_equal('STORED', @rc.alist_push("aa","11"))
    assert_equal(0, @rc.alist_index("aa","11") )
    assert_nil( @rc.alist_index("aa","22") )
    assert_equal('STORED', @rc.alist_push("aa","22"))
    assert_equal(0, @rc.alist_index("aa","11") )
    assert_equal(1, @rc.alist_index("aa","22") )
  end

  def test_insert
    @rc.delete("aa")

    assert_equal('STORED', @rc.alist_insert("aa",0,"11"))
    assert_equal('STORED', @rc.alist_insert("aa",0,"22"))
    assert_equal('STORED', @rc.alist_insert("aa",1,"33"))
    assert_equal(["22","33","11"],  @rc.alist_to_s("aa")[1])
  end

  def test_sized_insert
    @rc.delete("aa")

    assert_equal('STORED', @rc.alist_sized_insert("aa",5,"11"))
    assert_equal('STORED', @rc.alist_sized_insert("aa",5,"22"))
    assert_equal('STORED', @rc.alist_sized_insert("aa",5,"33"))
    assert_equal('STORED', @rc.alist_sized_insert("aa",5,"44"))
    assert_equal('STORED', @rc.alist_sized_insert("aa",5,"55"))
    assert_equal(["55","44","33","22","11"], @rc.alist_to_s("aa")[1] )
    assert_equal('STORED', @rc.alist_sized_insert("aa",5,"66"))
    assert_equal(["66","55","44","33","22"], @rc.alist_to_s("aa")[1] )
    assert_equal('STORED', @rc.alist_sized_insert("aa",5,"55"))
    assert_equal(["55","66","55","44","33"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_sized_insert("aa",3,"22"))
    assert_equal(["22","55","66"], @rc.alist_to_s("aa")[1] )
    assert_equal('STORED', @rc.alist_sized_insert("aa",6,"77"))
    assert_equal(["77","22","55","66"], @rc.alist_to_s("aa")[1] )
    assert_equal('STORED', @rc.alist_sized_insert("aa",6,"88"))
    assert_equal(["88","77","22","55","66"], @rc.alist_to_s("aa")[1] )
    assert_equal('STORED', @rc.alist_sized_insert("aa",6,"99"))
    assert_equal(["99","88","77","22","55","66"], @rc.alist_to_s("aa")[1] )
    assert_equal('STORED', @rc.alist_sized_insert("aa",6,"00"))
    assert_equal(["00","99","88","77","22","55"], @rc.alist_to_s("aa")[1] )
    assert_equal('STORED', @rc.alist_sized_insert("aa",6,"00"))
    assert_equal(["00","00","99","88","77","22"], @rc.alist_to_s("aa")[1] )
  end

  def test_sized_insert2
    @rc.delete("aa")

    100.times{|i|
      v = (i % 10).to_s * 1024
      assert_equal('STORED', @rc.alist_sized_insert("aa",50,v))
      if i+1 >= 50
        assert_equal(50, @rc.alist_length("aa"))
      else
        assert_equal(i+1, @rc.alist_length("aa"))
      end
    }
    res = @rc.alist_join("aa",",")
    assert_equal(50, res[0])
    assert_equal(50, res[1].split(",").length)
  end

  def test_swap_and_insert
    @rc.delete("aa")

    assert_equal('STORED', @rc.alist_swap_and_insert("aa","11"))
    assert_equal('STORED', @rc.alist_swap_and_insert("aa","22"))
    assert_equal('STORED', @rc.alist_swap_and_insert("aa","33"))
    assert_equal('STORED', @rc.alist_swap_and_insert("aa","44"))
    assert_equal('STORED', @rc.alist_swap_and_insert("aa","55"))
    assert_equal(["55","44","33","22","11"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_swap_and_insert("aa","66"))
    assert_equal(["66","55","44","33","22","11"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_swap_and_insert("aa","55"))
    assert_equal(["55","66","44","33","22","11"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_swap_and_insert("aa","22"))
    assert_equal(["22","55","66","44","33","11"], @rc.alist_to_s("aa")[1])
  end

  def test_swap_and_sized_insert
    @rc.delete("aa")

    assert_equal('STORED', @rc.alist_swap_and_sized_insert("aa",5,"11"))
    assert_equal('STORED', @rc.alist_swap_and_sized_insert("aa",5,"22"))
    assert_equal('STORED', @rc.alist_swap_and_sized_insert("aa",5,"33"))
    assert_equal('STORED', @rc.alist_swap_and_sized_insert("aa",5,"44"))
    assert_equal('STORED', @rc.alist_swap_and_sized_insert("aa",5,"55"))
    assert_equal(["55","44","33","22","11"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_swap_and_sized_insert("aa",5,"66"))
    assert_equal(["66","55","44","33","22"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_swap_and_sized_insert("aa",5,"55"))
    assert_equal(["55","66","44","33","22"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_swap_and_sized_insert("aa",3,"22"))
    assert_equal(["22","55","66"], @rc.alist_to_s("aa")[1])
  end

  def test_expired_swap_and_insert
    @rc.delete("aa")

    assert_equal('STORED', @rc.alist_expired_swap_and_insert("aa",5,"11"))
    assert_equal('STORED', @rc.alist_expired_swap_and_insert("aa",5,"22"))
    assert_equal('STORED', @rc.alist_expired_swap_and_insert("aa",5,"33"))
    assert_equal(["33","22","11"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_expired_swap_and_insert("aa",5,"11"))
    assert_equal(["11","33","22"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_expired_swap_and_insert("aa",5,"33"))
    assert_equal(["33","11","22"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_expired_swap_and_insert("aa",0,"44"))
    assert_equal(["44"], @rc.alist_to_s("aa")[1])
  end

  def test_expired_swap_and_sized_insert
    @rc.delete("aa")

    # for sized logic
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_insert("aa",5,10,"00"))
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_insert("aa",5,10,"11"))
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_insert("aa",5,10,"22"))
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_insert("aa",5,10,"33"))
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_insert("aa",5,10,"44"))
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_insert("aa",5,10,"55"))
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_insert("aa",5,10,"66"))
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_insert("aa",5,10,"77"))
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_insert("aa",5,10,"88"))
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_insert("aa",5,10,"99"))
    assert_equal(["99","88","77","66","55","44","33","22","11","00"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_insert("aa",5,10,"aa"))
    assert_equal(["aa","99","88","77","66","55","44","33","22","11"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_insert("aa",5,10,"bb"))
    assert_equal(["bb","aa","99","88","77","66","55","44","33","22"], @rc.alist_to_s("aa")[1])
    
    # for swaped logic
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_insert("aa",5,10,"55"))
    assert_equal(["55","bb","aa","99","88","77","66","44","33","22"], @rc.alist_to_s("aa")[1])

    # for expired logic
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_insert("aa",0,10,"44"))
    assert_equal(["44"], @rc.alist_to_s("aa")[1])
  end

  def test_join
    @rc.delete("aa")

    assert( @rc.alist_join("aa","|")==nil )
    t = Time.now.to_i
    assert( @rc.alist_push("aa","11")=='STORED' )
    assert( @rc.alist_join("aa","|")[1]=="11" )
    # get a time of insert, maybe in 1 sec
    assert_operator(1,:>,t - @rc.alist_join_with_time("aa","|")[2].to_i)
    assert_equal('STORED', @rc.alist_push("aa","22"))
    assert_equal("11|22", @rc.alist_join("aa","|")[1])
    assert_equal('STORED', @rc.alist_push("aa","33"))
    assert_equal('STORED', @rc.alist_push("aa","44"))
    assert_equal('STORED', @rc.alist_push("aa","55"))
    assert_equal('STORED', @rc.alist_push("aa","66"))
    assert_equal('STORED', @rc.alist_push("aa","77"))
    ret = @rc.alist_join_with_time("aa","|")
    assert_equal(7,ret[0] )
    assert_equal(7, ret[1].split('|').length )
    assert_equal(7, ret[2].split('|').length )
    assert_nil( @rc.alist_join("aa","|",10) ) # out of index
    assert_equal("66", @rc.alist_join("aa","|",5)[1])
    assert_equal(7, @rc.alist_join("aa","|",1..4)[0] )
    assert_equal("22|33|44|55", @rc.alist_join("aa","|",1..4)[1])
    assert_equal("22|33|44|55|66|77", @rc.alist_join("aa","|",1..10)[1])
    assert_equal("22|33|44|55|66|77", @rc.alist_join("aa","|",1..-1)[1])
    ret = @rc.alist_join_with_time("aa","|",1..4)
    assert_equal(7, ret[0] )
    assert_equal("22|33|44|55", ret[1])
    assert_equal(4, ret[2].split('|').length )
  end

  def test_json
    @rc.delete("aa")

    assert_nil( @rc.alist_to_json("aa") )
    assert_equal('STORED', @rc.alist_push("aa","11"))
    assert_equal('["11"]', @rc.alist_to_json("aa"))
    assert_equal('STORED', @rc.alist_push("aa","22"))
    assert_equal('["11","22"]', @rc.alist_to_json("aa"))
    assert_equal('STORED', @rc.alist_push("aa","33"))
    assert_equal('STORED', @rc.alist_push("aa","44"))
    assert_equal('STORED', @rc.alist_push("aa","55"))
    assert_equal('STORED', @rc.alist_push("aa","66"))
    assert_equal('STORED', @rc.alist_push("aa","77"))
  
    assert_nil( @rc.alist_to_json("aa",10) )
    assert_equal('["66"]', @rc.alist_to_json("aa",5))
    assert_equal('["22","33","44","55"]', @rc.alist_to_json("aa",1..4))
    assert_equal('["22","33","44","55","66","77"]', @rc.alist_to_json("aa",1..10))
    assert_equal('["22","33","44","55","66","77"]', @rc.alist_to_json("aa",1..-1))
  end

  def test_length
    @rc.delete("aa")

    assert_equal('NOT_FOUND', @rc.alist_length("aa"))
    assert_equal('STORED', @rc.alist_push("aa","11"))
    assert_equal(1, @rc.alist_length("aa") )
    assert_equal('STORED',  @rc.alist_push("aa","22"))
    assert_equal('STORED',  @rc.alist_push("aa","33"))
    assert_equal('STORED',  @rc.alist_push("aa","44"))
    assert_equal('STORED',  @rc.alist_push("aa","55"))
    assert_equal(5, @rc.alist_length("aa") )
    @rc.alist_clear("aa")
    assert_equal(0, @rc.alist_length("aa") )
  end

  def test_pop
    @rc.delete("aa")

    assert_nil( @rc.alist_pop("aa") )
    assert_equal('STORED', @rc.alist_push("aa","11"))
    assert_equal(1, @rc.alist_length("aa") )
    assert_equal('11', @rc.alist_pop("aa") )
    assert_equal(0, @rc.alist_length("aa") )
    assert_equal('STORED', @rc.alist_push("aa","11"))
    assert_equal('STORED', @rc.alist_push("aa","22"))
    assert_equal('STORED', @rc.alist_push("aa","33"))
    assert_equal('STORED', @rc.alist_push("aa","44"))
    assert_equal(4, @rc.alist_length("aa"))
    assert_equal('44', @rc.alist_pop("aa"))
    assert_equal(3, @rc.alist_length("aa"))
    assert_equal('33', @rc.alist_pop("aa"))
    assert_equal('22', @rc.alist_pop("aa"))
    assert_equal('11', @rc.alist_pop("aa"))
    assert_equal(0, @rc.alist_length("aa"))
  end

  def test_push
    @rc.alist_clear("aa")
    assert_equal('STORED', @rc.alist_push("aa","11"))
    res = @rc.alist_to_s("aa")
    assert_equal(2, res.length )
    assert_equal(1, res[0] )
    assert_equal('11', res[1][0] )
    
    assert_equal('STORED', @rc.alist_push("aa","22"))
    assert_equal('STORED', @rc.alist_push("aa","33"))
    assert_equal('STORED', @rc.alist_push("aa","44"))

    res = @rc.alist_to_s("aa")
    assert_equal(4, res[0])
    assert_equal('11', res[1][0])
    assert_equal('22', res[1][1])
    assert_equal('33', res[1][2])
    assert_equal('44', res[1][3])
  end

  def test_sized_push
    @rc.alist_clear("aa")
    assert_equal('STORED', @rc.alist_sized_push("aa",5,"11"))
    assert_equal('STORED', @rc.alist_sized_push("aa",5,"22"))
    assert_equal('STORED', @rc.alist_sized_push("aa",5,"33"))
    assert_equal('STORED', @rc.alist_sized_push("aa",5,"44"))
    assert_equal('STORED', @rc.alist_sized_push("aa",5,"55"))
    assert_equal('NOT_PUSHED', @rc.alist_sized_push("aa",5,"66"))
    assert_equal('NOT_PUSHED', @rc.alist_sized_push("aa",5,"77"))
    assert_equal(5, @rc.alist_to_s("aa")[0])
  end

  def test_swap_and_push
    @rc.alist_clear("aa")
    assert_equal('STORED', @rc.alist_swap_and_push("aa","11"))
    assert_equal(["11"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_swap_and_push("aa","22"))
    assert_equal(["11", "22"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_swap_and_push("aa","33"))
    assert_equal(["11", "22", "33"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_swap_and_push("aa","11"))
    assert_equal(["22", "33", "11"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_swap_and_push("aa","44"))
    assert_equal(["22", "33", "11", "44"], @rc.alist_to_s("aa")[1])
  end

  def test_swap_and_sized_push
    @rc.alist_clear("aa")
    assert_equal('STORED', @rc.alist_swap_and_sized_push("aa",5,"11"))
    assert_equal('STORED', @rc.alist_swap_and_sized_push("aa",5,"22"))
    assert_equal('STORED', @rc.alist_swap_and_sized_push("aa",5,"33"))
    assert_equal(["11","22","33"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_swap_and_sized_push("aa",5,"11"))
    assert_equal(["22","33","11"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_swap_and_sized_push("aa",5,"44"))
    assert_equal(["22","33","11","44"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_swap_and_sized_push("aa",5,"55"))
    assert_equal(["22","33","11","44","55"], @rc.alist_to_s("aa")[1])
    assert_equal('NOT_PUSHED', @rc.alist_swap_and_sized_push("aa",5,"66"))
    assert_equal(["22","33","11","44","55"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_swap_and_sized_push("aa",5,"22"))
    assert_equal(["33","11","44","55","22"], @rc.alist_to_s("aa")[1])
  end

  def test_expired_swap_and_push
    @rc.delete("aa")

    assert_equal('STORED', @rc.alist_expired_swap_and_push("aa",5,"11"))
    assert_equal('STORED', @rc.alist_expired_swap_and_push("aa",5,"22"))
    assert_equal('STORED', @rc.alist_expired_swap_and_push("aa",5,"33"))
    assert_equal(["11","22","33"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_expired_swap_and_push("aa",5,"11"))
    assert_equal(["22","33","11"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_expired_swap_and_push("aa",5,"33"))
    assert_equal(["22","11","33"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_expired_swap_and_push("aa",0,"44"))
    assert_equal(["44"], @rc.alist_to_s("aa")[1])
  end

  def test_expired_swap_and_sized_push
    @rc.delete("aa")

    # for sized logic
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_push("aa",5,10,"00"))
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_push("aa",5,10,"11"))
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_push("aa",5,10,"22"))
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_push("aa",5,10,"33"))
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_push("aa",5,10,"44"))
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_push("aa",5,10,"55"))
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_push("aa",5,10,"66"))
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_push("aa",5,10,"77"))
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_push("aa",5,10,"88"))
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_push("aa",5,10,"99"))
    assert_equal(["00","11","22","33","44","55","66","77","88","99"], @rc.alist_to_s("aa")[1])
    assert_equal('NOT_PUSHED', @rc.alist_expired_swap_and_sized_push("aa",5,10,"aa"))
    assert_equal(["00","11","22","33","44","55","66","77","88","99"], @rc.alist_to_s("aa")[1])
    assert_equal('NOT_PUSHED', @rc.alist_expired_swap_and_sized_push("aa",5,10,"bb"))
    assert_equal(["00","11","22","33","44","55","66","77","88","99"], @rc.alist_to_s("aa")[1])
      
    # for swaped logic
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_push("aa",5,10,"55"))
    assert_equal(["00","11","22","33","44","66","77","88","99","55"], @rc.alist_to_s("aa")[1])

    # for expired logic
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_push("aa",0,10,"44"))
    assert_equal(["44"], @rc.alist_to_s("aa")[1])
  end

  def test_alist_update_at
    @rc.delete("aa")

    assert_equal('NOT_FOUND', @rc.alist_update_at("aa",0,"a0"))

    assert_equal('STORED', @rc.alist_push("aa","00"))
    assert_equal('STORED', @rc.alist_push("aa","11"))
    assert_equal('STORED', @rc.alist_push("aa","22"))
    assert_equal('STORED', @rc.alist_push("aa","33"))
    assert_equal('STORED', @rc.alist_push("aa","44"))
    
    assert_equal(["00","11","22","33","44"], @rc.alist_to_s("aa")[1])

    assert_equal('NOT_FOUND', @rc.alist_update_at("aa",-1,"a0"))
    assert_equal('NOT_FOUND', @rc.alist_update_at("aa",5,"a0"))

    assert_equal('STORED', @rc.alist_update_at("aa",2,"a2"))
    assert_equal(["00","11","a2","33","44"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_update_at("aa",0,"a0"))
    assert_equal(["a0","11","a2","33","44"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_update_at("aa",1,"a1"))
    assert_equal(["a0","a1","a2","33","44"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_update_at("aa",3,"a3"))
    assert_equal(["a0","a1","a2","a3","44"], @rc.alist_to_s("aa")[1])
    assert_equal('STORED', @rc.alist_update_at("aa",4,"a4"))
    assert_equal(["a0","a1","a2","a3","a4"], @rc.alist_to_s("aa")[1])
  end

  def test_shift
    @rc.delete("aa")

    assert_nil( @rc.alist_shift("aa"))
    assert_equal('STORED', @rc.alist_push("aa","11"))
    assert_equal(1, @rc.alist_length("aa") )
    assert_equal('11', @rc.alist_shift("aa") )
    assert_equal(0, @rc.alist_length("aa") )
    assert_equal('STORED', @rc.alist_push("aa","11"))
    assert_equal('STORED', @rc.alist_push("aa","22"))
    assert_equal('STORED', @rc.alist_push("aa","33"))
    assert_equal('STORED', @rc.alist_push("aa","44"))
    assert_equal(4, @rc.alist_length("aa") )
    assert_equal('11', @rc.alist_shift("aa") )
    assert_equal(3, @rc.alist_length("aa") )
    assert_equal('22', @rc.alist_shift("aa") )
    assert_equal('33', @rc.alist_shift("aa") )
    assert_equal('44', @rc.alist_shift("aa") )
    assert_equal(0, @rc.alist_length("aa") )
  end

  def test_to_s
    @rc.delete("aa")

    assert_nil( @rc.alist_to_s("aa") )
    assert_equal('STORED', @rc.alist_push("aa","11"))
    assert( @rc.alist_to_s("aa")[1]==['11'] )
    @rc.alist_clear("aa")
    assert_equal(0, @rc.alist_to_s("aa")[0] )
    assert_equal('STORED',  @rc.alist_push("aa","11"))
    assert_equal('STORED',  @rc.alist_push("aa","22"))
    assert_equal('STORED',  @rc.alist_push("aa","33"))
    assert_equal('STORED',  @rc.alist_push("aa","44"))
    assert_equal(["11", "22", "33", "44"], @rc.alist_to_s("aa")[1])
    assert_equal(['11'], @rc.alist_to_s("aa",0)[1] )
    assert_nil( @rc.alist_to_s("aa",10)[1] )
    assert_equal(["22", "33"], @rc.alist_to_s("aa",1..2)[1])
    assert_equal(["11", "22", "33", "44"], @rc.alist_to_s("aa",0..3)[1])
    assert_equal(["11", "22", "33", "44"], @rc.alist_to_s("aa",0..-1)[1])
    assert_equal(["11", "22", "33", "44"], @rc.alist_to_s("aa",0..10)[1])
  end

  def test_alist_spushv

    st, nid, vn = create_storage_and_calc_vn('aa')

    # create a data ,it's a past time
    pt =Time.now.to_i
    st.set(vn,'aa',0,0xffffffff,Marshal.dump([['11','22','33','44','55'],[pt,pt,pt,pt,pt]]))

    @rc.delete("aa")
    @rc.alist_push("aa","55")
    @rc.alist_push("aa","33")
    @rc.alist_push("aa","11")

    push_a_vnode_stream(st, vn, nid)
    
    assert_equal(["55", "33", "11", "22", "44"], @rc.alist_to_s("aa")[1])
    
    # create a data out of list
    st.set(vn,'aa',0,0xffffffff,'val-aa')
    push_a_vnode_stream(st, vn, nid)
    # do not write a value
    assert_equal(["55", "33", "11", "22", "44"], @rc.alist_to_s("aa")[1])

    # increases to logical clock
    10.times{
      st.set(vn,'aa',0,0xffffffff,'val-aa')
    }
    push_a_vnode_stream(st, vn, nid)
    # write over a value, cause increased a logical clock
    assert_equal('val-aa', @rc.get("aa",true))
  end

  def create_storage_and_calc_vn(k)
    # calc vn
    nid,d = @rc.rttable.search_node("aa")
    vn = @rc.rttable.get_vnode_id(d)

    st = Roma::Storage::TCMemStorage.new
    st.vn_list = [vn]
    st.storage_path = 'storage_test'
    st.opendb

    [st,nid,vn]
  end
  private :create_storage_and_calc_vn

  def push_a_vnode_stream(st, vn, nid)
    con = Roma::Messaging::ConPool.instance.get_connection(nid)
    con.write("alist_spushv roma #{vn}\r\n")
    res = con.gets
    st.each_vn_dump(vn){|data|
      con.write(data)
    }
    con.write("\0"*20) # end of steram
    res = con.gets # STORED\r\n or error string
    Roma::Messaging::ConPool.instance.return_connection(nid,con)    
  end
  private :push_a_vnode_stream

end # ListPluginTest

class ListPluginTestForceForward < ListPluginTest
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

  undef test_alist_spushv
end
