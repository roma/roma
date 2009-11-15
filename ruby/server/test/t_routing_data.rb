#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

require 'roma/routing/routing_data'
require 'yaml'

class RoutingDataTest < Test::Unit::TestCase
  def setup
  end

  def teardown
  end

  def cnt_obj
    GC.start
    n=0
    ObjectSpace.each_object{|o| n+=1 }
    n    
  end

  def test_object_count
    c1=cnt_obj
    rd=Roma::Routing::RoutingData.create(32,9,2,['roma0_11211','roma0_11212'])
    c2=cnt_obj
    puts
    puts "RoutingData.create #{c2-c1} objects"
    c1=c2
    rd.save("routing_data.test.route")
    rd2=Roma::Routing::RoutingData.load("routing_data.test.route")
    c2=cnt_obj
    puts "RoutingData.load #{c2-c1} objects"
    File::unlink("routing_data.test.route")
  end

  def test_save_load
    rd=Roma::Routing::RoutingData.create(32,8,1,['roma0_3300'])
    rd.save("routing_data.test.route")
    rd2=Roma::Routing::RoutingData.load("routing_data.test.route")
    assert( YAML.dump(rd) == YAML.dump(rd2) )
    File::unlink("routing_data.test.route")
  end

  def test_next_vnode
    rd=Roma::Routing::RoutingData.create(32,8,1,['roma0_3300'])
    assert( 0x01000000 == rd.next_vnode(0x00000000) )
    assert( 0x00000000 == rd.next_vnode(0xff000000) )
    assert( 0x56000000 == rd.next_vnode(0x55000000) )
  end

  def test_create_nodes_from_v_idx
    rd=Roma::Routing::RoutingData.create(32,8,1,['roma0','roma1','roma2'])
    rd.nodes.clear
    rd.create_nodes_from_v_idx
    assert( rd.nodes == ['roma0','roma1','roma2'] )
  end

  def test_create
    # ダイジェストの総ビット数 32
    # バーチャルノードのビット数 8
    # 冗長度 1
    # ノードIDの配列 [roma0_3300]
    rd=Roma::Routing::RoutingData.create(32,8,1,['roma0_3300'])
    
    assert( rd.v_idx.length==256 )
    assert( rd.nodes.length==1 )
    assert( rd.search_mask==0xff000000 )
    assert( rd.dgst_bits==32 )
    assert( rd.div_bits==8 )
    assert( rd.rn==1 )

    # ダイジェストの総ビット数 32
    # バーチャルノードのビット数 16
    # 冗長度 2
    # ノードIDの配列 ['roma0_3300','roma1_3300','roma2_3300']
    rd=Roma::Routing::RoutingData.create(32,16,2,['roma0_3300','roma1_3300','roma2_3300'])

    assert( rd.v_idx.length==65536 )
    assert( rd.nodes.length==3 )
    assert( rd.search_mask==0xffff0000 )
    assert( rd.dgst_bits==32 )
    assert( rd.div_bits==16 )
    assert( rd.rn==2 )

    c0=c1=c2=0
    rd.v_idx.each_value{|v|
      case v[0]
      when 'roma0_3300'
        c0+=1
      when 'roma1_3300'
        c1+=1
      when 'roma2_3300'
        c2+=1
      end
    }
    # バラつきは10%より小さいでしょ
    assert( (c0-c1).abs < rd.v_idx.length/10 )
    assert( (c1-c2).abs < rd.v_idx.length/10 )
  end

end
