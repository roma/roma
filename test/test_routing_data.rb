require 'test_helper'

require 'roma/routing/routing_data'
require 'yaml'

class RoutingDataTest < Test::Unit::TestCase
  self.test_order = :defined
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

  def test_saved_format
    rd=Roma::Routing::RoutingData.create(32,8,1,['roma0_3300'])
    decoded_rd=Roma::Routing::RoutingData.decode_binary(rd.dump_binary)
    decoded_rd.save("routing_data.test.route")
    rd_text=File.open("routing_data.test.route"){|f| f.read }
    assert( /\!binary \|\-/ !~ rd_text )
    File::unlink("routing_data.test.route")
  end

  def test_create_nodes_from_v_idx
    rd=Roma::Routing::RoutingData.create(32,8,1,['roma0','roma1','roma2'])
    rd.nodes.clear
    rd.create_nodes_from_v_idx
    assert( rd.nodes == ['roma0','roma1','roma2'] )
  end

  def test_create
    # digest bit count 32
    # vn bit count 8
    # redundancy 1
    # array of node ID [roma0_3300]
    rd=Roma::Routing::RoutingData.create(32,8,1,['roma0_3300'])

    assert( rd.v_idx.length==256 )
    assert( rd.nodes.length==1 )
    assert( rd.search_mask==0xff000000 )
    assert( rd.dgst_bits==32 )
    assert( rd.div_bits==8 )
    assert( rd.rn==1 )

    # digest bit count 32
    # vn bit count 9
    # redundancy 2
    # array of node ID ['roma0_3300','roma1_3300','roma2_3300']
    rd=Roma::Routing::RoutingData.create(32,9,2,['roma0_3300','roma1_3300','roma2_3300'])

    assert( rd.v_idx.length==512 )
    assert( rd.nodes.length==3 )
    assert( rd.search_mask==4286578688 )
    assert( rd.dgst_bits==32 )
    assert( rd.div_bits==9 )
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
    # confirming dispersion is lower than 10%
    assert( (c0-c1).abs < rd.v_idx.length/10 )
    assert( (c1-c2).abs < rd.v_idx.length/10 )
  end

  def test_dump_binary
    rd=Roma::Routing::RoutingData.create(32,9,2,['roma0_3300','roma1_3300'])
    # set to a bummy clock
    (2**rd.div_bits).times{|i|
      vn=i<<(rd.dgst_bits-rd.div_bits)
      rd.v_clk[vn] = i
    }

    bin = rd.dump_binary
#    puts bin.length

    magic, ver, dgst_bits, div_bits, rn, nodeslen = bin.unpack('a2nCCCn')
    assert_equal('RT', magic)
    assert_equal(1, ver)
    assert_equal(rd.dgst_bits, dgst_bits)
    assert_equal(rd.div_bits, div_bits)
    assert_equal(rd.rn, rn)
    assert_equal(rd.nodes.length, nodeslen)
    bin = bin[9..-1]
    nodeslen.times{|i|
      len, = bin.unpack('n')
      bin = bin[2..-1]
      nid, = bin.unpack("a#{len}")
      bin = bin[len..-1]
      assert_equal(rd.nodes[i], nid)
    }
    (2**div_bits).times{|i|
      vn=i<<(dgst_bits-div_bits)
      v_clk,len = bin.unpack('Nc')
      assert_equal(i, rd.v_clk[vn])
      assert_equal(rd.v_idx[vn].length, len)
#      puts "#{i} #{vn} #{v_clk} #{len}"
      bin = bin[5..-1]
      len.times{|i|
        idx, = bin.unpack('n')
        assert_equal(rd.nodes[idx], rd.v_idx[vn][i])
        bin = bin[2..-1]
#        puts rd.nodes[idx]
      }
    }
    assert_equal(0, bin.length)
  end

  def test_dump_binary2
    rd=Roma::Routing::RoutingData.create(32,9,2,['roma0_3300','roma1_3300'])
    # set to a bummy clock
    (2**rd.div_bits).times{|i|
      vn=i<<(rd.dgst_bits-rd.div_bits)
      rd.v_clk[vn] = i
    }

    bin = rd.dump_binary
    bin2 = Roma::Routing::RoutingData.decode_binary(bin).dump_binary

    assert_equal(bin, bin2)
  end
end
