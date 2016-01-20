#!/usr/bin/env ruby

require 'roma/routing/routing_data'
require 'yaml'

class RoutingDataTest < Test::Unit::TestCase
  self.test_order = :defined
  def setup
  end

  def teardown
  end

  def test_make_rich_routing
    # digest bit count 32
    # vn bit count 16
    # redundancy 2
    # array of node ID ['roma0_3300','roma1_3300','roma2_3300']
    rd=Roma::Routing::RoutingData.create(32,16,2,['roma0_3300','roma1_3300','roma2_3300']) # vn bit count 16

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
    # confirming dispersion is lower than 10%
    assert( (c0-c1).abs < rd.v_idx.length/10 )
    assert( (c1-c2).abs < rd.v_idx.length/10 )
  end

end
