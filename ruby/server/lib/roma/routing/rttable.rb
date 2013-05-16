# -*- coding: utf-8 -*-
require 'roma/logging/rlogger'
require 'roma/routing/routing_data'
require 'roma/routing/merkle_tree'
require 'yaml'
require 'json'

module Roma
  module Routing

    class RoutingTable

      attr :rd
      attr :search_mask
      attr :fail_cnt
      attr :mtree
      attr_reader :hbits
      attr_reader :rn
      attr_reader :div_bits
      attr_accessor :fail_cnt_threshold
      attr_accessor :fail_cnt_gap
      attr_accessor :sub_nid

      def initialize(rd)
        @log = Roma::Logging::RLogger.instance
        @rd = rd
        @rn = @rd.rn
        @div_bits=@rd.div_bits
        @hbits = 2**@rd.dgst_bits
        @search_mask = @rd.search_mask
        @fail_cnt = Hash.new(0)
        @fail_cnt_threshold = 5
        @fail_cnt_gap = 0
        @fail_time = Time.now
        @sub_nid = {}
        init_mtree
      end

      def get_stat(ap)

        pn = sn = short = lost = 0
        @rd.v_idx.each_pair{|vn, nids|
          if nids == nil || nids.length == 0
            lost += 1
            next
          elsif nids[0] == ap
            pn += 1
          elsif nids.include?(ap)
            sn += 1
          end
          short += 1 if nids.length < @rd.rn 
        }

        ret = {}
        ret['routing.redundant'] = @rn
        ret['routing.nodes.length'] = nodes.length
        ret['routing.nodes'] = nodes.inspect
        ret['routing.dgst_bits'] = @rd.dgst_bits
        ret['routing.div_bits'] = @div_bits
        ret['routing.vnodes.length'] = vnodes.length
        ret['routing.primary'] = pn
        ret['routing.secondary'] = sn
        ret['routing.short_vnodes'] = short
        ret['routing.lost_vnodes'] = lost
        ret['routing.fail_cnt_threshold'] = @fail_cnt_threshold
        ret['routing.fail_cnt_gap'] = @fail_cnt_gap
        ret['routing.sub_nid'] = @sub_nid.inspect
        ret
      end

      def init_mtree
        @mtree = MerkleTree.new(@rd.dgst_bits,@rd.div_bits)
        @rd.v_idx.each_pair{ |vn, nids|
          @mtree.set(vn,nids)
        }
      end

      def nodes
        @rd.nodes.clone
      end

      def vnodes
        @rd.v_idx.keys
      end

      # ハッシュ値からvnode idを返す
      def get_vnode_id(d)
        d & @search_mask
      end

      # vnode があるノードIDの配列を返す
      # +vn+: vnode id
      def search_nodes(vn)
        @rd.v_idx[vn].clone
      rescue
        nil
      end

      # 離脱ノードを検索リストから削除する
      # +nid+: 離脱ノード
      def leave(nid)
        @rd.nodes.delete(nid)
        # リストから nid を消す
        @rd.v_idx.each_pair{ |vn, nids|
          nids.delete_if{ |nid2| nid2 == nid}
          if nids.length == 0
            @log.error("Vnode data is lost.(Vnode=#{vn})")
          end
          @mtree.set(vn,nids)
        }
        @fail_cnt.delete(nid)
      end

      def dump
        Marshal.dump(@rd)
      end

      def dump_yaml
        YAML.dump(@rd)
      end

      def dump_json
        JSON.generate(
                      [{:dgst_bits=>@rd.dgst_bits,:div_bits=>@rd.div_bits,:rn=>@rd.rn},
                       @rd.nodes,@rd.v_idx])
      end

      def dump_binary
        @rd.dump_binary
      end

      def proc_failed(nid)
        t = Time.now
        if t - @fail_time > @fail_cnt_gap
          @fail_cnt[nid] += 1
          if @fail_cnt[nid] >= @fail_cnt_threshold
            leave(nid)
          end
        end
        @fail_time = t
      end

      def proc_succeed(nid)
        @fail_cnt.delete(nid)
      end

      # v_idx から nodes を再構築する
      def create_nodes_from_v_idx
        @rd.create_nodes_from_v_idx
      end

      # Returns a new RoutingData object which replaced host name by the sub_nid attribute.
      def sub_nid_rd(addr)
        sub_nid.each do |mask, sub|
          if check_netmask?(addr, mask)
            return get_replaced_rd(sub[:regexp], sub[:replace])
          end
        end
        nil
      end

      private

      def get_replaced_rd(regxp, replace)
        rd = Marshal.load(dump)
        
        rd.nodes.map! do |nid|
          nid.sub(regxp, replace)
        end

        rd.v_idx.each_value do |nids|
          nids.map! do |nid|
            nid.sub(regxp, replace)
          end
        end
        rd
      end
      
      def check_netmask?(addr, mask)
        if addr =~ /(\d+)\.(\d+)\.(\d+)\.(\d+)/
          iaddr = ($1.to_i  << 24) + ($2.to_i << 16) + ($3.to_i << 8) + $4.to_i
        else
          @log.error("#{__method__}:Illigal format addr #{addr}")
          return false
        end

        if mask =~ /(\d+)\.(\d+)\.(\d+)\.(\d+)\/(\d+)/
          imask_addr = ($1.to_i  << 24) + ($2.to_i << 16) + ($3.to_i << 8) + $4.to_i
          imask = (2 ** $5.to_i - 1) << (32 - $5.to_i)
        else
          @log.error("#{__method__}:Illigal format mask #{mask}")
          return false
        end
        (iaddr & imask) == (imask_addr & imask)
      end
      
    end # class RoutingTable

  end # module Routing
end # module Roma
