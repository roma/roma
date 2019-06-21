require 'yaml'
require 'roma/routing/random_balancer'

module Roma
  module Routing
    class RoutingData
      include Routing::RandomBalancer

      attr_accessor :dgst_bits
      attr_accessor :div_bits
      attr_accessor :rn
      attr_accessor :nodes
      attr_accessor :v_idx
      attr_accessor :v_clk

      DEFAULT_DIGEST_BIT_SIZE = 32
      DEFAULT_DIVIDE_BIT_SIZE = 9
      DEFAULT_REDUNDANT_SIZE = 2
      MAX_BIT_LENGTH = 32

      def initialize(dgst_bits = DEFAULT_DIGEST_BIT_SIZE, div_bits = DEFAULT_DIVIDE_BIT_SIZE, rn = DEFAULT_REDUNDANT_SIZE)
        raise ArgumentError, 'The hash bits should be divide bits or more' if dgst_bits < div_bits
        raise ArgumentError, "The upper bound of divide bits is #{MAX_BIT_LENGTH}." if div_bits > MAX_BIT_LENGTH

        @dgst_bits = dgst_bits
        @div_bits = div_bits
        @rn = rn
        @nodes = []
        @v_idx = {}
        @v_clk = {}
      end

      def save
        @nodes.sort!
        @nodes.each do |node|
          file_path = "#{node}.route"
          open(file_path, 'wb') do |io|
            YAML.dump(self, io)
          end
        end
      end

      def self.load(fname)
        rd = load_snapshot(fname)
        rd.load_log_all(fname)
        rd
      end

      def self.load_snapshot(file_path)
        YAML.load_file(file_path)
      end

      def self.snapshot(file_name)
        rd = load_snapshot(file_name)
        loglist = rd.get_file_list(file_name)
        return false if loglist.length < 2

        loglist.delete(loglist.last)
        loglist.each do |i,f|
          rd.load_log_one(f)
          File.rename(f, "#{f}~")
        end
        File.rename(file_name, "#{file_name}~")
        rd.save(file_name)
        true
      end

      def self.decode_binary(bin)
        magic, ver, dgst_bits, div_bits, rn, nodeslen = bin.unpack('a2nCCCn')
        raise 'Illegal format error' if magic != 'RT'
        raise 'Unsupported version error' if ver != 1

        routing_data = RoutingData.new(dgst_bits, div_bits, rn)

        bin = bin[9..-1]
        nodeslen.times do |i|
          len, = bin.unpack('n')
          bin = bin[2..-1]
          nid, = bin.unpack("a#{len}")
          bin = bin[len..-1]
          nid.encode!("utf-8")
          routing_data.nodes << nid
        end
        (2**div_bits).times do |i|
          vn = i << (dgst_bits - div_bits)
          v_clk, len = bin.unpack('Nc')
          routing_data.v_clk[vn] = v_clk
          bin = bin[5..-1]
          len.times do |i|
            idx, = bin.unpack('n')
            routing_data.v_idx[vn] ||= []
            routing_data.v_idx[vn] << routing_data.nodes[idx]
            bin = bin[2..-1]
          end
        end
        routing_data
      end

      # for deep copy
      def clone
        Marshal.load(Marshal.dump(self))
      end

      # 2 bytes('RT'):magic code
      # unsigned short:format version
      # unsigned char:dgst_bits
      # unsigned char:div_bits
      # unsigned char:rn
      # unsigned short:number of nodes
      # while number of nodes
      #  unsigned short:length of node-id string
      #  node-id string
      # while umber of vnodes
      #  unsigned int32:v_clk
      #  unsigned char:number of nodes
      #  while umber of nodes
      #   unsigned short:index of nodes
      def dump_binary
        format_version = 1
        # 9 bytes
        ret = ['RT',format_version,dgst_bits,div_bits,rn,nodes.length].pack('a2nCCCn')
        rev_hash = {}
        nodes.each_with_index{|nid,idx|
          rev_hash[nid] = idx
          # 2 + nid.length bytes
          ret += [nid.length,nid].pack('na*')
        }
        (2**div_bits).times{|i|
          vn=i<<(dgst_bits-div_bits)
          # 5 bytes
          ret += [v_clk[vn],v_idx[vn].length].pack('Nc')
          v_idx[vn].each{|nid|
            # 2 bytes
            ret += [rev_hash[nid]].pack('n')
          }
        }
        ret
      end

      def each_log_all(fname)
        loglist=get_file_list(fname)
        loglist.each{|i,f|
          each_log_one(f){|t,l| yield t,l}
        }
      end

      def each_log_one(fname)
        File.open(fname,"r"){|f|
          while((line=f.gets)!=nil)
            line.chomp!
            next if line[0]=="#" || line.length==0
            if line =~ /(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.\d+\s(.+)/
              yield Time.mktime($1, $2, $3, $4, $5, $6), $7
            end
          end
        }
      end

      def load_log_all(fname)
        each_log_all(fname){|t,line|
          parse_log(t,line)
        }
        @nodes.sort!
      end

      def load_log_one(fname)
        each_log_one(fname){|t,line|
          parse_log(t,line)
        }
        @nodes.sort!
      end

      def parse_log(t,line)
        s=line.split(' ')
        case s[0]
        when 'setroute'
          # setroute <vnode-id> <clock> <node-id> ...
          nids=[]
          s[3..-1].each{ |nid| nids << nid }
          @v_idx[s[1].to_i]=nids
          @v_clk[s[1].to_i]=s[2].to_i
        when 'join'
          # join <node-id>
          @nodes << s[1] unless @nodes.include?(s[1])
        when 'leave'
          # leave <node-id>
          @nodes.delete(s[1])
        else
          raise "RoutingData.parse_log:parse error #{line}"
        end
      end

      def search_mask
        2**@div_bits-1<<(@dgst_bits-@div_bits)
      end

      def next_vnode(vn)
        n = (vn >> (@dgst_bits-@div_bits)) + 1
        n = 0 if n == (2**@div_bits)
        n << (@dgst_bits-@div_bits)
      end

      def create_nodes_from_v_idx
        buf_nodes={}
        v_idx.each_value{|nids|
          nids.each{|nid| buf_nodes[nid]=nid }
        }
        @nodes=buf_nodes.values.sort
      end

      # Returns the losted vnode-id list.
      def get_lost_vnodes
        ret=[]
        v_idx.each_pair{|vn,nids|
          ret << vn if nids.length == 0
        }
        ret
      end

      def self.create(digest_bit_size: DEFAULT_DIGEST_BIT_SIZE, divide_bit_size: DEFAULT_DIVIDE_BIT_SIZE, redundant: DEFAULT_REDUNDANT_SIZE, nodes: [], replication_in_host: false)
        raise ArgumentError, 'The node-id number should be redundant number or more.' if nodes.length < redundant

        routing_data = RoutingData.new(digest_bit_size, divide_bit_size, redundant)
        routing_data.nodes = nodes.clone

        rnlm = RandomNodeListMaker.new(nodes, replication_in_host)

        (2**divide_bit_size).times do |i|
          vn = i<<(digest_bit_size - divide_bit_size)
          routing_data.v_clk[vn] = 0
          routing_data.v_idx[vn] = rnlm.list(redundant)
        end

        # vnode balanceing process
        rlist = routing_data.get_balanced_vn_replacement_list(replication_in_host)
        routing_data.balance!(rlist, replication_in_host) if rlist

        routing_data
      end

      # Returns the log file list by old ordered.
      # +fname+:: Prefix of a log file.(ex.roma0_3300.route)
      # One of the following example:
      #   [[1, "roma0_3300.route.1"], [2, "roma0_3300.route.2"]]
      def get_file_list(fname)
        l={}
        files=Dir.glob("#{fname}*")
        files.each{ |file|
          if /#{fname}\.(\d+)$/=~file
            l[$1.to_i]=$&
          end
        }
        # sorted by old order
        l.to_a.sort{|a,b| a[0]<=>b[0]}
      end

      def get_histgram
        ret = {}
        nodes.each{|nid|
          ret[nid] = Array.new(rn,0)
        }
        v_idx.each_pair{|vn,nids|
          nids.each_with_index{|nid,i|
            ret[nid][i] += 1
          }
        }
        ret
      end

      private

      class RandomNodeListMaker
        def initialize(nodes,repethost)
          @repethost=repethost
          @nodes=nodes
          @host_idx={}
          nodes.each{|nid|
            h,p=nid.split('_')
            if @host_idx.key?(h)
              @host_idx[h] << nid
            else
              @host_idx[h]=[nid]
            end
          }
        end

        # Returns the random node-list without repetition.
        # +n+:: list length
        def list(n)
          ret=[]
          hosts=[]
          proc_other_one = :get_other_one
          proc_other_one = :get_other_one_repethost if @repethost
          n.times{
            nid=nil
            nid=send(proc_other_one,hosts,ret)
            break unless nid
            hosts << nid.split('_')[0]
            ret << nid
          }
          ret
        end

        # +exp_hosts+:: ignore
        # +exp_nodes+:: exceptional nodes(ex.['roma0_11211'])
        def get_other_one_repethost(exp_hosts,exp_nodes)
          buf=@nodes.clone
          buf.delete_if{|nid| exp_nodes.include?(nid)}
          buf[rand(buf.length)]
        end

        # +exp_hosts+:: exceptional hosts(ex.['roma0','roma1'])
        # +exp_nodes+:: ignore
        def get_other_one(exp_hosts,exp_nodes)
          hidx=@host_idx.clone
          exp_hosts.each{|h| hidx.delete(h) }
          return nil if hidx.length == 0

          rh=hidx.keys[rand(hidx.keys.length)]
          nodes=hidx[rh]
          nodes[rand(nodes.length)]
        end
      end # class RandomNodeListMaker

    end # class RoutingData

  end # module Routing
end # module Roma
