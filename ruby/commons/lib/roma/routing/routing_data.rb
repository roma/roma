require 'yaml'

module Roma
  module Routing

    class RoutingData
      attr_accessor :dgst_bits
      attr_accessor :div_bits
      attr_accessor :rn
      attr_accessor :nodes
      attr_accessor :v_idx
      attr_accessor :v_clk

      def initialize(dgst_bits,div_bits,rn)
        @dgst_bits=dgst_bits
        @div_bits=div_bits
        @rn=rn
        @nodes=[]
        @v_idx={}
        @v_clk={}
      end

      def save(fname)
        @nodes.sort!
        open(fname,'wb'){|io|
          io.write(YAML.dump(self))
        }
      end

      def self.load(fname)
        rd=load_snapshot(fname)
        rd.load_log_all(fname)
        rd
      end

      def self.load_snapshot(fname)
        rd=nil
        open(fname,'rb'){|io|
          rd = YAML.load(io.read)
        }
        rd
      end

      def self.snapshot(fname)
        rd=load_snapshot(fname)
        loglist=rd.get_file_list(fname)
        if loglist.length<2
          return false
        end
        loglist.delete(loglist.last)
        loglist.each{|i,f|
          rd.load_log_one(f)
          File.rename(f,"#{f}~")
        }
        File.rename(fname,"#{fname}~")
        rd.save(fname)
        true
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

      def self.create(dgst_bits,div_bits,rn,nodes,repethost=false)
        ret=RoutingData.new(dgst_bits,div_bits,rn)
        ret.nodes=nodes.clone

        rnlm=RandomNodeListMaker.new(nodes,repethost)

        (2**div_bits).times{|i|
          vn=i<<(dgst_bits-div_bits)
          ret.v_clk[vn]=0
          ret.v_idx[vn]=rnlm.list(rn)
        }
        ret
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
