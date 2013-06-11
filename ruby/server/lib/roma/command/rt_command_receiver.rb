
module Roma
  module Command

    module RoutingCommandReceiver

      # join <node id>
      def ev_join(s)
        @rttable.add_node(s[1])
        send_data("ADDED\r\n")
      end

      # leave <node id>
      def ev_leave(s)
        @log.warn("receive a leave #{s[1]} message.")
        @rttable.leave(s[1])
        send_data("DELETED\r\n")
      end

      def ev_nodelist(s)
        nl = nil
        @rttable.nodes.each{ |nid|
          if nl
            nl << " #{nid}"
          else
            nl = nid.clone
          end
        }
        send_data("#{nl}\r\n")
      end

      def ev_create_nodes_from_v_idx(s)
        @rttable.create_nodes_from_v_idx
        send_data("CREATED\r\n")
      end

      # routingdump [yaml|json|yamlbytes|bin]\r\n
      def ev_routingdump(s)
        if s.length == 1
          dmp = @rttable.dump
          send_data("#{dmp.length}\r\n#{dmp}\r\nEND\r\n")
        elsif s[1] == 'yaml'
          dmp = @rttable.dump_yaml
          send_data("#{dmp}\r\nEND\r\n")
        elsif s[1] == 'json'
          dmp = @rttable.dump_json
          send_data("#{dmp}\r\nEND\r\n")
        elsif s[1] == 'yamlbytes'
          dmp = @rttable.dump_yaml
          send_data("#{dmp.length + 7}\r\nEND\r\n")
        elsif s[1] == 'bin'
          dmp = @rttable.dump_binary
          send_data("#{dmp.length}\r\n#{dmp}\r\nEND\r\n")
        else
          send_data("CLIENT_ERROR\r\n")
        end
      end

      # setroute <vnode-id> <clock> <node-id> ...
      def ev_setroute(s)
        if s.length < 4
          send_data("CLIENT_ERROR\r\n")
        else
          nids=[]
          s[3..-1].each{ |nid| nids << nid }
          res=@rttable.set_route(s[1].to_i, s[2].to_i, nids)
          if res.is_a?(Integer)
            send_data("STORED\r\n")
          else
            send_data("SERVER_ERROR #{res}\r\n")
          end
        end
      end

      # getroute <vnode-id>
      def ev_getroute(s)
        if s.length < 2
          send_data("CLIENT_ERROR\r\n")
          return
        end
        clk,nids = @rttable.search_nodes_with_clk(s[1].to_i)
        if clk == nil
          send_data("END\r\n")
          return
        end
        res = "#{clk-1}"
        nids.each{ |nid| res << " #{nid}" }
        send_data("#{res}\r\n")
      end

      # mklhash <id>
      def ev_mklhash(s)
        send_data("#{@rttable.mtree.get(s[1])}\r\n")
      end

      # history_of_lost [yyyymmddhhmmss]
      def ev_history_of_lost(s)
        if s.length != 2
          t = Time.mktime(2000, 1, 1, 0, 0, 0)
        else
          t = Time.mktime(s[1][0..3], s[1][4..5], s[1][6..7], s[1][8..9], s[1][10..11], s[1][12..13])
        end
        nodes = @rttable.search_lost_vnodes(t)
        nodes.each{|vn| send_data("#{vn}\r\n") }
        send_data("END\r\n")
      rescue =>e
        send_data("CLIENT_ERROR\r\n")
      end

      # set_auto_recover [true|false]
      def ev_set_auto_recover(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        elsif
          s[1] != "true" && s[1] != "false"
          return send_data("CLIENT_ERROR arguments must be true or false\r\n")
        end

        res = broadcast_cmd("rset_auto_recover #{s[1]}\r\n")
        @rttable.auto_recover = s[1].to_s
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")
      end

      def ev_rset_auto_recover(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        elsif
          s[1] != "true" && s[1] != "false"
          return send_data("CLIENT_ERROR arguments must be true or false\r\n")
        end

        @rttable.auto_recover = s[1].to_s
        send_data("STORED\r\n")
      end

      # set_threshold_for_failover <n>
      def ev_set_threshold_for_failover(s)
        if s.length != 2 || s[1].to_i == 0
          return send_data("usage:set_threshold_for_failover <n>\r\n")
        end
        res = broadcast_cmd("rset_threshold_for_failover #{s[1]}\r\n")
        @rttable.fail_cnt_threshold = s[1].to_i
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")
      end

      # rset_threshold_for_failover <n>
      def ev_rset_threshold_for_failover(s)
        if s.length != 2 || s[1].to_i == 0
          return send_data("usage:set_threshold_for_failover <n>\r\n")
        end
        @rttable.fail_cnt_threshold = s[1].to_i
        send_data("STORED\r\n")
      end

      # set_gap_for_failover
      def ev_set_gap_for_failover(s)
        if s.length != 2
          return send_data("usage:set_gap_for_failover <n>\r\n")
        end
        res = broadcast_cmd("rset_gap_for_failover #{s[1]}\r\n")
        @rttable.fail_cnt_gap = s[1].to_f
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")        
      end

      # rset_gap_for_failover
      def ev_rset_gap_for_failover(s)
        if s.length != 2
          return send_data("usage:rset_gap_for_failover <n>\r\n")
        end
        @rttable.fail_cnt_gap = s[1].to_f
        send_data("STORED\r\n")        
      end

    end # module RoutingCommandReceiver
  end # module Command
end # module Roma
