
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
        rt = @rttable
        rd = @rttable.sub_nid_rd(@addr)
        rt = Roma::Routing::RoutingTable.new(rd) if rd

        if s.length == 1
          dmp = rt.dump
          send_data("#{dmp.length}\r\n#{dmp}\r\nEND\r\n")
        elsif s[1] == 'yaml'
          dmp = rt.dump_yaml
          send_data("#{dmp}\r\nEND\r\n")
        elsif s[1] == 'json'
          dmp = rt.dump_json
          send_data("#{dmp}\r\nEND\r\n")
        elsif s[1] == 'yamlbytes'
          dmp = rt.dump_yaml
          send_data("#{dmp.length + 7}\r\nEND\r\n")
        elsif s[1] == 'bin'
          dmp = rt.dump_binary
          send_data("#{dmp.length}\r\n#{dmp}\r\nEND\r\n")
        else
          send_data("CLIENT_ERROR\r\n")
        end
      end

      def ev_enabled_repetition_in_routing?(s)
        rt = @rttable
        rd = @rttable.sub_nid_rd(@addr)
        rt = Roma::Routing::RoutingTable.new(rd) if rd

        if s.length == 1
          repetition = rt.check_repetition_in_routing
          send_data("#{repetition}\r\n")
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
          # check irregular node name
          nids.each{ |nid|
            if !nid.ascii_only? || nid.empty?
              send_data("CLIENT_ERROR : irregular node name was input.[\"#{nid}\"]\r\n")
              return
            end
          }
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

      # set_auto_recover [true|false] <sec>
      def ev_set_auto_recover(s)
        #check argument
        if /^true$|^false$/ !~ s[1]
          return send_data("CLIENT_ERROR arguments must be true or false\r\n")
        elsif s.length != 2 && s.length != 3
          return send_data("CLIENT_ERROR number of arguments(0 for 1)\r\n")
        elsif s.length == 3 && s[2].to_i < 1
          return send_data("CLIENT_ERROR length must be greater than zero\r\n")
        end
        res = broadcast_cmd("rset_auto_recover #{s[1]} #{s[2]}\r\n")
        if s[1] == "true"
          @rttable.auto_recover = true
        elsif s[1] == "false"
          @rttable.auto_recover = false
        end
        @rttable.auto_recover_status = "waiting"
        @rttable.auto_recover_time = s[2].to_i if s[2]
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")
      end

      def ev_rset_auto_recover(s)
        if /^true$|^false$/ !~ s[1]
          return send_data("CLIENT_ERROR arguments must be true or false #{s[1]} #{s[1].class} \r\n")
        elsif s.length != 2 && s.length != 3
          return send_data("CLIENT_ERROR number of arguments(0 for 1)\r\n")
        elsif s.length == 3 && s[2].to_i < 1
          return send_data("CLIENT_ERROR length must be greater than zero\r\n")
        end
        if s[1] == "true"
          @rttable.auto_recover = true
        elsif s[1] == "false"
          @rttable.auto_recover = false
        end
        @rttable.auto_recover_status = "waiting"
        @rttable.auto_recover_time = s[2].to_i if s[2]
        send_data("STORED\r\n")
      end

      # set_lost_action [auto_assign|shutdown]
      def ev_set_lost_action(s)
        if s.length != 2 || /^auto_assign$|^shutdown$/ !~ s[1]
          return send_data("CLIENT_ERROR changing lost_action must be auto_assign or shutdown\r\n")
        elsif /^auto_assign$|^shutdown$/ !~ @rttable.lost_action
          return send_data("CLIENT_ERROR can use this command only current lost action is auto_assign or shutdwn mode\r\n")
        end
        res = broadcast_cmd("rset_lost_action #{s[1]}\r\n")
        @rttable.lost_action = s[1].to_sym
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")
      end
      
      def ev_rset_lost_action(s)
        if s.length != 2 || /^auto_assign$|^shutdown$/ !~ s[1]
          return send_data("CLIENT_ERROR changing lost_action must be auto_assign or shutdown\r\n")
        elsif /^auto_assign$|^shutdown$/ !~ @rttable.lost_action
          return send_data("CLIENT_ERROR can use this command only current lost action is auto_assign or shutdwn mode\r\n")
        end
        @rttable.lost_action = s[1].to_sym
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

      # cleat RTTABLE_SUB_NID map
      def ev_clear_rttable_sub_nid(s)
        res = broadcast_cmd("rclear_rttable_sub_nid\r\n")
        @rttable.sub_nid.clear()
        res[@stats.ap_str] = "CLEARED"
        send_data("#{res}\r\n")
      end

      def ev_rclear_rttable_sub_nid(s)
        @rttable.sub_nid.clear()
        send_data("CLEARED\r\n")
      end
      
      # add_rttable_sub_nid <netmask> <regexp> <replace>
      def ev_add_rttable_sub_nid(s)
        if s.length != 4
          return send_data("usage:add_rttable_sub_nid <netmask> <regexp> <replace>\r\n")
        end
        res = broadcast_cmd("radd_rttable_sub_nid #{s[1]} #{s[2]} #{s[3]}\r\n")
        @rttable.sub_nid[s[1]] = {:regexp => "#{s[2]}", :replace => "#{s[3]}"}
        res[@stats.ap_str] = "ADDED"
        send_data("#{res}\r\n")
      end

      # radd_rttable_sub_nid <netmask> <regexp> <replace>
      def ev_radd_rttable_sub_nid(s)
        if s.length != 4
          return send_data("usage:add_rttable_sub_nid <netmask> <regexp> <replace>\r\n")
        end
        @rttable.sub_nid[s[1]] = {:regexp => "#{s[2]}", :replace => "#{s[3]}"}
        send_data("ADDED\r\n")
      end

      # delete_rttable_sub_nid <netmask>
      def ev_delete_rttable_sub_nid(s)
        if s.length != 2
          return send_data("usage:delete_rttable_sub_nid <netmask>\r\n")
        end

        res = broadcast_cmd("rdelete_rttable_sub_nid #{s[1]}\r\n")
        unless @rttable.sub_nid.delete s[1]
          res[@stats.ap_str] = "NOT_FOUND"
        else
          res[@stats.ap_str] = "DELETED"
        end
        send_data("#{res}\r\n")
      end

      # rdelete_rttable_sub_nid <netmask>
      def ev_rdelete_rttable_sub_nid(s)
        if s.length != 2
          return send_data("usage:delete_rttable_sub_nid <netmask>\r\n")
        end

        unless @rttable.sub_nid.delete s[1]
          send_data("NOT_FOUND\r\n")
        else
          send_data("DELETED\r\n")
        end
      end

      # get_key_info <key>
      def ev_get_key_info(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments(0 for 1)\r\n")
        end

        d = Digest::SHA1.hexdigest(s[1]).hex % @rttable.hbits
        vn = @rttable.get_vnode_id(d)
        nodes = @rttable.search_nodes_for_write(vn)
        send_data(sprintf("d = %s 0x%x\r\n",d,d))
        send_data(sprintf("vn = %s 0x%x\r\n",vn,vn))
        send_data("nodes = #{nodes.inspect}\r\n")
        send_data("END\r\n")
      end

    end # module RoutingCommandReceiver
  end # module Command
end # module Roma
