module Roma
  module Routing
    module RandomBalancer

      # The randomly selected +from+'s vertual-node changes to +to+.
      # +idx+:: As for 0 is primary, 1 or more are secondary.
      def randomly_change_nid!(idx, from, to, repethost = false)
        vns = []
        v_idx.each_pair do |vn, nids|
          cnt = 0
          nids.each_with_index do |nid, i|
            if idx == i
              cnt += 1 if nid == from
            else
              if repethost == true
                cnt += 1 if nid != to
              else
                cnt += 1 if nid.split('_')[0] != to.split('_')[0]
              end
            end
          end
          vns << vn if cnt == nids.length
        end
        return nil if vns.length == 0
        vn = vns[rand(vns.length)]
        #puts "#{vn} #{v_idx[vn]}"
        v_idx[vn][idx] = to
        #puts "#{vn} #{v_idx[vn]}"
        vn
      end

      # Returns min/max values and correspondent node-id of the histgram.
      # +idx+:: As for 0 is primary, 1 or more are secondary.
      def get_min_max_histgram(idx)
        h = get_histgram
        min_nid = max_nid = nil
        min = v_idx.length
        max = 0
        h.each do |nid, v|
          if v[idx] < min
            min = v[idx]
            min_nid = nid
          end
          if v[idx] > max
            max = v[idx]
            max_nid = nid
          end
        end
        [min, min_nid, max, max_nid]
      end
      
      # Returns a replacement list for balanced routing.
      def get_balanced_vn_replacement_list(repethost = false)
        rd = clone
        ret = []
        @rn.times do |idx| # primary, secondary1, ...
          loop do # until balanced
            min, min_nid, max, max_nid = rd.get_min_max_histgram(idx)
            break if max - min < 2 || min_nid == max_nid
            vn = rd.randomly_change_nid!(idx, max_nid, min_nid, repethost)
            return nil unless vn # error
            ret << {:vn=>vn, :idx=>idx, :from=>max_nid, :to=>min_nid}
          end
        end
        ret
      end
      
      def balance!(vn_replacement_list, repethost = false)
        vn_replacement_list.each do |rep|
          v_idx[rep[:vn]][rep[:idx]] = rep[:to]
        end
      end

    end # module RandomBalancer
  end
end
