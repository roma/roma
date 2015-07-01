#
# File: jaro_winkler.rb
#
require "jaro_winkler"

module Roma
  module Event
    module Distance
      def self.check_distance(cmd, ev_list)
        jaro_winkler_distance = 0.0000 # initialize
        similar_cmd = ''
        ev_list.each_key{|ev|
          distance = JaroWinkler.distance(cmd, ev)
          if distance > jaro_winkler_distance
            jaro_winkler_distance = distance 
            similar_cmd = ev
          end
        }
        return (1-jaro_winkler_distance), similar_cmd
      end
    end # module Distance
  end # module Event
end # module Roma
