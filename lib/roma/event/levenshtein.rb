#
# File: levenshtein.rb
#
require "levenshtein"

module Roma
  module Event
    module Distance
      def self.check_distance(cmd, ev_list)
        levenshtein_distance = 1.0 # initialize
        similar_cmd = ''
        ev_list.each_key{|ev|
          distance = Levenshtein::normalized_distance(cmd, ev)
          if distance < levenshtein_distance
            levenshtein_distance = distance 
            similar_cmd = ev
          end
        }
        return levenshtein_distance, similar_cmd
      end
    end # module Distance
  end # module Event
end # module Roma
