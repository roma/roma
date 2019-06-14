require 'thor'
require 'roma/routing/routing_data'

module Roma
  class CLI < Thor
    class Routing < Thor
      MAX_BIT_LENGTH = 32

      method_option :hash, type: :numeric, default: 32, aliases: '-h', desc: 'Bit length of hash'
      method_option :divide, type: :numeric, default: 9, aliases: '-d', desc: 'Bit length for dividing'
      method_option :redundant, type: :numeric, default: 2, aliases: '-r', desc: 'Number of redundant'
      method_option :enabled_repeathost, type: :boolean, default: false, desc: 'Allow to repeat host'
      method_option :replication_in_host, type: :boolean, default: false, desc: 'Allow to replicate in the same host'
      desc 'create <NODE>, [<NODE>...]', 'Create routing for nodes'
      def create(node, *nodes)
        nodes.unshift(node)
        nodes = nodes.map { |n| n.sub(':', '_') }

        STDERR.puts 'The hash bits should be divide bits or more' if options[:hash] < options[:divide]
        STDERR.puts "The upper bound of divide bits is #{MAX_BIT_LENGTH}." if options[:divide] > MAX_BIT_LENGTH
        STDERR.puts 'The node-id number should be redundant number or more.' if nodes.length < options[:redundant]

        routing_data = Roma::Routing::RoutingData::create(
          options[:hash], options[:divide], options[:redundant], nodes, options[:replication_in_host]
        )

        nodes.each do |nid|
          routing_data.save("#{nid}.route")
        end

        puts "nodes => #{nodes}"
        puts "Routing table has created."
      end
    end
  end
end