require 'thor'
require 'roma/routing/routing_data'

module Roma
  class CLI < Thor
    class Routing < Thor
      method_option :digest_bit_size, type: :numeric, default: Roma::Routing::RoutingData::DEFAULT_DIGEST_BIT_SIZE, aliases: '-h', desc: 'Bit length of digest hash'
      method_option :divide_bit_size, type: :numeric, default: Roma::Routing::RoutingData::DEFAULT_DIVIDE_BIT_SIZE, aliases: '-d', desc: 'Bit length for dividing'
      method_option :redundant, type: :numeric, default: Roma::Routing::RoutingData::DEFAULT_REDUNDANT_SIZE, aliases: '-r', desc: 'Number of redundant'
      method_option :replication_in_host, type: :boolean, default: false, desc: 'Allow to replicate in the same host'
      desc 'create <NODE>, [<NODE>...]', 'Create routing for nodes'
      def create(node, *nodes)
        nodes.unshift(node)
        nodes = nodes.map { |n| n.sub(':', '_') }
        options['nodes'] = nodes

        arguments = options.inject({}) { |args, (k, v)| args[k.to_sym] = v; args }

        routing_data = Roma::Routing::RoutingData::create(**arguments)
        routing_data.save

        puts "nodes => #{nodes}"
        puts "Routing table has created."
      end
    end
  end
end