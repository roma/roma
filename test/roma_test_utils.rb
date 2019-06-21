require 'shell'
require 'pathname'
require 'fileutils'
require 'rbconfig'
require 'roma/config'
require 'roma/messaging/con_pool'
require 'roma/routing/routing_data'
require 'roma/server'
require 'roma/logging/rlogger'
require 'roma/client/rclient'
require 'roma/client/version'
require 'roma/client/client_pool'

Roma::Client::RomaClient.class_eval do
  def init_sync_routing_proc
  end
end

module RomaTestUtils
  DEFAULT_CONFIG = 'config4test.rb'
  DEFAULT_HOST = 'localhost'
  DEFAULT_IP = '127.0.0.1'
  DEFAULT_PORTS = %w(11211 11212)
  DEFAULT_NODES = DEFAULT_PORTS.map { |port| "#{DEFAULT_HOST}_#{port}" }
  DEFAULT_TIMEOUT_SEC = 90
  REPLICA_PORTS = %w(21211 21212)
  REPLICA_NODES = REPLICA_PORTS.map { |port| "#{DEFAULT_HOST}_#{port}" }
  SHELL_LOG = 'roma_test_outputs.log'

  Roma::Logging::RLogger.create_singleton_instance("#{Roma::Config::LOG_PATH}/roma_test.log",
                                                   Roma::Config::LOG_SHIFT_AGE,
                                                   Roma::Config::LOG_SHIFT_SIZE)

  module_function

  def start_roma(conf = DEFAULT_CONFIG, div_bits: 3, replication_in_host: true)
    FileUtils.rm_rf(Dir.glob("#{Roma::Config::STORAGE_PATH}/#{DEFAULT_NODES[0][0..-2]}?*"))
    FileUtils.rm_rf(Dir.glob("#{Roma::Config::STORAGE_PATH}/#{DEFAULT_IP}_#{DEFAULT_PORTS[0][0..-2]}?*"))
    sleep 0.1

    routing_data = Roma::Routing::RoutingData.create(divide_bit_size: div_bits, replication_in_host: true, nodes: DEFAULT_NODES)
    routing_data.save
    sleep 0.2

    DEFAULT_NODES.each do |node|
      do_command_romad(node, conf, replication_in_host)
    end
    sleep 1
  end

  def start_roma_replica(conf = DEFAULT_CONFIG, div_bits: 3, replication_in_host: true)
    FileUtils.rm_rf(Dir.glob("#{Roma::Config::STORAGE_PATH}/#{REPLICA_NODES[0][0..-2]}?*"))
    FileUtils.rm_rf(Dir.glob("#{Roma::Config::STORAGE_PATH}/#{DEFAULT_IP}_#{REPLICA_PORTS[0][0..-2]}?*"))
    sleep 0.1

    routing_data = Roma::Routing::RoutingData.create(divide_bit_size: div_bits, replication_in_host: replication_in_host, nodes: REPLICA_NODES)
    routing_data.save
    sleep 0.2

    REPLICA_NODES.each do |node|
      do_command_romad(node, conf, replication_in_host)
    end
    sleep 1
  end

  def do_command_romad(node, conf, replication_in_host = true, is_join = false)
    host, port = node.split('_')
    romad_command = [
      ruby_path, roma_path,
      'server start',
      host,
      '-p', port,
      '-d', '--verbose', '--disabled-cmd-protect',
      '--config', "#{test_dir}/#{conf}"
    ]
    romad_command << '--replication-in-host' if replication_in_host
    romad_command << "-j #{DEFAULT_NODES[0]}" if is_join
    romad_command << ">> #{SHELL_LOG} 2>&1"

    system(romad_command.join(' '))
  end

  def get_client
    client_pool = Roma::Client::ClientPool.instance
    client_pool.servers = DEFAULT_NODES
    client_pool.client
  end

  def wait_join(node)
    client = get_client
    wait_count = 0
    sleep 5

    until client.rttable.nodes.include?(node)
      sleep 1
      client.update_rttable
      wait_count += 1
      fail "#{__method__} timeout" if wait_count > DEFAULT_TIMEOUT_SEC
    end

    while client.stats(node: node)['stats.run_join'] == 'true'
      sleep 1
      wait_count += 1
      fail "#{__method__} timeout" if wait_count > DEFAULT_TIMEOUT_SEC
    end
  end

  def wait_failover(down_node)
    client = get_client
    stats_node = down_node == DEFAULT_NODES[0] ? DEFAULT_NODES[1] : DEFAULT_NODES[0]
    wait_count = 0
    sleep 1

    while client.stats(node: stats_node)['routing.nodes'] =~ /#{down_node}/
      sleep 1
      wait_count += 1
      fail "#{__method__} timeout" if wait_count > DEFAULT_TIMEOUT_SEC
    end
  end

  def wait_release(node)
    client = get_client
    wait_count = 0
    sleep 1

    while client.stats(node: node)['stats.run_release'] == 'true'
      sleep 1
      wait_count += 1
      fail "#{__method__} timeout" if wait_count > DEFAULT_TIMEOUT_SEC
    end
  end

  def stop_roma
    balse_message = %w(balse yes)
    send_message(messages: balse_message)
    Roma::Client::ConPool.instance.close_all
    Roma::Messaging::ConPool.instance.close_all
  end

  def stop_roma_replica
    balse_message = %w(balse yes)
    send_message(messages: balse_message, node: REPLICA_NODES[0])
    Roma::Client::ConPool.instance.close_all
    Roma::Messaging::ConPool.instance.close_all
  end

  def stop_roma_node(node)
    send_message(messages: 'rbalse', node: node)
    Roma::Client::ConPool.instance.close_all
    Roma::Messaging::ConPool.instance.close_all
  end

  def release_roma_node(node)
    send_message(messages: 'release', node: node)
  end

  private

  def base_dir
    Pathname(__FILE__).dirname.parent.expand_path
  end

  def bin_dir
    base_dir + 'bin'
  end

  def test_dir
    base_dir + 'test'
  end

  def mkroute_path
    (bin_dir + 'mkroute').to_s
  end

  def roma_path
    (bin_dir + 'roma').to_s
  end

  def ruby_path
    File.join(RbConfig::CONFIG['bindir'],
              RbConfig::CONFIG['ruby_install_name'])
  end

  def send_message(messages: [], node: DEFAULT_NODES[0])
    conn = Roma::Messaging::ConPool.instance.get_connection(node)
    return false unless conn

    messages = [messages] if messages.class == String

    messages.each do |message|
      conn.write "#{message}\r\n"
      conn.gets
    end
  rescue => e
    puts "#{e} #{$ERROR_POSITION}"
  ensure
    Roma::Messaging::ConPool.instance.return_connection(node, conn)
  end
end
