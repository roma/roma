#!/usr/bin/env ruby

class RoutingLogicTest < Test::Unit::TestCase
  include RomaTestUtils

  NEW_PORTS = %w(11213 11214)

  def teardown
    stop_roma
  rescue => e
    puts "#{e} #{$ERROR_POSITION}"
  end

  def test_routing_logic_join
    data= {
      'single_host' => [true, NEW_PORTS.map { |port| "#{DEFAULT_HOST}_#{port}" }],
      'multiple_hosts' => [false, NEW_PORTS.map { |port| "#{DEFAULT_IP}_#{port}" }],
      'multiple_hosts_include_some_hosts' => [false, ["#{DEFAULT_IP}_#{NEW_PORTS[0]}", "#{DEFAULT_HOST}_#{NEW_PORTS[1]}"]]
    }
    data.values.each{|replication_in_host, new_nodes|
      start_roma(div_bits: 6, replication_in_host: replication_in_host)
      sleep 12 # Wait cluster starting, otherwise join will never finish
      client = get_client

      # Join nodes
      assert_equal(DEFAULT_NODES.size, client.rttable.nodes.size)
      new_nodes.each do |new_node|
        join_roma(new_node, replication_in_host: replication_in_host)
        wait_join(new_node)
        stats = client.stats(node: new_node)
        assert_match(/#{new_node}/, stats['routing.nodes'])
        assert_not_equal(0, stats['routing.secondary'].to_i)
      end

      new_node = new_nodes.last

      # Release and stop a node
      release_roma_node(new_node)
      wait_release(new_node)
      stop_roma_node(new_node)
      wait_failover(new_node)
      stats = client.stats
      assert_no_match(/#{new_node}/, stats['routing.nodes'])
      assert_equal(0, stats['routing.short_vnodes'].to_i)

      # Join a node without short vnodes
      join_roma(new_node, replication_in_host: replication_in_host)
      wait_join(new_node)
      stats = client.stats(node: new_node)
      assert_match(/#{new_node}/, stats['routing.nodes'])
      assert_not_equal(0, stats['routing.secondary'].to_i)

      # Stop a node and generate short vnodes
      stop_roma_node(new_node)
      wait_failover(new_node)
      stats = client.stats
      assert_no_match(/#{new_node}/, stats['routing.nodes'])
      assert_not_equal(0, stats['routing.short_vnodes'].to_i)

      # Join a node with short vnodes
      join_roma(new_node, replication_in_host: replication_in_host)
      wait_join(new_node)
      stats = client.stats(node: new_node)
      assert_match(/#{new_node}/, stats['routing.nodes'])
      assert_not_equal(0, stats['routing.secondary'].to_i)

      stop_roma
    }
  end

  def test_routing_logic_join_when_num_of_hosts_changing
    replication_in_host = false
    same_host_node = "#{DEFAULT_HOST}_#{NEW_PORTS[0]}"
    other_host_node = "#{DEFAULT_IP}_#{NEW_PORTS[1]}"

    start_roma(div_bits: 6, replication_in_host: replication_in_host)
    sleep 12 # Wait cluster starting, otherwise join will never finish
    client = get_client

    # Join other_host_node
    join_roma(other_host_node, replication_in_host: replication_in_host)
    wait_join(other_host_node)
    stats = client.stats(node: other_host_node)
    assert_match(/#{other_host_node}/, stats['routing.nodes'])
    assert_not_equal(0, stats['routing.secondary'].to_i)

    # Stop other_host_node and generate short vnodes
    stop_roma_node(other_host_node)
    wait_failover(other_host_node)
    stats = client.stats
    assert_no_match(/#{other_host_node}/, stats['routing.nodes'])
    assert_not_equal(0, stats['routing.short_vnodes'].to_i)

    # Join other_host_node
    join_roma(other_host_node, replication_in_host: replication_in_host)
    wait_join(other_host_node)
    stats = client.stats(node: other_host_node)
    assert_match(/#{other_host_node}/, stats['routing.nodes'])
    assert_not_equal(0, stats['routing.secondary'].to_i)

    # Stop other_host_node and generate short vnodes
    stop_roma_node(other_host_node)
    wait_failover(other_host_node)
    stats = client.stats
    assert_no_match(/#{other_host_node}/, stats['routing.nodes'])
    assert_not_equal(0, stats['routing.short_vnodes'].to_i)

    # Join same_host_node (secondary will be 0 in this case)
    join_roma(same_host_node, replication_in_host: replication_in_host)
    wait_join(same_host_node)
    stats = client.stats(node: same_host_node)
    assert_match(/#{same_host_node}/, stats['routing.nodes'])
    assert_equal(0, stats['routing.secondary'].to_i)
  end

  # TODO: recover, balance command tests
end
