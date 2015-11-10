#!/usr/bin/env ruby

require 'socket'

class NewFuncTest < Test::Unit::TestCase
  include RomaTestUtils

  def setup
    start_roma
    @sock = TCPSocket.new("localhost", 11211)
  end

  def teardown
    @sock.close if @sock
    stop_roma
  rescue => e
    puts "#{e} #{$@}"
  end

  def test_jaro_winkler
    @sock.write("staat\r\n")
    assert_equal("", @sock.gets.chomp)
    assert_equal("ERROR: 'staat' is not roma command.", @sock.gets.chomp)
    assert_equal("Did you mean this?", @sock.gets.chomp)
    assert_equal("\tstat", @sock.gets.chomp)

    @sock.write("outingdump yaml\r\n")
    assert_equal("", @sock.gets.chomp)
    assert_equal("ERROR: 'outingdump' is not roma command.", @sock.gets.chomp)
    assert_equal("Did you mean this?", @sock.gets.chomp)
    assert_equal("\troutingdump", @sock.gets.chomp)

    @sock.write("hoge\r\n")
    assert_equal("ERROR: 'hoge' is not roma command. Please check command.", @sock.gets.chomp)
    assert_equal("(closing telnet connection command is 'quit')", @sock.gets.chomp)
  end

  def test_stat_log_level
    @sock.write("stat log_level\r\n")
    assert_equal("stats.log_level debug", @sock.gets.chomp)
    assert_equal("END", @sock.gets.chomp)
  end

  def test_stat_failover
    @sock.write("stat failover\r\n")
    assert_equal("routing.enabled_failover false", @sock.gets.chomp)
    assert_equal("END", @sock.gets.chomp)
  end

  def test_stat_secondary
    @sock.write("stat secondary\r\n")
    column, param = @sock.gets.chomp.split("\s")
    assert_equal("routing.secondary1", column)
    assert_equal(true, param.to_i > 1)
    assert_equal("END", @sock.gets.chomp)
  end

  def test_shutdown_self_no
    @sock.write("shutdown_self\r\n")
    assert_equal("", @sock.gets.chomp)
    assert_equal("=================================================================", @sock.gets.chomp)
    assert_equal("CAUTION!!: ", @sock.gets.chomp)
    assert_equal("\tThis command kill the instance!", @sock.gets.chomp)
    assert_equal("\tThere is some possibility of occuring redundancy down!", @sock.gets.chomp)
    assert_equal("=================================================================", @sock.gets.chomp)
    assert_equal("", @sock.gets.chomp)
    assert_equal("Are you sure to shutdown this instance?(yes/no)", @sock.gets.chomp)

    @sock.write("no\r\n")
    assert_nil(@sock.gets)
  end

  def test_shutdown_self_yes
    @sock.write("shutdown_self\r\n")
    assert_equal("", @sock.gets.chomp)
    assert_equal("=================================================================", @sock.gets.chomp)
    assert_equal("CAUTION!!: ", @sock.gets.chomp)
    assert_equal("\tThis command kill the instance!", @sock.gets.chomp)
    assert_equal("\tThere is some possibility of occuring redundancy down!", @sock.gets.chomp)
    assert_equal("=================================================================", @sock.gets.chomp)
    assert_equal("", @sock.gets.chomp)
    assert_equal("Are you sure to shutdown this instance?(yes/no)", @sock.gets.chomp)

    @sock.write("yes\r\n")
    assert_equal("BYE", @sock.gets.chomp)
    assert_nil(@sock.gets)

    sock2 = TCPSocket.new("localhost", 11212)
    sock2.write("nodelist\r\n")
    nodelist = sock2.gets.chomp.split("\s")
    assert_equal(1, nodelist.size)
    assert_equal("localhost_11212", nodelist[0])
  end

  def test_get_key_info
    @sock.write("set key1 0 0 4\r\nval1\r\n")
    assert_equal("STORED", @sock.gets.chomp)

    @sock.write("get_key_info key1\r\n")
    assert_match(/^d = \d+ 0x\h+$/, @sock.gets.chomp)
    assert_match(/^vn = \d+ 0x\h+$/, @sock.gets.chomp)
    assert_match(/^nodes = \["localhost_1121[1|2]", "localhost_1121[1|2]"\]$/, @sock.gets.chomp)
    assert_equal('END', @sock.gets.chomp)
  end

  def test_enabled_repetition_in_routing
    @sock.write("enabled_repetition_in_routing?\r\n")
    assert_equal("true", @sock.gets.chomp)
  end

  def test_switch_dns_caching
    @sock.write("stat dns\r\n")
    assert_equal("dns_caching false", @sock.gets.chomp)
    assert_equal("END", @sock.gets.chomp)

    @sock.write("switch_dns_caching on \r\n")
    assert_equal('{"localhost_11212"=>"ENABLED", "localhost_11211"=>"ENABLED"}', @sock.gets.chomp)
    @sock.write("stat dns\r\n")
    assert_equal("dns_caching true", @sock.gets.chomp)
    assert_equal("END", @sock.gets.chomp)

    @sock.write("switch_dns_caching off \r\n")
    assert_equal('{"localhost_11212"=>"DISABLED", "localhost_11211"=>"DISABLED"}', @sock.gets.chomp)
    @sock.write("stat dns\r\n")
    assert_equal("dns_caching false", @sock.gets.chomp)
    assert_equal("END", @sock.gets.chomp)

    @sock.write("switch_dns_caching off \r\n")
    assert_equal('{"localhost_11212"=>"DISABLED", "localhost_11211"=>"DISABLED"}', @sock.gets.chomp)
    @sock.write("stat dns\r\n")
    assert_equal("dns_caching false", @sock.gets.chomp)
    assert_equal("END", @sock.gets.chomp)
  end

  def test_nid_brank
    @sock.write("stat sub\r\n")
    assert_equal('routing.sub_nid {}', @sock.gets.chomp)
    assert_equal('END', @sock.gets.chomp)
    @sock.write("routingdump yaml\r\n")
    dump = []
    while select [@sock], nil, nil, 0.5
      dump << @sock.gets.chomp!
    end
    assert_equal(true, dump.grep(/vm/).empty?)
  end

  def test_add_rttable_sub_nid
    @sock.write("stat sub\r\n")
    assert_equal('routing.sub_nid {}', @sock.gets.chomp)
    assert_equal('END', @sock.gets.chomp)

    @sock.write("add_rttable_sub_nid 127.0.0.0/24 localhost vm\r\n")
    assert_equal('{"localhost_11212"=>"ADDED", "localhost_11211"=>"ADDED"}', @sock.gets.chomp)

    @sock.write("stat sub\r\n")
    assert_equal('routing.sub_nid {"127.0.0.0/24"=>{:regexp=>"localhost", :replace=>"vm"}}', @sock.gets.chomp)
    assert_equal('END', @sock.gets.chomp)

    @sock.write("routingdump yaml\r\n")
    dump = []
    while select [@sock], nil, nil, 0.5
      dump << @sock.gets.chomp!
    end
    assert_equal(false, dump.grep(/vm/).empty?)
  end

  def test_delete_rttable_sub_nid
    @sock.write("add_rttable_sub_nid 127.0.0.0/24 localhost vm\r\n")
    assert_equal('{"localhost_11212"=>"ADDED", "localhost_11211"=>"ADDED"}', @sock.gets.chomp)

    @sock.write("delete_rttable_sub_nid 111.22.33.44/24\r\n")
    assert_equal('{"localhost_11212"=>"NOT_FOUND", "localhost_11211"=>"NOT_FOUND"}', @sock.gets.chomp)

    @sock.write("delete_rttable_sub_nid 127.0.0.0/24\r\n")
    assert_equal('{"localhost_11212"=>"DELETED", "localhost_11211"=>"DELETED"}', @sock.gets.chomp)

    @sock.write("stat sub\r\n")
    assert_equal('routing.sub_nid {}', @sock.gets.chomp)
    assert_equal('END', @sock.gets.chomp)

    @sock.write("routingdump yaml\r\n")
    dump = []
    while select [@sock], nil, nil, 0.5
      dump << @sock.gets.chomp!
    end
    assert_equal(true, dump.grep(/vm/).empty?)
  end

  def test_clear_rttable_sub_nid
    @sock.write("add_rttable_sub_nid 127.0.0.0/24 localhost vm\r\n")
    assert_equal('{"localhost_11212"=>"ADDED", "localhost_11211"=>"ADDED"}', @sock.gets.chomp)

    @sock.write("clear_rttable_sub_nid\r\n")
    assert_equal('{"localhost_11212"=>"CLEARED", "localhost_11211"=>"CLEARED"}', @sock.gets.chomp)

    @sock.write("stat sub\r\n")
    assert_equal('routing.sub_nid {}', @sock.gets.chomp)
    assert_equal('END', @sock.gets.chomp)

    @sock.write("routingdump yaml\r\n")
    dump = []
    while select [@sock], nil, nil, 0.5
      dump << @sock.gets.chomp!
    end
    assert_equal(true, dump.grep(/vm/).empty?)
  end

  def test_stat_latency
    @sock.write("stat latency\r\n")
    assert_equal("stats.hilatency_warn_time 5.0", @sock.gets.chomp)
    assert_equal("stats.latency_log false", @sock.gets.chomp)
    assert_equal('stats.latency_check_cmd ["get", "set", "delete"]', @sock.gets.chomp)
    assert_equal("stats.latency_check_time_count false", @sock.gets.chomp)
  end

  def test_del_latency_avg_calc_cmd
    @sock.write("del_latency_avg_calc_cmd set delete\r\n")
    assert_equal('{"localhost_11212"=>"DELETED", "localhost_11211"=>"DELETED"}', @sock.gets.chomp)
    @sock.write("stat latency_check_cmd\r\n")
    assert_equal('stats.latency_check_cmd ["get"]', @sock.gets.chomp)
    assert_equal('END', @sock.gets.chomp)
  end

  def test_add_latency_avg_calc_cmd
    @sock.write("add_latency_avg_calc_cmd add\r\n")
    assert_equal('{"localhost_11212"=>"SET", "localhost_11211"=>"SET"}', @sock.gets.chomp)
    @sock.write("stat latency_check_cmd\r\n")
    assert_equal('stats.latency_check_cmd ["get", "set", "delete", "add"]', @sock.gets.chomp)
    assert_equal('END', @sock.gets.chomp)

    @sock.write("add_latency_avg_calc_cmd get\r\n")
    assert_equal('ALREADY SET [get] command', @sock.gets.chomp)
    @sock.write("stat latency_check_cmd\r\n")
    assert_equal('stats.latency_check_cmd ["get", "set", "delete", "add"]', @sock.gets.chomp)
    assert_equal('END', @sock.gets.chomp)

    @sock.write("add_latency_avg_calc_cmd balse\r\n")
    assert_equal('NOT SUPPORT [balse] command', @sock.gets.chomp)
    @sock.write("stat latency_check_cmd\r\n")
    assert_equal('stats.latency_check_cmd ["get", "set", "delete", "add"]', @sock.gets.chomp)
    assert_equal('END', @sock.gets.chomp)
  end

  def test_chg_latency_avg_calc_time_count
    @sock.write("chg_latency_avg_calc_time_count 60\r\n")
    assert_equal('{"localhost_11212"=>"CHANGED", "localhost_11211"=>"CHANGED"}', @sock.gets.chomp)
    @sock.write("stat latency_check_time_count\r\n")
    assert_equal('stats.latency_check_time_count 60', @sock.gets.chomp)
    assert_equal('END', @sock.gets.chomp)

    @sock.write("chg_latency_avg_calc_time_count nil\r\n")
    assert_equal('{"localhost_11212"=>"CHANGED", "localhost_11211"=>"CHANGED"}', @sock.gets.chomp)
    @sock.write("stat latency_check_time_count\r\n")
    assert_equal('stats.latency_check_time_count false', @sock.gets.chomp)
    assert_equal('END', @sock.gets.chomp)
  end

  def test_set_latency_avg_calc_rule
    @sock.write("set_latency_avg_calc_rule off\r\n")
    assert_equal('{"localhost_11212"=>"DEACTIVATED", "localhost_11211"=>"DEACTIVATED"}', @sock.gets.chomp)
    @sock.write("stat latency\r\n")
    assert_equal("stats.hilatency_warn_time 5.0", @sock.gets.chomp)
    assert_equal("stats.latency_log false", @sock.gets.chomp)
    assert_equal('stats.latency_check_cmd []', @sock.gets.chomp)
    assert_equal("stats.latency_check_time_count false", @sock.gets.chomp)
    assert_equal("END", @sock.gets.chomp)

    @sock.write("set_latency_avg_calc_rule on 30 get set\r\n")
    assert_equal('{"localhost_11212"=>"ACTIVATED", "localhost_11211"=>"ACTIVATED"}', @sock.gets.chomp)
    @sock.write("stat latency\r\n")
    assert_equal("stats.hilatency_warn_time 5.0", @sock.gets.chomp)
    assert_equal("stats.latency_log true", @sock.gets.chomp)
    assert_equal('stats.latency_check_cmd ["get", "set"]', @sock.gets.chomp)
    assert_equal("stats.latency_check_time_count 30", @sock.gets.chomp)
    assert_equal("END", @sock.gets.chomp)
  end

end
