require 'test_helper'
require 'logger'
require 'stringio'
require 'roma/write_behind'
require 'roma/client/rclient'
require 'roma/messaging/con_pool'
require 'roma/client/plugin/alist'
require 'roma/client/plugin/map'

module FileWriterTests
  # making and writing test
  def test_wb_write
    system('rm -rf wb_test')
    fw = Roma::WriteBehind::FileWriter.new('wb_test', 1024 * 1024, Logger.new(nil))
    path = "wb_test/roma0_11211/roma/#{Time.now.strftime('%Y%m%d')}/"

    assert(!File.exist?("#{path}/0.wb"))
    100.times do |i|
      fw.write('roma', i, "key-#{i}", "val-#{i}")
    end
    assert(File.exist?("#{path}/0.wb"))
    assert(!File.exist?("#{path}/1.wb"))

    fw.rotate('roma')

    i = 100
    fw.write('roma', i, "key-#{i}", "val-#{i}")
    assert(File.exist?("#{path}/1.wb"))

    fw.close_all

    wb0 = read_wb("#{path}/0.wb")
    assert_equal(100, wb0.length)
    wb0.each do |_last, cmd, key, val|
      assert_equal("key-#{cmd}", key)
      assert_equal("val-#{cmd}", val)
    end
    wb1 = read_wb("#{path}/1.wb")
    assert_equal(1, wb1.length)
  end

  # rotation test per data
  def test_wb_rotation
    system('rm -rf wb_test')
    fw = Roma::WriteBehind::FileWriter.new('wb_test', 900, Logger.new(nil))
    path = "wb_test/roma0_11211/roma/#{Time.now.strftime('%Y%m%d')}/"

    100.times do |i|
      fw.write('roma', 0,
               sprintf('key-%04d', i),
               sprintf('val-%04d', i))
    end

    assert(File.exist?("#{path}/0.wb"))
    assert(File.exist?("#{path}/1.wb"))
    assert(File.exist?("#{path}/2.wb"))
    assert(File.exist?("#{path}/3.wb"))
    assert(!File.exist?("#{path}/4.wb"))
  end

  # rotation test per time
  def test_rotation2
    system('rm -rf wb_test')
    fw = Roma::WriteBehind::FileWriter.new('wb_test', 1024 * 1024, Logger.new(nil))
    path = "wb_test/roma0_11211/roma/#{Time.now.strftime('%Y%m%d')}/"

    # rottime's usec have some value,from instance was created
    rt = fw.instance_eval { @rottime }
    assert_not_equal(0, rt.hour + rt.min + rt.sec + rt.usec)
    # formatting execute in today's date
    assert_equal(Time.now.day, rt.day)
    # confirming the file do not exist
    assert(!File.exist?("#{path}/0.wb"))
    fw.write('roma', 1, 'key', 'val')
    # Open when somenthing to write,and in same timing rottime is updated
    rt = fw.instance_eval { @rottime }
    # In this time, under of date become "0"
    assert_equal(0, rt.hour + rt.min + rt.sec + rt.usec)
    # date become tomorrow
    assert_not_equal(Time.now.day, rt.day)
    10.times do |i|
      fw.write('roma', i, "key-#{i}", "val-#{i}")
    end
    # confirming file is only 1 not over 2
    assert(File.exist?("#{path}/0.wb"))
    assert(!File.exist?("#{path}/1.wb"))

    # set rottime to now forcibly
    fw.instance_eval { @rottime = Time.now }
    # confriming to change rottime
    assert_not_equal(rt, fw.instance_eval { @rottime })
    # When something write, rotation is occured
    fw.write('roma', 1, 'key', 'val')
    assert(File.exist?("#{path}/1.wb"))
    # rottime turn back because of test shold not step over the day.
    assert_equal(rt, fw.instance_eval { @rottime })
  end

  # rotation test from outside
  def test_wb_rotation3
    system('rm -rf wb_test')
    fw = Roma::WriteBehind::FileWriter.new('wb_test', 1024 * 1024, Logger.new(nil))
    path = "wb_test/roma0_11211/roma/#{Time.now.strftime('%Y%m%d')}/"

    # confirming file don't exist
    assert(!File.exist?("#{path}/0.wb"))
    10.times do |i|
      fw.write('roma', i, "key-#{i}", "val-#{i}")
    end
    # confirming file is only 1 not over 2
    assert(File.exist?("#{path}/0.wb"))
    assert(!File.exist?("#{path}/1.wb"))

    fw.rotate('roma')
    10.times do |i|
      fw.write('roma', i, "key-#{i}", "val-#{i}")
    end
    # confirming files are 2 not over 3
    assert(File.exist?("#{path}/0.wb"))
    assert(File.exist?("#{path}/1.wb"))
    assert(!File.exist?("#{path}/2.wb"))

    # test of do rotation continuously
    fw.rotate('roma')
    fw.rotate('roma')
    fw.rotate('roma')
    # confimring file's count are still 2 (not changing).
    assert(File.exist?("#{path}/0.wb"))
    assert(File.exist?("#{path}/1.wb"))
    assert(!File.exist?("#{path}/2.wb"))
    10.times do |i|
      fw.write('roma', i, "key-#{i}", "val-#{i}")
    end
    # confirming files are 3 not over 4
    assert(File.exist?("#{path}/0.wb"))
    assert(File.exist?("#{path}/1.wb"))
    assert(File.exist?("#{path}/2.wb"))
    assert(!File.exist?("#{path}/3.wb"))
  end

  def test_wb_get_current_file_path
    system('rm -rf wb_test')
    fw = Roma::WriteBehind::FileWriter.new('wb_test', 900, Logger.new(nil))

    assert_nil(fw.get_current_file_path('roma'))

    fw.write('roma', 0, 'key', 'val')

    path = File.expand_path("./wb_test/roma0_11211/roma/#{Time.now.strftime('%Y%m%d')}/")
    assert_equal(File.join(path, '0.wb'), fw.get_current_file_path('roma'))

    fw.rotate('roma')
    assert_nil(fw.get_current_file_path('roma'))

    fw.write('roma', 0, 'key', 'val')
    assert_equal(File.join(path, '1.wb'), fw.get_current_file_path('roma'))
  end

  def test_wb_get_path
    system('rm -rf wb_test')
    fw = Roma::WriteBehind::FileWriter.new('wb_test', 900, Logger.new(nil))
    path = File.expand_path('./wb_test/roma0_11211/roma')
    assert_equal(path, fw.wb_get_path('roma'))
  end

  def read_wb(fname)
    ret = []
    open(fname, 'rb') do |f|
      until f.eof?
        b1 = f.read(10)
        last, cmd, klen = b1.unpack('NnN')
        key = f.read(klen)
        b2 = f.read(4)
        vlen = b2.unpack('N')[0]
        val = f.read(vlen)
        ret << [last, cmd, key, val]
      end
    end
    ret
  end
end

class FileWriterTest < Test::Unit::TestCase
  self.test_order = :defined
  include FileWriterTests

  def setup
    @stats = Roma::Stats.instance
    @stats.address = 'roma0'
    @stats.port = 11_211
  end

  def teardown
    system('rm -rf wb_test')
  end
end

class WriteBehindTest < Test::Unit::TestCase
  self.test_order = :defined
  include FileWriterTests
  include RomaTestUtils

  def setup
    start_roma
    @rc = Roma::Client::RomaClient.new(%w(localhost_11211 localhost_11212),
                                       [Roma::Client::Plugin::Alist,
                                        Roma::Client::Plugin::Map])
    system('rm -rf wb')
  end

  def teardown
    stop_roma
    Roma::Messaging::ConPool.instance.close_all
  rescue => e
    puts "#{e} #{$ERROR_POSITION}"
  end

  def test_wb2_stat
    ret = send_cmd('localhost_11211', 'stat wb_command_map')
    assert_equal("stats.wb_command_map {}\r\n", ret)
  end

  def test_wb2_command_map
    send_cmd('localhost_11211', 'wb_command_map {:set=>1}')
    ret = send_cmd('localhost_11211', 'stat wb_command_map')
    assert_equal("stats.wb_command_map {:set=>1}\r\n", ret)
  end

  def test_wb2_set
    send_cmd('localhost_11211', 'wb_command_map {:set=>1}')
    assert_equal('STORED', @rc.set('abc', 'value abc', 0, true))
    send_cmd('localhost_11211', 'writebehind_rotate roma')
    sleep 1

    wb0 = read_wb("#{wb_path}/0.wb")
    assert_equal(1, wb0.length)
    wb0.each do |_last, cmd, key, val|
      # puts "#{cmd} #{key} #{val.inspect}"
      assert_equal(1, cmd)
      assert_equal('abc', key)
      assert_equal('value abc', val)
    end
  end

  def test_wb2_set2
    send_cmd('localhost_11211', 'wb_command_map {:set=>1, :set__prev=>2}')
    assert_equal('STORED', @rc.set('abc', 'val1', 0, true))
    assert_equal('STORED', @rc.set('abc', 'val2', 0, true))
    send_cmd('localhost_11211', 'writebehind_rotate roma')
    sleep 1

    res = [[1, 'abc', 'val1'], [2, 'abc', 'val1'], [1, 'abc', 'val2']]
    wb0 = read_wb("#{wb_path}/0.wb")
    assert_equal(3, wb0.length)
    i = 0
    wb0.each do |_last, cmd, key, val|
      # puts "#{cmd} #{key} #{val.inspect} #{i}"
      assert_equal(res[i][0], cmd)
      assert_equal(res[i][1], key)
      assert_equal(res[i][2], val)
      i += 1
    end
  end

  def test_wb2_storage_commands
    h = { set: 1, delete: 2, add: 3, replace: 4, append: 5, prepend: 6, cas: 7, incr: 8, decr: 9, set_expt: 10 }
    send_cmd('localhost_11211', "wb_command_map #{h}")
    assert_equal('STORED', @rc.set('abc', '1', 0, true))
    assert_equal('DELETED', @rc.delete('abc'))
    assert_equal('STORED', @rc.add('abc', '1', 0, true))
    assert_equal('STORED', @rc.replace('abc', '2', 0, true))
    assert_equal('STORED', @rc.append('abc', '3'))
    assert_equal('STORED', @rc.prepend('abc', '1'))
    res = @rc.cas('abc', 0, true) do |_v|
      v = '128'
    end
    assert_equal('STORED', res)
    assert_equal(129, @rc.incr('abc'))
    assert_equal(128, @rc.decr('abc'))
    res = send_cmd('localhost_11211', 'set_expt abc 100')
    assert_equal('STORED', res.chomp)
    send_cmd('localhost_11211', 'writebehind_rotate roma')
    sleep 1

    res = { 1 => '1', 2 => '1', 3 => '1', 4 => '2', 5 => '23', 6 => '123', 7 => '128', 8 => '129', 9 => '128', 10 => nil }
    wb0 = read_wb("#{wb_path}/0.wb")
    assert_equal(10, wb0.length)
    wb0.each do |_last, cmd, _key, val|
      # puts "#{cmd} #{key} #{val.inspect}"
      assert_equal(res[cmd], val) if res[cmd]
    end
  end

  def test_wb2_storage_commands2
    h = {
      set: 1, set__prev: 11,
      delete: 2, delete__prev: 12,
      add: 3, add__prev: 13,
      replace: 4, replace__prev: 14,
      append: 5, append__prev: 15,
      prepend: 6, prepend__prev: 16,
      cas: 7, cas__prev: 17,
      incr: 8, incr__prev: 18,
      decr: 9, decr__prev: 19,
      set_expt: 10, set_expt__prev: 20
    }
    send_cmd('localhost_11211', "wb_command_map #{h}")
    assert_equal('STORED', @rc.set('abc', '1', 0, true))
    assert_equal('DELETED', @rc.delete('abc'))
    assert_equal('STORED', @rc.add('abc', '1', 0, true))
    assert_equal('STORED', @rc.replace('abc', '2', 0, true))
    assert_equal('STORED', @rc.append('abc', '3'))
    assert_equal('STORED', @rc.prepend('abc', '1'))
    res = @rc.cas('abc', 0, true) do |_v|
      v = '128'
    end
    assert_equal('STORED', res)
    assert_equal(129, @rc.incr('abc'))
    assert_equal(128, @rc.decr('abc'))
    res = send_cmd('localhost_11211', 'set_expt abc 100')
    assert_equal('STORED', res.chomp)

    send_cmd('localhost_11211', 'writebehind_rotate roma')
    sleep 1

    res = [
      [1, 'abc', '1'], [12, 'abc', '1'],
      [2, 'abc', '1'],
      [3, 'abc', '1'], [14, 'abc', '1'],
      [4, 'abc', '2'], [15, 'abc', '2'],
      [5, 'abc', '23'], [16, 'abc', '23'],
      [6, 'abc', '123'], [17, 'abc', '123'],
      [7, 'abc', '128'], [18, 'abc', '128'],
      [8, 'abc', '129'], [19, 'abc', '129'],
      [9, 'abc', '128'],
      [20, 'abc'], [10, 'abc']
    ]

    wb0 = read_wb("#{wb_path}/0.wb")
    assert_equal(18, wb0.length)
    i = 0
    wb0.each do |_last, cmd, key, val|
      # puts "#{cmd} #{key} #{val.inspect}"
      assert_equal(res[i][0], cmd)
      assert_equal(res[i][1], key)
      assert_equal(res[i][2], val) if res.length < 3
      i += 1
    end
  end

  def test_wb2_alist_commands
    h = {
      alist_clear: 1,
      alist_delete: 2,
      alist_delete_at: 3,
      alist_insert: 4,
      alist_sized_insert: 5,
      alist_swap_and_insert: 6,
      alist_swap_and_sized_insert: 7,
      alist_expired_swap_and_insert: 8,
      alist_expired_swap_and_sized_insert: 9,
      alist_push: 10,
      alist_sized_push: 11,
      alist_swap_and_push: 12,
      alist_swap_and_sized_push: 13,
      alist_expired_swap_and_push: 14,
      alist_expired_swap_and_sized_push: 15,
      alist_update_at: 16
    }
    send_cmd('localhost_11211', "wb_command_map #{h}")
    assert_equal('STORED', @rc.alist_push('abc', '1')) # ['1']
    assert_equal('STORED', @rc.alist_insert('abc', 0, '2')) # ['2','1']
    assert_equal('STORED', @rc.alist_sized_insert('abc', 5, '3')) # ['3','2','1']
    assert_equal('STORED', @rc.alist_swap_and_insert('abc', '4')) # ['4','3','2','1']
    assert_equal('STORED', @rc.alist_swap_and_sized_insert('abc', 5, '5')) # ['5','4','3','2','1']
    assert_equal('STORED', @rc.alist_expired_swap_and_insert('abc', 100, '6')) # ['6','5','4','3','2','1']
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_insert('abc', 100, 10, '7')) # ['7','6','5','4','3','2','1']
    assert_equal('STORED', @rc.alist_sized_push('abc', 10, '8')) # ['7','6','5','4','3','2','1','8']
    assert_equal('STORED', @rc.alist_swap_and_push('abc', '9')) # ['7','6','5','4','3','2','1','8','9']
    assert_equal('STORED', @rc.alist_swap_and_sized_push('abc', 10, '10')) # ['7','6','5','4','3','2','1','8','9','10']
    assert_equal('STORED', @rc.alist_expired_swap_and_push('abc', 100, '11')) # ['7','6','5','4','3','2','1','8','9','10','11']
    assert_equal('STORED', @rc.alist_expired_swap_and_sized_push('abc', 100, 12, '12')) # ['7','6','5','4','3','2','1','8','9','10','12']
    assert_equal('STORED', @rc.alist_update_at('abc', 0, '13')) # ['13','6','5','4','3','2','1','8','9','10','12']
    assert_equal('DELETED', @rc.alist_delete('abc', '3')) # ['13','6','5','4','2','1','8','9','10','12']
    assert_equal('DELETED', @rc.alist_delete_at('abc', 1)) # ['13','5','4','2','1','8','9','10','12']
    assert_equal('CLEARED', @rc.alist_clear('abc'))
    send_cmd('localhost_11211', 'writebehind_rotate roma')
    sleep 1   

    res = {
      10 => '1', 4 => '2', 5 => '3', 6 => '4', 7 => '5', 8 => '6', 9 => '7', 11 => '8', 12 => '9',
      13 => '10', 14 => '11', 15 => '12', 16 => '13', 2 => '3', 3 => '6',
      1 => %w(13 5 4 2 1 8 9 10 11 12) }
    wb0 = read_wb("#{wb_path}/0.wb")
    assert_equal(16, wb0.length)
    wb0.each do |_last, cmd, _key, val|
      begin
        val = Marshal.load(val)[0]
      rescue
      end
      # puts "#{cmd} #{key} #{val.inspect}"
      assert_equal(res[cmd], val)
    end
  end

  def test_wb2_map_commands
    h = {
      map_set: 1,
      map_delete: 2,
      map_clear: 3
    }
    send_cmd('localhost_11211', "wb_command_map #{h}")
    assert_equal('STORED', @rc.map_set('abc', 'mapkey1', 'value1'))
    assert_equal('DELETED', @rc.map_delete('abc', 'mapkey1'))
    assert_equal('STORED', @rc.map_set('abc', 'mapkey1', 'value1'))
    assert_equal('CLEARED', @rc.map_clear('abc'))
    send_cmd('localhost_11211', 'writebehind_rotate roma')
    sleep 1   

    res = { 1 => { 'mapkey1' => 'value1' }, 2 => {}, 3 => {} }
    wb0 = read_wb("#{wb_path}/0.wb")
    assert_equal(4, wb0.length)
    wb0.each do |_last, cmd, _key, val|
      begin
        val = Marshal.load(val)
      rescue
      end
      # puts "#{cmd} #{key} #{val.inspect}"
      assert_equal(res[cmd], val)
    end
  end

  def test_wb2_map_commands2
    h = {
      map_set: 1, map_set__prev: 11,
      map_delete: 2, map_delete__prev: 12,
      map_clear: 3, map_clear__prev: 13
    }
    send_cmd('localhost_11211', "wb_command_map #{h}")
    assert_equal('STORED', @rc.map_set('abc', 'mapkey1', 'value1'))
    assert_equal('STORED', @rc.map_set('abc', 'mapkey2', 'value2'))
    assert_equal('DELETED', @rc.map_delete('abc', 'mapkey1'))
    assert_equal('STORED', @rc.map_set('abc', 'mapkey1', 'value1'))
    assert_equal('CLEARED', @rc.map_clear('abc'))
    send_cmd('localhost_11211', 'writebehind_rotate roma')
    sleep 1   

    res = [
      [1, { 'mapkey1' => 'value1' }],
      [11, { 'mapkey1' => 'value1' }], [1, { 'mapkey1' => 'value1', 'mapkey2' => 'value2' }],
      [12, { 'mapkey1' => 'value1', 'mapkey2' => 'value2' }], [2, { 'mapkey2' => 'value2' }],
      [11, { 'mapkey2' => 'value2' }], [1, { 'mapkey2' => 'value2', 'mapkey1' => 'value1' }],
      [13, { 'mapkey2' => 'value2', 'mapkey1' => 'value1' }], [3, {}]

    ]
    wb0 = read_wb("#{wb_path}/0.wb")
    assert_equal(res.length, wb0.length)
    cnt = 0
    wb0.each do |_last, cmd, _key, val|
      begin
        val = Marshal.load(val)
      rescue
      end
      # puts "#{cmd} #{key} #{val.inspect}"
      assert_equal(res[cnt][0], cmd)
      assert_equal(res[cnt][1], val)
      cnt += 1
    end
  end

  def send_cmd(host, cmd)
    con = Roma::Messaging::ConPool.instance.get_connection(host)
    con.write("#{cmd}\r\n")
    ret = con.gets
    con.close
    ret
  end

  def wb_path
    path = "wb/#{wb_hostname}/roma/#{Time.now.strftime('%Y%m%d')}/"
  end

  def wb_hostname
    if File.exist?('wb/localhost_11211')
      'localhost_11211'
    elsif File.exist?('wb/localhost_11212')
      'localhost_11212'
    else
      nil
    end
  end
end
