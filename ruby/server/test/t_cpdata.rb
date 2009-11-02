#!/usr/bin/env ruby
# -*- coding: utf-8 -*-
require 'test/unit'

path =  File.dirname(File.expand_path($PROGRAM_NAME))
$LOAD_PATH << path + "/../lib"
$LOAD_PATH << path  + "/../../commons/lib"
$LOAD_PATH << path  + "/../../client/lib"

require 'rbconfig'
require 'shell'
require 'roma/client/rclient'
require 'timeout'

def start_roma
  ruby_path = File.join(RbConfig::CONFIG["bindir"],
                        RbConfig::CONFIG["ruby_install_name"])
  path =  File.dirname(File.expand_path($PROGRAM_NAME))
  sh = Shell.new
  sh.transact do
    Dir.glob("localhost_1121?.*").each{|f| rm f }
  end
  rm_rf("localhost_11211")
  rm_rf("localhost_11212")
  
  sh.system(ruby_path,"#{path}/../bin/mkroute",
            "localhost_11211","localhost_11212",
            "-d","3",
            "--enabled_repeathost")
  sh.system(ruby_path,"#{path}/../bin/romad","localhost","-p","11211","-d","--verbose")
  sh.system(ruby_path,"#{path}/../bin/romad","localhost","-p","11212","-d","--verbose")
  sleep 2
end


def stop_roma
  puts "#{__method__}"
  conn = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")
  conn.write "balse\r\n"
  conn.gets
  conn.write "yes\r\n"
  conn.gets
  conn.close
  Roma::Messaging::ConPool.instance.close_all
rescue =>e
  puts "#{e}"
end

# looked like a "rm -rf" command
def rm_rf(fname)
  return unless File::exist?(fname)
  if File::directory?(fname)
    Dir["#{fname}/*"].each{|f| rm_rf(f) }
    Dir.rmdir(fname)
  else
    File.delete(fname)
  end
end

$dat = {}

def receive_command_server
  $gs = TCPServer.open(11213)
  while true
    Thread.new($gs.accept){|s|
      begin
        loop {
          res = s.gets
          p res
          if res==nil
            s.close
          elsif res.start_with?("pushv")
            ss = res.split(" ")
            s.write("READY\r\n")
            len = s.gets.chomp
            $dat[ss[2].to_i] = receive_dump(s, len.to_i)
            s.write("STORED\r\n")
          elsif res.start_with?("spushv")
            ss = res.split(" ")
            s.write("READY\r\n")
            $dat[ss[2].to_i] = receive_stream_dump(s)
            s.write("STORED\r\n")
          elsif res.start_with?("whoami")
            s.write("ROMA\r\n")
          elsif res.start_with?("rbalse")
            s.write("BYE\r\n")
            s.close
            break
          else
            s.write("STORED\r\n")
          end
        }
      rescue =>e
        p e
        p $@
      end
    }
  end
rescue =>e
  p e
end

def receive_stream_dump(sok)
  ret = {}
  v = nil
  loop {
    context_bin = sok.read(20)
    vn, last, clk, expt, klen = context_bin.unpack('NNNNN')

    break if klen == 0 # end of dump ?
    k = sok.read(klen)
    vlen_bin = sok.read(4)
    vlen, =  vlen_bin.unpack('N')
    if vlen != 0
      v = sok.read(vlen)
    end
    ret[k] = [vn, last, clk, expt, v].pack('NNNNa*')
  }
  ret
rescue =>e
  p e
end

def receive_dump(sok, len)
  dmp = ''
  while(dmp.length != len.to_i)
    dmp = dmp + sok.read(len.to_i - dmp.length)
  end
  sok.read(2)
  if sok.gets == "END\r\n"
    return Marshal.load(dmp)
  else
    return nil
  end
rescue =>e
  false
end


MiniTest::Unit.class_eval{
  alias run2 run
  undef run

  def run args = []
    Thread.new{ receive_command_server }
#    start_roma
    run2 args
#    stop_roma
    
  end
}

# vnode をコピーするテスト
class CopyDataTest < Test::Unit::TestCase

  def setup
#    @th = Thread.new{ receive_command_server }
    start_roma
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
  end

  def teardown
    stop_roma
#    @th.kill
  end

  def test_pushv
    make_dummy(1000)
    dat = reqpushv('roma',0)
    assert_not_nil( dat )
    # 正常ケース
    con = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")
    assert_equal("STORED", push_a_vnode('roma',0,con,Marshal.dump(dat)))

    # 存在しない仮想ストレージ
    con = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")
    assert_equal("SERVER_ERROR @storages[roma1] dose not found.",
                 push_a_vnode('roma1',0,con,Marshal.dump(dat)))

    # END を送らない
    con = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")
    assert_equal("SERVER_ERROR END was not able to be received.",
                 push_a_vnode('roma',0,con,Marshal.dump(dat),true))
    
    # 壊れたデータを送る
    dat['abc']="ajjkdlfsoifulwkejrweorlkjflksjflskaf"
    con = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")
    assert_equal(
      "SERVER_ERROR An invalid vnode number is include.key=abc vn=1634364011",
       push_a_vnode('roma',2,con,Marshal.dump(dat)))
  end

  def push_a_vnode(hname ,vn, con, dmp, nonend=false)
    con.write("pushv #{hname} #{vn}\r\n")
    res = con.gets # READY\r\n or error string
    if res != "READY\r\n"
      con.close
      return res.chomp
    end
    if nonend
      con.write("#{dmp.length}\r\n#{dmp}\r\n\r\n")
    else
      con.write("#{dmp.length}\r\n#{dmp}\r\nEND\r\n")
    end
    res = con.gets # STORED\r\n or error string
    con.close
    res.chomp! if res
    res
  rescue =>e
    con.close if con
    "#{e}"
  end
  private :push_a_vnode
  

  def test_spushv
    # vn = 0 のキー
    keys = []
    n = 1000
    n.times{|i|
      d = Digest::SHA1.hexdigest(i.to_s).hex % @rc.rttable.hbits
      vn = @rc.rttable.get_vnode_id(d)
      if vn == 0
        keys << i.to_s
      end
    }
    nid = @rc.rttable.search_nodes(0)

    push_a_vnode_stream('roma', 0, nid[0], keys)

    keys.each{|k|
      assert_equal( "#{k}-stream", @rc.get(k))
#      puts "#{k} #{@rc.get(k)}"
    }
  end

  def push_a_vnode_stream(hname, vn, nid, keys)
    con = Roma::Messaging::ConPool.instance.get_connection(nid)
    con.write("spushv #{hname} #{vn}\r\n")
    
    res = con.gets # READY\r\n or error string
    if res != "READY\r\n"
      con.close
      return res.chomp
    end

    keys.each{|k|
      v = k + "-stream"
      data = [vn, Time.now.to_i, 1, 0x7fffffff, k.length, k, v.length, v].pack("NNNNNa#{k.length}Na#{v.length}")
      con.write(data)
    }
    con.write("\0"*20) # end of steram
    
    res = con.gets # STORED\r\n or error string
    Roma::Messaging::ConPool.instance.return_connection(nid,con)
    res.chomp! if res
    res
  rescue =>e
    "#{e}"
  end
  private :push_a_vnode_stream


  def test_reqpushv
    make_dummy(1000)

    dat=[]
    dat[0] = reqpushv('roma',0)
    assert_not_nil( dat[0] )
    dat[0] = reqpushv('roma',0)
    assert_not_nil( dat[0] )  # 同じ vnode を2度アクセスしても良いことを確認

    dat[1] = reqpushv('roma',536870912)
    assert_not_nil( dat[1] )
    dat[2] = reqpushv('roma',1073741824)
    assert_not_nil( dat[2] )
    dat[3] = reqpushv('roma',1610612736)
    assert_not_nil( dat[3])
    dat[4] = reqpushv('roma',2147483648)
    assert_not_nil( dat[4] )
    dat[5] = reqpushv('roma',2684354560)
    assert_not_nil( dat[5] )
    dat[6] = reqpushv('roma',3221225472,true)
    assert_not_nil( dat[6] )
    dat[7] = reqpushv('roma',3758096384,true)
    assert_not_nil( dat[7] )

    a = 0
    dat.each{|v| a+=v.length }
    assert_equal( 1000,a )
  end

  def wait(vn)
    while $dat.key?(vn) do
      sleep 0.01
    end
    $dat[vn]
  end

  # n 個の dummy data を set
  def make_dummy(n)
    n.times{|i|
      assert( @rc.set(i.to_s,i.to_s)=="STORED" )
    }
  end

  def reqpushv(hname,vn,is_primary=false)
    $dat.delete(vn)
    con = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")
    con.write("reqpushv #{vn} localhost_11213 #{is_primary}\r\n")
    res = con.gets
    assert_equal( "PUSHED\r\n", res )
    con.close

    until $dat.key?(vn) do
      Thread.pass
      sleep 0.01
    end
    $dat[vn]
  rescue =>e
    p e
    p $@
    return nil
  end

  def receive_dump(sok, len)
    dmp = ''
    while(dmp.length != len.to_i)
      dmp = dmp + sok.read(len.to_i - dmp.length)
    end
    sok.read(2)
    if sok.gets == "END\r\n"
      return Marshal.load(dmp)
    else
      return nil
    end
  rescue =>e
    @log.error("#{e}\n#{$@}")
    false
  end
  
end
