#!/usr/bin/ruby
# encoding: utf-8
require 'socket'
require 'ipaddr'
require 'optparse'
UDP_PORT=14329
MULTICAST_ADDR="225.0.0.123"
ROMA_LOAD_PATH=File.expand_path(File.join(File.dirname(__FILE__),".."))
RUBY_COMMAND_OPTIONS=["-I",ROMA_LOAD_PATH]
ROMAD_OPTIONS=["--enabled_repeathost"]
ROMAD_PATH= File.expand_path(File.join(File.dirname(__FILE__),"romad.rb"))
MKROUTE_PATH= File.expand_path(File.join(File.dirname(__FILE__),"mkroute.rb"))

ROMAD_WORK_DIR='.'
class Tribunus
  #protocol
  #
  #update:<reply_ocunt>: <romad_hostname> [<romad_port> ...]

  RomadHost=Struct.new(:hostname,:ports,:updated_at)
  def log(obj)
    if @verbose
      if obj.is_a? String
        $stderr.puts obj
      else
        $stderr.puts obj.inspect
      end
    end
  end
  private :log

  def initialize(romad_hostname,romad_ports,options={})
    @multi_addr=options[:multicast_addr]||MULTICAST_ADDR
    @port=options[:udp_port]||UDP_PORT
    @romad_work_dir=options[:romad_work_dir]||ROMAD_WORK_DIR
    @ruby_command_name=options[:ruby_command_name]||"ruby"
    @verbose=options[:verbose]
    log [:initalized,@multi_addr,@port,@romad_work_dir,@ruby_command_name]

    @threads=[]
    @romads={} #port => pid
    @local_romad_host=RomadHost.new(romad_hostname,romad_ports,nil)

    @mutex=Mutex.new
    @remote_servers={} #ipaddr => RomadHost

  end
  
  def from_remote?(ipaddr)
      from_remote= !Socket.ip_address_list.any?{|addr|addr.ip_address==ipaddr}
  end
  private :from_remote?


  def spawn_new_roma_ring
    spawn_romads(nil,nil)
  end


  def spawn_romads(remote_host,remote_port)
    nodes=@local_romad_host.ports.map do|port|
      "#{@local_romad_host.hostname}_#{port}"
    end
    nodes << "#{remote_host}_#{remote_port}" if remote_host
    pid=Process.spawn(@ruby_command_name,*RUBY_COMMAND_OPTIONS,MKROUTE_PATH,*nodes,:chdir=>@romad_work_dir)
    Process.waitpid(pid)
    
    @local_romad_host.ports.each do|port|
      pid=Process.spawn(@ruby_command_name,*RUBY_COMMAND_OPTIONS,ROMAD_PATH,*ROMAD_OPTIONS,"-p",port.to_s,@local_romad_host.hostname, :chdir=>@romad_work_dir)
      @romads[port]=pid
    end
  end

  def spawn_romads_join(remote_host,remote_port)
    @local_romad_host.ports.map do|port|
      spawn_romad_join(port,remote_host,remote_port)
    end
  end


  def spawn_romad_join(port,remote_host,remote_port)
    pid=Process.spawn(@ruby_command_name,*RUBY_COMMAND_OPTIONS,ROMAD_PATH,*ROMAD_OPTIONS,"-p",port.to_s,@local_romad_host.hostname,"-j","#{remote_host}_#{remote_port}",:chdir=>@romad_work_dir)
    @romads[port]=pid
  end


  def receive_update_command(ipaddr,reply_count,params)
    param_ary=params.strip.split(/\s+/)
    unless param_ary.empty?
      hostname=param_ary[0]
      ports=param_ary[1..-1].map{|port| port.to_i}
      rhost=RomadHost.new(hostname,ports,Time.now)
      @remote_servers[ipaddr]=rhost
      if reply_count>0
        unicast(ipaddr,update_message(reply_count-1))
      end
    end
  end

  def run_command(ipaddr,msg)
    match_data=/\A(.*):(\d+):(.*)/.match(msg)
    if(match_data)
      command=match_data[1]
      reply_count=match_data[2].to_i
      rest=match_data[3]
      case command
      when "update"
        receive_update_command(ipaddr,reply_count,rest)
      end
    end
  end
  private :run_command

  def update_message(reply_count,initial=false)
    msg="update:#{reply_count}: #{@local_romad_host.hostname}"
    if !initial && !@romads.keys.empty?
      msg+=" "
      msg+= @romads.keys.join(' ')
    end
    log [:msg, msg,@romads]
    msg
  end

  def server_loop
    sock=UDPSocket.new
    sock.bind('0.0.0.0',@port)
    sock.setsockopt(Socket::IPPROTO_IP, Socket::IP_ADD_MEMBERSHIP, IPAddr.new(MULTICAST_ADDR).hton+IPAddr.new('0.0.0.0').hton)
    log 'start_listen'
    Socket.udp_server_loop_on([sock]) do|msg,msg_src|
        log [:received ,msg,msg_src]
      if from_remote?(msg_src.remote_address.ip_address)
        run_command(msg_src.remote_address.ip_address,msg)
      end
    end
  end

  HEARTBEAT_SECONDS=300
  HEARTBEAT_LOOP_INTERVAL=50
  TIMEOUT_SECONDS=600


  def heartbeat_loop
    loop do
      delete_ipaddrs=[]
      @remote_servers.each do|ipaddr,host|

        if host.updated_at+TIMEOUT_SECONDS < Time.now
          delete_ipaddrs << ipaddr
        elsif host.updated_at+HEARTBEAT_SECONDS < Time.now
          unicast(ipaddr,update_message(0))
        elsif host.ports.empty?
          unicast(ipaddr,update_message(1))
        end
      end

      @mutex.synchronize do
        delete_ipaddrs.each do|ipaddr|
          @remote_servers.delete(ipaddr)
        end
      end


      

      sleep(HEARTBEAT_LOOP_INTERVAL)
    end
  rescue =>e
    p e
  end

  def set_trap
    [:INT,:TERM,:HUP].each do|sig|
      Signal.trap(sig){ Process.kill(sig,*romads.values)  }
    end
  end

  def prepare_to_start
    set_trap
    @threads << Thread.start{self.server_loop}
    @threads << Thread.start{self.heartbeat_loop}
  end
  private :prepare_to_start

  def start_new_ring
    prepare_to_start
    spawn_new_roma_ring
  end

  def choose_rhost
    @remote_servers.each do|ipaddr,rhost|
      unless rhost.ports.empty?
        return rhost
      end
    end
    nil
  end
  def start_join(host,port)
    prepare_to_start
    sleep(0.2)
    spawn_romads_join(host,port)
  end

  def start_discover
    prepare_to_start
    sleep(0.2)
    multicast(update_message(1,true))
    10.times{sleep(0.3)}
    rhost=choose_rhost
    if rhost
      spawn_romads_join(rhost.hostname,rhost.ports.first)
    else
      $stderr.puts "no server responded"
      exit 1
    end

  end

  def join
    @threads.each{|t|t.join}
  end

  def unicast(ipaddr,msg)
    log [:message, ipaddr,msg]
    s=UDPSocket.new
    begin
      s.connect(ipaddr,@port)
      s.sendmsg(msg)
    ensure
        s.close
    end
  end

  def multicast(msg)
    unicast(@multi_addr,msg)
  end
end

opt=OptionParser.new
conf={}
opt.on('-d','discover the node by multicast [default]') do|v|
  conf[:mode]=:discover
end
opt.on('-c','craete new ring') do|v|
  conf[:mode]=:new_ring
end
opt.on('-j HOST:PORT',/\A.+?[_:]\d+\Z/,'join the specified romad node') do|v|
  conf[:mode]=:join
  node=v.split(/[_:]/)
  conf[:joining_node]=[node[0],node[1].to_i]
end

opt.on('-p UDP_PORT',/\A\d+\Z/,'the port for multicast') do|v|
  conf[:udp_port]=v.to_i
end
opt.on('-w WORKING_DIR','the directory where romads run') do|v|
  conf[:romad_work_dir]=v
end
opt.on('-m MULTICAST_ADDRESS',/\A\d+\.\d+\.\d+\.\d+\Z/,'the ip address for multicast') do|v|
  conf[:multicast_addr]=v
end
opt.on('-r RUBY_COMMAND','name of ruby interpreter (default: "ruby")') do|v|
  conf[:ruby_command_name]=v
end
opt.on('-v','--verbose') do|v|
  conf[:verbose]=v
end
opt.banner += " hostname port_range"

opt.parse!(ARGV)

if ARGV.size!=2
  puts opt.help
  exit 1
end
hostname=ARGV[0]
port_ary=ARGV[1].split('-')
ports=(port_ary[0].to_i..port_ary[1].to_i).to_a
if ports.size < 2
  puts 'less ports'
  exit 1
end
if ports.size >100
  puts 'too many ports'
  exit 1
end

tri=Tribunus.new(hostname,ports,conf)
case conf[:mode] 
when:new_ring
  tri.start_new_ring
when :join 
  tri.start_join(conf[:joining_node][0],conf[:joining_node][1])
else
  tri.start_discover
end
tri.join
