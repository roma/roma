require 'shell'

require 'pathname'
require 'fileutils'
require 'rbconfig'

module RomaTestUtils
  module_function
  def base_dir
    Pathname(__FILE__).dirname.parent.parent.expand_path
  end

  def server_base_dir
    base_dir + "server"
  end

  def server_bin_dir
    server_base_dir + "bin"
  end

  def mkroute_path
    (server_bin_dir + "mkroute").to_s
  end

  def romad_path
    (server_bin_dir + "romad").to_s
  end

  def ruby_path
    File.join(RbConfig::CONFIG["bindir"],
              RbConfig::CONFIG["ruby_install_name"])
  end

  def start_roma
    sh = Shell.new
    sh.transact do
      Dir.glob("localhost_1121?.*").each{|f| rm f }
    end
    FileUtils.rm_rf("localhost_11211")
    FileUtils.rm_rf("localhost_11212")
    sleep 1

    sh.system(ruby_path, mkroute_path,
              "localhost_11211","localhost_11212",
              "-d","3",
              "--enabled_repeathost")
    sleep 1
    sh.system(ruby_path,romad_path,"localhost","-p","11211","-d","--verbose")
    sh.system(ruby_path,romad_path,"localhost","-p","11212","-d","--verbose")
    sleep 2
  end

  def stop_roma
    conn = Roma::Messaging::ConPool.instance.get_connection("localhost_11211")
    if conn
      conn.write "balse\r\n"
      conn.gets
      conn.write "yes\r\n"
      conn.gets
      conn.close
    end
  rescue =>e
    puts "#{e} #{$@}"
  end
end
