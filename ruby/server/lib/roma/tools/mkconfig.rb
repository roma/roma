require 'pathname'
require 'yaml'
require 'fileutils'

module Roma

  class Mkconfig
    TREE_TOP = "menu"
    LIB_PATH = Pathname(__FILE__).dirname.parent.parent
    CONFIG_PATH = File.join("roma", "config.rb")
    CONFIG_FULL_PATH = File.expand_path(File.join(LIB_PATH, CONFIG_PATH))
    PLUGIN_DIR = File.expand_path(File.join(LIB_PATH, File.join("roma", "plugin")))
    BNUM_COEFFICIENT = 2 #reccomend1-4.
    TC_FILE = 10
    REDUNDANCY = 2
    DEFAULT_ROMA_CONNECTION = 12
    RUBY_CONNECTION = 10
    JAVA_CONNECTION = 30
    PHP_CONNECTION = 256
    KB = 1024
    GB = 1024 * 1024 * 1024
    OS_MEMORY_SIZE = 2 * GB
    END_MSG = ["exit", "quit", "balse"]

    class Base
      attr_accessor :all, :flag

      def initialize
        flag = false
        @all = YAML.load <<-YAML
        menu:
          name:
          path_name: menu
          message: Please select by number.
          choice:
            - Select storage 
            - Select plugin
            - Calculate File Descriptor
            - Save
          next:
            - storage
            - plugin
            - fd_server
            - save
        storage:
          name: selected_storage
          path_name: storage
          message: Please select by number.
          choice:
            - Ruby Hash
            - Tokyo Cabinet
          next:
            - menu
            - memory
        memory:
          name: memory_size_GB
          path_name:
          message: How big memory size? Please measure in GB.
          next: process
        process:
          name: process_num
          path_name:
          message: How many run ROMA process per machine?
          next: server
        server:
          name: server_num
          path_name:
          message: How many machine run as ROMA server?
          next: data
        data:
          name: data_num
          path_name:
          message: How many data will you store?
          next: key
        key:
          name: key_size_KB
          path_name:
          message: How big is key size per data? Please measure in KB.
          next: value
        value:
          name: value_size_KB
          path_name:
          message: How big is value size per data? Please measure in KB.
          next: menu
        plugin:
          name: selected_plugin
          path_name: plugin
          message: Please select by number.
          choice:
            #{load_path(PLUGIN_DIR) << "Select all plugins"}
          next:
            #{
              r = Array.new
              load_path(PLUGIN_DIR).count.times{ r << "continue" }
              r << "menu"
              r
            }
          store_type: Array
        continue:
          name:
          path_name: 
          message: Please select by number.
          choice:
            - Select more
            - No more
          next:
            - plugin
            - menu
        fd_server:
          name: server_num
          path_name: FileDescriptor
          message: How many machine run as ROMA server?
          next: fd_client
        fd_client:
          name: client_num 
          path_name:
          message: How many machine run as ROMA client?
          next: language
        language:
          name: client_language
          path_name:
          message: Please select programming language of client by number.
          choice:
            - Ruby
            - Java
            - PHP
          next:
            - menu
            - menu
            - menu
        save: END
        YAML
      end

      def keys
        all.keys
      end

      def [](s)
        all[s]
      end

      def load_path(path)
        ret = Array.new
        files = Dir::entries(path)
        files.each do |file|
p file
          ret << file if File::ftype(File.join(path, file)) == "file"
        end
        ret
      end

      def print_question(key)
        target = all[key]

        print "#{target["message"]}\n"

        if target.key?("choice")
          target["choice"].each_with_index do |k, i|
            print "[#{i + 1}] #{k}\n"
          end
        end
      end

      def next (key, input)
        target = all[key]["next"]

        if target.class == Array
          return target[input.to_i - 1]
        else
          return target
        end
      end
    end

    class Input
      module InputReadline
        def get_line
          return Readline.readline("> ", false)
        end
      end

      module InputSimple
        def get_line
          print ">"
          return gets.chomp!
        end
      end

      def initialize
        begin
          require "readline"
        rescue LoadError
          self.extend InputSimple
          return
        end
        self.extend InputReadline
      end
    end

    class Config_status
      attr_accessor :name, :value, :print, :default_value
      def initialize(hash, input, store_type = nil)
        @name = hash["name"]
        @value = input
        if store_type == "Array"
          @value = Array.new
          @value << input
        end
        @print = true
      end
    end

    class Box
      def self.print_edge(width)
        print "+"
          width.times { print "-" }
        print "+\n"
      end

      def self.print_with_box(arg)
        return if arg.count == 0

        if arg.class == Hash
          strs = Array.new
          arg.each do |k, v|
             strs << "#{k}: #{arg[k]}"
          end
          arg = strs
        end

        width = max_length(arg) + 1
        print_edge(width)

        arg.each do |s|
          print "|#{s}"
          (width - s.length).times do
            print " "
          end
          print "|\n"
        end

        print_edge(width)
      end

      private

      def self.max_length(arg)
        max = 0
        arg.each do |s|
          max = s.length if s.length > max
        end
        max
      end
    end

    class Calculate
      def self.get_bnum(res)
        ans = res["data"].value.to_i * BNUM_COEFFICIENT * REDUNDANCY / res["server"].value.to_i / TC_FILE
        return ans
      end

      def self.get_xmsize_max(res)
        ans = (res["memory"].value.to_i * GB - OS_MEMORY_SIZE) / res["process"].value.to_i / TC_FILE
        if ans <= 0
          ans = res["memory"].value.to_i * GB / 2 / res["process"].value.to_i / TC_FILE
        end
        return ans
      end

      def self.get_xmsize_min(res)
        ans = (res["key"].value.to_i * KB + res["value"].value.to_i * KB) * res["data"].value.to_i * REDUNDANCY / res["server"].value.to_i / res["process"].value.to_i / TC_FILE
        ans = ans
      end

      def self.get_fd(res)
        res["fd_server"].value.to_i * connection_num(res) + (res["fd_client"].value.to_i- 1) * DEFAULT_ROMA_CONNECTION * 2
      end

      def self.connection_num(res)
        case res["language"].value
          when "Ruby"
            connection = RUBY_CONNECTION
          when "Java"
            connection = JAVA_CONNECTION
          when "PHP"
            connection = PHP_CONNECTION
        end

        return connection
      end
    end

    def initialize(mode = :no_menu)
      @base = Base.new
      @results = Hash::new
      @next_hash = TREE_TOP
      begin
        @defaults = load_config([:STORAGE_CLASS, :STORAGE_OPTION, :PLUGIN_FILES])
      rescue LoadError
        puts 'Not found config.rb file.'
        return
      rescue
        p $!
        puts "Content of config.rb is wrong."
        return
      end
      mkconfig(mode)
    end

    def load_config(targets)
      require CONFIG_PATH
      d_value = Hash.new
      Config.constants.each do |cnst|
        if targets.include?(cnst)
          d_value[cnst] = Config.const_get(cnst)
        end
      end
      return d_value
    end

    def mkconfig(mode)
      skip = skip_menu!(mode)
      while true
        clear_screen
        skip.call if @next_hash == "menu"
        break if end?(@base[@next_hash])
        Box.print_with_box(@defaults)
        print_status(@results)
        @base.print_question(@next_hash)
        input = get_input(@base[@next_hash])
        if END_MSG.include?(input)
          break if exit?
        else
          @results = store_result(@results, @base, @next_hash, input)
          @next_hash = @base.next(@next_hash, input)
        end
      end
    end

    def clear_screen
      print "\e[H\e[2J"
    end

    def skip_menu!(menu)
      return Proc.new {} if menu == :with_menu

      i = 0
      return Proc.new do
        @next_hash = @base["menu"]["next"][i]
        i += 1
      end
    end

    def end?(s)
      if s == "END"
        save_data(@results)
        true
      end
    end

    def print_status(results)
      strs = Array.new
      results.each_value do |v|
        strs << "#{v.name} : #{v.value}"
      end
      Box.print_with_box(strs)
    end

    def get_input(hash)
      receiver = Input.new
      input = ""

      while !correct_in?(hash,input)
        input = receiver.get_line
      end

      input
    end

    def correct_in?(hash,input)
      if END_MSG.include?(input)
        return true
      end

      if hash == "continue"
        if (input == "y" || input == "n")
          return true
        end
      else
        if hash.key?('choice')
          if hash['choice'].count >= input.to_i && input.to_i > 0
            return true
          end
        else
          if 0 < input.to_i
            return true
          end
        end
      end

      return false
    end

    def exit?
      question = "Settings are not saved. Really exit? [y/n]\n"
      receiver = Input.new
      input = ""

      print question
      input = receiver.get_line
      if input == "y"
        print "bye.\n"
        true
      else
        print "Continue.\n"
        sleep 1
        false
      end
    end

    def store_result(results, base, hash, input)
      target = base[hash]

      return results if !target["name"]

      if target.key?("choice")
        if target["store_type"] == "Array"
          if base.flag
            results[hash].value << target["choice"][input.to_i - 1] if !results[hash].value.include?(target["choice"][input.to_i - 1])
          else
            results[hash] = Config_status.new(target, target["choice"][input.to_i - 1], target["store_type"])
            base.flag = true
          end

          if input.to_i == target["choice"].count
            results[hash].value = target["choice"][0..-2]
          end
        else
          results[hash] = Config_status.new(target, target["choice"][input.to_i - 1])
        end
      else
        results[hash] = Config_status.new(target, input)
      end

      base.flag = false if hash == "menu"
      return results
    end

    def save_data(res)
      if res.key?("storage")
        if res["storage"].value == "Ruby Hash"
          req = "rh_storage"
          storage = "RubyHashStorage"
        end

        if res["storage"].value == "Tokyo Cabinet"
          req = "tc_storage"
          storage = "TCStorage"
          bnum = Calculate.get_bnum(res)
          bnum = 5000000 if bnum < 5000000
          xmsiz = Calculate.get_xmsize_max(res)
        end
      end

      if res.key?("fd_server")
        fd = Calculate.get_fd(res)
        print "\r\nPlease set FD bigger than #{fd}.\r\n\r\n" 
      end

      FileUtils.copy(CONFIG_FULL_PATH, CONFIG_FULL_PATH+".org") if !File.exist?(CONFIG_FULL_PATH+".org")
      FileUtils.copy(CONFIG_FULL_PATH, CONFIG_FULL_PATH+".old")
      open(CONFIG_FULL_PATH, "r+") do |f|
        f.flock(File::LOCK_EX)
        body = f.read

        if req
          body = ch_assign(body, "require", " ", "roma\/storage\/#{req}")
          body = ch_assign(body, "STORAGE_CLASS", "Roma::Storage::#{storage}")

          if req == "rh_storage"
            body = ch_assign(body, "STORAGE_OPTION","")
          end

          if req == "tc_storage"
            body = ch_assign(body, "STORAGE_OPTION", "bnum=#{bnum}\#xmsiz=#{xmsiz}\#opts=d#dfunit=10")
          end
        end

        if res.key?("plugin")
          body = ch_assign(body, "PLUGIN_FILES", res["plugin"].value)
        end

        f.rewind
        f.puts body
        f.truncate(f.tell)
        f.flock(File::LOCK_UN)
      end

      puts "Before"
      Box.print_with_box(@defaults)

      re_require(CONFIG_PATH, CONFIG_FULL_PATH, Config)
      results = load_config([:STORAGE_CLASS, :STORAGE_OPTION, :PLUGIN_FILES])
      print "\r\nAfter\r\n"
      Box.print_with_box(results)
      print "\r\nMkconfig is finish.\r\n\r\n"
    end

    def ch_assign(text, exp, sep = " = ", str)
      sep = " = " if sep == "="
      text = text.gsub(/(\s*#{exp}).*/) do |s|
        name = $1
        if str.class == String
          if str =~ /::/ || str =~ /^\d+$/
            name + sep + str
          else
            name + sep + str.inspect
          end
        else
          name + sep + str.to_s.sub("\\", "")
        end
      end
    end

    def re_require(lib_path, path, c_obj)
      $".delete(File.expand_path(path))
      c_obj.constants.each do |cnst|
        c_obj.class_eval { remove_const cnst }
      end
      require lib_path
    end

  end # Mkconfig
end # module Roma
