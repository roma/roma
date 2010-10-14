#!/usr/bin/env ruby
require 'roma/command/command_definition'
require 'digest/sha1'

module CommandModuleTest1
  include Roma::Command::Definition

  def_command_with_relay :balse do |s|
    command_argument_validation_num s, 3
    case s[2]
    when 'ex_runtime'
      raise s[1]
    when 'ex_client'
      raise Roma::Command::Definition::ClientErrorException, s[1]
    when 'ex_server'
      raise Roma::Command::Definition::ServerErrorException, s[1]
    end
    s
  end
  
  def_command_with_key :get, :no_forward do |ctx|
    case ctx.argv[2]
    when 'ex_runtime'
      raise ctx.argv[1]
    when 'ex_client'
      raise Roma::Command::Definition::ClientErrorException, ctx.argv[1]
    when 'ex_server'
      raise Roma::Command::Definition::ServerErrorException, ctx.argv[1]
    end
    ctx
  end

  def_command_with_key_value :set, 4, :no_forward do |ctx|
    case ctx.argv[2]
    when 'ex_runtime'
      raise ctx.argv[1]
    when 'ex_client'
      raise Roma::Command::Definition::ClientErrorException, ctx.argv[1]
    when 'ex_server'
      raise Roma::Command::Definition::ServerErrorException, ctx.argv[1]
    end
    ctx
  end
end

module CommandModuleTest2
  include Roma::Command::Definition

  def_command_with_relay :runtimeexception do |s|
    raise
  end
end

module CommandModuleTest3
  include Roma::Command::Definition

  def_command_with_relay :balse do |s|
    "override"
  end
  
end

class DefCmdTest
  include Roma::Command::Definition
  include CommandModuleTest1
  include CommandModuleTest2

  #
  # define stub for test
  #

  class RtStub
    attr_reader :hbits

    def initialize
      @hbits = 10
    end

    def get_vnode_id(d) 1 end
    def search_nodes_for_write(vn) ['roma0','roma1'] end
  end

  class LogStub
    def warn str
    end
  end

  class StorageStub
    def get_raw(*arg)
      nil
    end
  end

  def initialize
    @stats = Struct.new(:ap_str).new(:ap_str)
    @rttable = RtStub.new
    @defhash = 'roma'
    @log = LogStub.new
    @storages = Hash.new(StorageStub.new)
  end

  def send_data str
    str
  end

  def broadcast_cmd str
    {:broadcast_cmd=>str}
  end

  def read_bytes n
    ret = 'a'
    (n - 1).times{ ret += ret[-1].succ }
    ret
  end

  def command_argument_validation_num s, num
    raise ClientErrorException, "number of arguments (#{s.length - 1} for #{num - 1})" if s.length != num
  end
end

class DefineCommandTest < Test::Unit::TestCase

  def setup
    @obj = DefCmdTest.new
  end

  def teardown
  end

  def test_defcmd
    # normal case
    res = @obj.ev_balse ['balse','arg1','arg2']
    res = eval res.chomp
    assert_equal ['balse','arg1','arg2'], res[:ap_str]
    assert_equal "rbalse arg1 arg2\r\n", res[:broadcast_cmd]

    res = @obj.ev_rbalse ['balse','arg1','arg2']
    res = eval res.chomp
    assert_equal ['balse','arg1','arg2'], res

    # case of argument error
    res = @obj.ev_balse ['balse','arg1']
    assert_equal "CLIENT_ERROR number of arguments (1 for 2)\r\n", res
    res = @obj.ev_balse ['balse','arg1', 'arg2', 'arg3']
    assert_equal "CLIENT_ERROR number of arguments (3 for 2)\r\n", res

    # case of exception occur
    assert_raise RuntimeError do
      res = @obj.ev_balse ['balse','arg1','ex_runtime']
    end
    res = @obj.ev_balse ['balse','arg1','ex_client']
    assert_equal "CLIENT_ERROR arg1\r\n", res
    res = @obj.ev_balse ['balse','arg2','ex_server']
    assert_equal "SERVER_ERROR arg2\r\n", res
  end

  def test_defcmd2
    # define a command in DefCmdTest class is not define yet
    DefCmdTest.class_eval do
      def_command_with_relay :balse2 do |s|
        "#{s.join(' ')} #{@stats.ap_str}"
      end
    end
    assert true

    # define the duplicated command in DefCmdTest scope
    assert_raise RuntimeError do
      DefCmdTest.class_eval do
        def_command_with_relay :balse do |s|
          "#{s.join(' ')} #{@stats.ap_str}"
        end
      end
    end

    # define the duplicated command in CommandModuleTest3 scope
    # RuntimeError is nothing to occur
    DefCmdTest.class_eval do
      include CommandModuleTest3
    end
    
    res = @obj.ev_balse ['balse','arg1','arg2']
    robj = eval res.chomp # String -> Hash
    assert_equal "override", robj[:ap_str]
    assert_equal "rbalse arg1 arg2\r\n", robj[:broadcast_cmd]

    res = @obj.ev_rbalse ['balse','arg1','arg2']
    assert_equal "override\r\n", res

    # test for exception occured in a command
    assert_raise RuntimeError do
      @obj.ev_runtimeexception ['balse']
    end
  end
  
  def test_defcmd_k
    # normal case
    ctx = @obj.ev_get ['get','arg1']
    assert_equal ['get','arg1'], ctx.argv
    assert_equal 'arg1', ctx.params.key
    assert_equal 'roma', ctx.params.hash_name
    assert_equal Digest::SHA1.hexdigest('arg1').hex % 10, ctx.params.digest
    assert_equal 1, ctx.params.vn
    assert_equal ['roma0','roma1'], ctx.params.nodes

    ctx = @obj.ev_get ['get',"arg2\eruby"]
    assert_equal ['get',"arg2\eruby"], ctx.argv
    assert_equal 'arg2', ctx.params.key
    assert_equal 'ruby', ctx.params.hash_name
    assert_equal 1, ctx.params.vn
    assert_equal Digest::SHA1.hexdigest('arg2').hex % 10, ctx.params.digest
    assert_equal ['roma0','roma1'], ctx.params.nodes
    
    # case of argument error
    res = @obj.ev_get ['get']
    assert_equal "CLIENT_ERROR dose not find key\r\n", res
    res = @obj.ev_get []
    assert_equal "CLIENT_ERROR dose not find key\r\n", res

    # case of exception occur
    assert_raise RuntimeError do
      res = @obj.ev_get ['get','arg1','ex_runtime']
    end
    res = @obj.ev_get ['get','arg1','ex_client']
    assert_equal "CLIENT_ERROR arg1\r\n", res
    res = @obj.ev_get ['get','arg2','ex_server']
    assert_equal "SERVER_ERROR arg2\r\n", res
  end
  
  def test_defcmd_kv
    # normal case
    ctx = @obj.ev_set ['set','arg1', '0', '0', '5']
    assert_equal ['set','arg1', '0', '0', '5'], ctx.argv
    assert_equal 'arg1', ctx.params.key
    assert_equal 'roma', ctx.params.hash_name
    assert_equal Digest::SHA1.hexdigest('arg1').hex % 10, ctx.params.digest
    assert_equal 1, ctx.params.vn
    assert_equal ['roma0','roma1'], ctx.params.nodes
    assert_equal 'abcde', ctx.params.value

    ctx = @obj.ev_set ['set',"arg2\eruby", '0', '0', '5']
    assert_equal ['set',"arg2\eruby", '0', '0', '5'], ctx.argv
    assert_equal 'arg2', ctx.params.key
    assert_equal 'ruby', ctx.params.hash_name
    assert_equal Digest::SHA1.hexdigest('arg2').hex % 10, ctx.params.digest
    assert_equal 1, ctx.params.vn
    assert_equal ['roma0','roma1'], ctx.params.nodes
    assert_equal 'abcde', ctx.params.value

    # case of argument error
    res = @obj.ev_set ['set']
    assert_equal "CLIENT_ERROR dose not find key\r\n", res
    res = @obj.ev_set []
    assert_equal "CLIENT_ERROR dose not find key\r\n", res

    # case of exception occur
    assert_raise RuntimeError do
      res = @obj.ev_set ['set','arg1','ex_runtime']
    end
    res = @obj.ev_set ['set','arg1','ex_client']
    assert_equal "CLIENT_ERROR arg1\r\n", res
    res = @obj.ev_set ['set','arg2','ex_server']
    assert_equal "SERVER_ERROR arg2\r\n", res
  end
end
