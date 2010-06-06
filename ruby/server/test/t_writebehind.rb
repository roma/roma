#!/usr/bin/env ruby
# -*- coding: utf-8 -*-
require 'logger'
require 'stringio'
require 'roma/write_behind'

class FileWriterTest < Test::Unit::TestCase
  
  def initialize(arg)
    super(arg)
  end

  def setup
    @stats = Roma::Stats.instance
    @stats.address = 'roma0'
    @stats.port = 11211

    @log = Logger.new(StringIO.new)
    @log.level = Logger::INFO
  end

  def teardown
    system('rm -rf wb_test')
  end

  # 作成と write のテスト
  def test_write
    system('rm -rf wb_test')
    fw = Roma::WriteBehind::FileWriter.new("wb_test", 1024 * 1024, @log)
    path = "wb_test/roma0_11211/roma/#{Time.now.strftime('%Y%m%d')}/"

    assert(!File.exist?("#{path}/0.wb"))
    100.times{|i|
      fw.write('roma',i,"key-#{i}","val-#{i}")
    }
    assert(File.exist?("#{path}/0.wb"))
    assert(!File.exist?("#{path}/1.wb"))

    fw.rotate('roma')

    i = 100
    fw.write('roma',i,"key-#{i}","val-#{i}")
    assert(File.exist?("#{path}/1.wb"))

    fw.close_all

    wb0 = read_wb("#{path}/0.wb")
    assert(100,wb0.length )
    wb0.each{|last, cmd, key, val|
      assert_equal( "key-#{cmd}",key)
      assert_equal( "val-#{cmd}",val)
    }
    wb1 = read_wb("#{path}/1.wb")
    assert_equal(1,wb1.length )
  end

  # サイズによるローテーションのテスト
  def test_rotation
    system('rm -rf wb_test')
    fw = Roma::WriteBehind::FileWriter.new("wb_test", 900, @log)
    path = "wb_test/roma0_11211/roma/#{Time.now.strftime('%Y%m%d')}/"

    100.times{|i|
      fw.write('roma',0,
               sprintf("key-%04d",i),
               sprintf("val-%04d",i))
    }

    assert(File.exist?("#{path}/0.wb"))
    assert(File.exist?("#{path}/1.wb"))
    assert(File.exist?("#{path}/2.wb"))
    assert(File.exist?("#{path}/3.wb"))
    assert(!File.exist?("#{path}/4.wb"))
  end

  # 時間によるローテーションのテスト
  def test_rotation2
    system('rm -rf wb_test')
    fw = Roma::WriteBehind::FileWriter.new("wb_test", 1024 * 1024, @log)
    path = "wb_test/roma0_11211/roma/#{Time.now.strftime('%Y%m%d')}/"

    # インスタンス生成直後の rottime の時分秒usecは何かの値を持っている
    rt = fw.instance_eval{ @rottime }
    assert_not_equal(0, rt.hour + rt.min + rt.sec+ rt.usec)
    # 初期化は今日の日付で行われる
    assert_equal(Time.now.day, rt.day)
    # ファイルは存在しない
    assert(!File.exist?("#{path}/0.wb"))
    fw.write('roma',1,"key","val")
    # 何かを書き込むとオープンされ、そのタイミングで rottime が更新される
    rt = fw.instance_eval{ @rottime }
    # この時、日付以下は 0 となる
    assert_equal(0, rt.hour + rt.min + rt.sec+ rt.usec)
    # 日付は明日になる
    assert_not_equal(Time.now.day, rt.day)
    10.times{|i|
      fw.write('roma',i,"key-#{i}","val-#{i}")
    }
    # ファイルは1つ
    assert(File.exist?("#{path}/0.wb"))
    assert(!File.exist?("#{path}/1.wb"))
    
    # ローテーションの時刻を強制的に今にする
    fw.instance_eval{ @rottime=Time.now }
    # rottime の変更を確認
    assert_not_equal(rt, fw.instance_eval{ @rottime })
    # 何かを書き込むとローテーションが発生する
    fw.write('roma',1,"key","val")
    assert(File.exist?("#{path}/1.wb"))
    # テストは日をまたがないので rottime は元に戻る
    assert_equal(rt, fw.instance_eval{ @rottime })
  end

  # 外部からローテーションするテスト
  def test_rotation3
    system('rm -rf wb_test')
    fw = Roma::WriteBehind::FileWriter.new("wb_test", 1024 * 1024, @log)
    path = "wb_test/roma0_11211/roma/#{Time.now.strftime('%Y%m%d')}/"

    # ファイルはない
    assert(!File.exist?("#{path}/0.wb"))
    10.times{|i|
      fw.write('roma',i,"key-#{i}","val-#{i}")
    }
    # ファイルは1つ
    assert(File.exist?("#{path}/0.wb"))
    assert(!File.exist?("#{path}/1.wb"))

    fw.rotate('roma')
    10.times{|i|
      fw.write('roma',i,"key-#{i}","val-#{i}")
    }
    # ファイルは2つ
    assert(File.exist?("#{path}/0.wb"))
    assert(File.exist?("#{path}/1.wb"))
    assert(!File.exist?("#{path}/2.wb"))

    # ローテーションの重複呼び出し
    fw.rotate('roma')
    fw.rotate('roma')
    fw.rotate('roma')
    # ファイルは2つで変化なし
    assert(File.exist?("#{path}/0.wb"))
    assert(File.exist?("#{path}/1.wb"))
    assert(!File.exist?("#{path}/2.wb"))
    10.times{|i|
      fw.write('roma',i,"key-#{i}","val-#{i}")
    }
    # ファイルは3つ
    assert(File.exist?("#{path}/0.wb"))
    assert(File.exist?("#{path}/1.wb"))
    assert(File.exist?("#{path}/2.wb"))
    assert(!File.exist?("#{path}/3.wb"))
  end


  def test_get_current_file_path
    system('rm -rf wb_test')
    fw = Roma::WriteBehind::FileWriter.new("wb_test", 900, @log)

    assert_nil( fw.get_current_file_path('roma') )

    fw.write('roma',0,"key","val")
    
    path = File.expand_path("./wb_test/roma0_11211/roma/#{Time.now.strftime('%Y%m%d')}/")
    assert_equal(  File.join(path,"0.wb"), fw.get_current_file_path('roma'))

    fw.rotate('roma')
    assert_nil( fw.get_current_file_path('roma'))

    fw.write('roma',0,"key","val")
    assert_equal( File.join(path,"1.wb"), fw.get_current_file_path('roma') )
  end

  def test_get_path
    system('rm -rf wb_test')
    fw = Roma::WriteBehind::FileWriter.new("wb_test", 900, @log)
    path = File.expand_path("./wb_test/roma0_11211/roma")
    assert_equal( path, fw.wb_get_path('roma'))
  end

  private

  def read_wb(fname)
    ret = []
    open(fname,'rb'){|f|
      until(f.eof?)
        b1 = f.read(10)
        last, cmd, klen = b1.unpack('NnN')
        key = f.read(klen)
        b2 = f.read(4)
        vlen = b2.unpack('N')[0]
        val = f.read(vlen)
        ret << [last,cmd,key,val]
      end
    }
    ret
  end

end
