#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

require 'roma/storage/tc_storage'
require 'roma/storage/dbm_storage'
require 'roma/storage/rh_storage'
require 'roma/storage/sqlite3_storage'

class TCStorageTest < Test::Unit::TestCase
  
  def initialize(arg)
    super(arg)
    @ndat=1000
  end

  def setup
    rmtestdir('storage_test')
    @st=Roma::Storage::TCStorage.new
    @st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @st.storage_path = 'storage_test'
    @st.opendb
  rescue =>e
    p e
  end

  def rmtestdir(dirname)
    if File::directory?(dirname)
      File.delete(*Dir["#{dirname}/*"])
      Dir.rmdir(dirname)
    end
  end

  def teardown
    @st.closedb
    rmtestdir('storage_test')
  end

  # 普通のset get
  def test_set_get
    assert_equal( 'abc_data',@st.set(0,'abc',0,0xffffffff,'abc_data')[4])
    assert_equal( 'abc_data', @st.get(0,'abc',0)  )
  end

  # 普通のset delete
  def test_set_delete
    @st.set(0,'abc',0,0xffffffff,'abc_data')
    assert_equal( 'abc_data', @st.delete(0,'abc',0)[4]) # 存在するキーの削除は value が返る
    assert_nil( @st.get(0,'abc',0) )
    assert_equal(:deletemark, @st.delete(0,'abc',0)) # 削除済みはマークがあることを返す
  end

  # 有効期限
  def test_set_exptime
    @st.set(0,'abc',0,Time.now.to_i,'abc_data')
    assert_equal('abc_data', @st.get(0,'abc',0) ) # 期限内
    @st.set(0,'abc',0,Time.now.to_i-1,'abc_data') # 有効期限を1秒前に
    assert_nil( @st.get(0,'abc',0)) # 期限切れ
  end

  # 期限切れデータの削除
  def test_exp_delete
    assert_nil( @st.delete(0,'abc',0)[4])
    assert_equal('abc_data' , @st.set(0,'abc',0,Time.now.to_i-1,'abc_data')[4]) # 有効期限を1秒前に
    assert_nil( @st.delete(0,'abc',0)[4]) # 期限切れ
  end

  def test_rset
    # クロックがカウントアップされる
    assert_equal(0, @st.set(0,'abc',0,Time.now.to_i,'abc_data')[2] )
    assert_equal(1, @st.set(0,'abc',0,Time.now.to_i,'abc_data')[2] )
    assert_equal(2, @st.set(0,'abc',0,Time.now.to_i,'abc_data')[2] )
    # 指定したクロックが挿入される
    assert_equal(4, @st.rset(0,'abc',0,4,Time.now.to_i,'new_data')[2] )
    # 古いクロックは拒否される
    assert_nil( @st.rset(0,'abc',0,4,Time.now.to_i,'new_data') )
    assert_nil( @st.rset(0,'abc',0,3,Time.now.to_i,'new_data') )
  end

  def test_rdelete
    # 指定したクロックで削除マークされる
    assert_equal(2, @st.rdelete(0,'abc',0,2)[2] )
    # 古いクロックの挿入は許されない
    assert_nil( @st.rset(0,'abc',0,1,Time.now.to_i,'new_data'))
    assert_nil( @st.rset(0,'abc',0,2,Time.now.to_i,'new_data'))
    # 古いクロックの削除も拒否される
    assert_nil( @st.rdelete(0,'abc',0,1) )
    assert_nil( @st.rdelete(0,'abc',0,2) )
    # 新しいクロックの削除はマークされる
    assert_equal( 3, @st.rdelete(0,'abc',0,3)[2] )
  end

  def test_out
    assert( !@st.out(0,'abc',0) )
    @st.set(0,'abc',0,Time.now.to_i,'abc_data')
    assert( @st.out(0,'abc',0) )
  end

  # 論理クロックの境界をテスト
  def test_clock_count
    assert_equal( 0xfffffffe, @st.rset(0,'set',0,0xfffffffe,Time.now.to_i,'new_data')[2])
    assert_equal(0xffffffff, @st.set(0,'set',0,Time.now.to_i,'abc_data')[2])
    assert_equal(0, @st.set(0,'set',0,Time.now.to_i,'abc_data')[2] )
    
    assert_equal(0xffffffff,@st.rdelete(0,'add',0,0xffffffff)[2])
    assert_equal(0, @st.add(0,'add',0,Time.now.to_i,'abc_data')[2] )
    
    assert_equal(0xffffffff,  @st.rset(0,'replace',0,0xffffffff,Time.now.to_i,'abc_data')[2])
    assert_equal(0, @st.replace(0,'replace',0,Time.now.to_i,'abc_data')[2] )

    assert_equal(0xffffffff,  @st.rset(0,'append',0,0xffffffff,Time.now.to_i,'abc_data')[2])
    assert_equal(0, @st.append(0,'append',0,Time.now.to_i,'abc_data')[2] )

    assert_equal(0xffffffff, @st.rset(0,'prepend',0,0xffffffff,Time.now.to_i,'abc_data')[2])
    assert_equal(0, @st.prepend(0,'prepend',0,Time.now.to_i,'abc_data')[2] )
    
    assert_equal(0xffffffff, @st.rset(0,'incr',0,0xffffffff,Time.now.to_i,'10')[2])
    assert_equal(0, @st.incr(0,'incr',0,10)[2] )

    assert_equal(0xffffffff, @st.rset(0,'decr',0,0xffffffff,Time.now.to_i,'10')[2])
    assert_equal(0, @st.decr(0,'decr',0,10)[2] )
  end

  def test_add
    assert_equal('abc_data',@st.add(0,'abc',0,Time.now.to_i+1,'abc_data')[4])
    assert_nil( @st.add(0,'abc',0,Time.now.to_i+1,'abc_data') ) # 上書きは失敗する
    assert_equal( 'abc_data', @st.delete(0,'abc',0)[4])
    assert_equal('abc_data', @st.add(0,'abc',0,Time.now.to_i,'abc_data')[4]) # delete 後の add の成功を確認
  end

  def test_replace
    assert_nil( @st.replace(0,'abc',0,Time.now.to_i,'abc_data') )
    assert_equal('abc_data', @st.add(0,'abc',0,Time.now.to_i,'abc_data')[4])
    assert_equal('new_data', @st.replace(0,'abc',0,Time.now.to_i,'new_data')[4] )

  end

  def test_append
    assert_nil( @st.append(0,'abc',0,Time.now.to_i,'abc_data') )
    assert_equal('abc_data',  @st.set(0,'abc',0,Time.now.to_i,'abc_data')[4])
    assert_equal( 'abc_data123',@st.append(0,'abc',0,Time.now.to_i,'123')[4] )
    assert_equal('abc_data123', @st.get(0,'abc',0) )
  end

  def test_prepend
    assert_nil( @st.prepend(0,'abc',0,Time.now.to_i,'abc_data') )
    assert_equal('abc_data',  @st.set(0,'abc',0,Time.now.to_i,'abc_data')[4])
    assert_equal('123abc_data',  @st.prepend(0,'abc',0,Time.now.to_i,'123')[4])
    assert_equal('123abc_data',  @st.get(0,'abc',0))    
  end

  def test_incr
    assert_nil( @st.incr(0,'abc',0,1) )
    assert_equal('100', @st.set(0,'abc',0,Time.now.to_i,'100')[4] )
    assert_equal('101',  @st.incr(0,'abc',0,1)[4])
    assert_equal('106',  @st.incr(0,'abc',0,5)[4])
    assert_equal('100',  @st.incr(0,'abc',0,-6)[4]) # 106 + (-6) = 100
    assert_equal('0', @st.incr(0,'abc',0,-200)[4] ) # 100 + (-200) = 0
    assert_equal('0', @st.incr(0,'abc',0,-200)[4] ) # 0 + (-200) = 0
    # 最大値をセット
    assert_equal('18446744073709551615',  @st.set(0,'abc',0,Time.now.to_i,
      '18446744073709551615')[4])
    assert_equal('1', @st.incr(0,'abc',0,2)[4] ) # max + 2 = 1
  end

  def test_decr
    assert_nil( @st.decr(0,'abc',0,1) )
    assert_equal('100', @st.set(0,'abc',0,Time.now.to_i,'100')[4] )
    assert_equal('99',  @st.decr(0,'abc',0,1)[4])
    assert_equal('94',  @st.decr(0,'abc',0,5)[4])
    assert_equal('100', @st.decr(0,'abc',0,-6)[4] ) # 94 - (-6) = 100
    assert_equal('0', @st.decr(0,'abc',0,200)[4] ) # 100 - 200 = 0
    assert_equal('0', @st.decr(0,'abc',0,200)[4] ) # 0 - 200 = 0
    # 最大値をセット
    assert_equal('18446744073709551615',  @st.set(0,'abc',0,Time.now.to_i,
      '18446744073709551615')[4])
    assert_equal('2',  @st.decr(0,'abc',0,-3)[4]) # max - (-2) = 2
  end

  def test_dump
    assert_nil( @st.dump(0) ) # 最初は０件
    @st.set(0,'abc',0,0xffffffff,'abc_data')
    assert_equal(1, Marshal.load(@st.dump(0)).length )
    @st.set(0,'def',0,0xffffffff,'def_data')
    assert_equal(2, Marshal.load(@st.dump(0)).length )
    assert_nil( @st.dump(1) ) # 異なるvnodeは０件

    # 10万件いれてみる
    n=@ndat
    n.times{|i|
      @st.set(2,i.to_s,0,0xffffffff,'abc_data')
    }
    assert_equal(n, Marshal.load(@st.dump(2)).length )
  end

  # 10万件程度
  def test_volume
    n=@ndat
    n.times{|i|
      @st.set(0,i.to_s,0,0xffffffff,'abc_data')
    }
    n.times{|i|
      assert_equal('abc_data',  @st.get(0,i.to_s,0))
    }
    n.times{|i|
      assert_equal('abc_data',  @st.delete(0,i.to_s,0)[4])
    }
    # 削除記録も含めた本当のレコード数
    assert_equal(n, @st.true_length )
  end

  def test_clean_up
    @st.each_clean_up_sleep = 0
    n=@ndat
    n.times{|i|
      @st.set(0,i.to_s,0,0xffffffff,'abc_data')
    }
    # 指定時刻より以前 and 有効期限切れを削除
    # 全てのデータは現在よりも以前だが、有効期限内なので０件
    assert_equal(0, @st.clean_up(Time.now.to_i+100) )
    # 10件を削除（削除は有効期限を０する）
    10.times{|i|
      assert_equal('abc_data', @st.delete(0,i.to_s,0)[4])
    }
    assert_nil( @st.get(0,'0',0) )
    assert_equal('abc_data', @st.get(0,'19',0) )
    # 削除時刻よりも以前を指定すると、０件
    assert_equal(0, @st.clean_up(Time.now.to_i-100) )
    # 削除時刻よりも進ませると、10件
    assert_equal(10, @st.clean_up(Time.now.to_i+100) )
    # 残は n-10
    assert_equal(n-10, Marshal.load(@st.dump(0)).length )
  end

  def test_each_clean_up
    n=10

    vnhash={}
    n.times{|i|
      n.times{|j|
        @st.set(i,"key#{i}-#{j}",0,0xffffffff,"val#{i}-#{j}")
      }
      vnhash[i]=:primary
    }
    # ---------+------+---------------------
    #        last < now+100
    # 全てのデータは現在よりも以前だが、有効期限内なので０件
    @st.each_clean_up_sleep = 0
    @st.each_clean_up(Time.now.to_i+100,vnhash){|k,vn|
      puts "k=#{k} vn=#{vn}"
      assert(false)
    }

    # vn=0 を10件削除
    n.times{|i| @st.delete(0,"key0-#{i}",0) }
    # 削除時刻よりも以前を指定すると、０件
    @st.each_clean_up(Time.now.to_i-100,vnhash){|k,vn|
      assert(false)
    }

    # 削除時刻よりも進ませると、10件
    cnt=0
    @st.each_clean_up(Time.now.to_i+100,vnhash){|k,vn|
      assert_equal(0, vn)
      assert_match(/key0-/, k )
      cnt += 1
    }
    assert_equal(10,cnt )

    # vn=1 を10件削除
    n.times{|i| @st.delete(1,"key1-#{i}",0) }
    # vn=1 をセカンダリとする
    vnhash[1]=:secondary
    # セカンダリは削除されないので、０件
    @st.each_clean_up(Time.now.to_i-100,vnhash){|k,vn|
      assert(false)
    }
    # vn=1 をプライマリに戻す
    vnhash[1]=:primary
    # 10件になる
    cnt=0
    @st.each_clean_up(Time.now.to_i+100,vnhash){|k,vn|
      assert_equal(1, vn)
      assert_match(/key1-/, k )
      cnt += 1
    }
    assert_equal(10,cnt)
    
    # 消すものが存在しないので、０件
    @st.each_clean_up(Time.now.to_i-100,vnhash){|k,vn|
      assert(false)
    }

    # vn=2 を担当から外す
    vnhash.delete(2)
    # vn=2 のデータは存在する
    n.times{|i|
      assert_match(/val2-/,@st.get(2,"key2-#{i}",0) )
    }
    # vn=2 の10件が削除される
    cnt=0
    @st.each_clean_up(Time.now.to_i+100,vnhash){|k,vn|
      assert_equal(2, vn)
      assert_match(/key2-/, k )
      cnt += 1
    }
    assert_equal(10,cnt)
    # vn=2 のデータは消えているはず
    n.times{|i|
      assert_nil( @st.get(2,"key2-#{i}",0) )
    }

    # vn=3 有効期限を 100秒前にする
    n.times{|i|
      @st.set(3,"key3-#{i}",0,Time.now.to_i-100,"val3-#{i}")
    }
    # 期限切れは last に関係なく削除されるので、10件
    cnt=0
    @st.each_clean_up(Time.now.to_i-100,vnhash){|k,vn|
      assert_equal(3, vn)
      assert_match(/key3-/, k )
      cnt += 1
    }
    assert_equal(10,cnt)
  end

  # 途中で止めるテスト
  def test_each_clean_up2
    n=10

    # テストデータを100件登録する
    vnhash={}
    n.times{|i|
      n.times{|j|
        @st.set(i,"key#{i}-#{j}",0,0xffffffff,"val#{i}-#{j}")
        @st.delete(i,"key#{i}-#{j}",0)
      }
      vnhash[i]=:primary
    }

    # 10msec の wait で each する
    cnt = 0
    th = Thread.new{
      @st.each_clean_up_sleep = 0.01
      @st.each_clean_up(Time.now.to_i+100,vnhash){|k,vn|
        cnt += 1
      }
    }
    # 500msec 後に停止を指示
    sleep 0.5
    @st.stop_clean_up
    th.join
    # 途中で停止したので cnt は 100未満
    assert_operator(100, :>, cnt)
    # 残りを削除
    @st.each_clean_up_sleep = 0
    @st.each_clean_up(Time.now.to_i+100,vnhash){|k,vn|
      cnt += 1
    }
    # 全件消えるので cnt は 100
    assert_equal(100, cnt)
  end

  def test_dump_and_load
    n=10
    n.times{|i|
      @st.set(0,i.to_s,0,0xffffffff,'abc_data')

    }
    assert_equal(0, @st.load(@st.dump(0)) ) # 同じ論理クロックはコピー件数 0 件

    # 進んだ論理クロックのデータを n 件作成
    h={}
    n.times{|i|
      h[i.to_s]=[0,Time.now.to_i,1,0xffffffff].pack('NNNN')+'new data'  
    }
    dmp=Marshal.dump(h)

    assert_equal(n, @st.load(dmp) ) # 進んだ論理クロックの n 件のみコピーされる
    assert_equal('new data',  @st.get(0,'0',0))
  end

  def test_dump_and_load2
    n=10
    n.times{|i|
      assert_nil( @st.delete(0,i.to_s,0)[4] ) # データが存在しなくても削除記録を残す
    }
    dmp=@st.dump(0)
    assert_equal(n, Marshal.load(dmp).length ) # 削除記録もダンプされる
    assert_equal(0, @st.load(@st.dump(0)) ) # 同じデータの場合はコピー件数 0 件

    # 遅れた論理クロックのデータを作成
    h={}
    n.times{|i|
      h[i.to_s]=[0,Time.now.to_i,0xffffffff,0xffffffff].pack('NNNN')+'old data'
    }
    dmp=Marshal.dump(h)
    assert_equal(0, @st.load(dmp) ) # 進んだ論理クロックの削除記録があるのでデータは上書きされない
    assert_nil( @st.get(0,'0',0) )
  end

  # closedb 後のアクセスは NoMethodError が発生することを確認する
  def test_close
    @st.closedb

    assert_raise NoMethodError do
      @st.get(0,'abc',0)
    end

    assert_raise NoMethodError do
      @st.set(0,'abc',0,0xffffffff,'abc_data')
    end
    
    assert_raise NoMethodError do
      @st.dump(0)
    end

    h={}
    100.times{|i|
      h[i.to_s]=[0,Time.now.to_i,0xffffffff,0xffffffff].pack('NNNN')+'old data'
    }
    dmp=Marshal.dump(h)

    assert_raise NoMethodError do
      @st.load(dmp)
    end

    # この場合は 0 件
    assert_equal(0,@st.clean_up(Time.now.to_i+100) )

    # clean_up 中に closedb をするテスト
    #
    # わざわざユニットテストを行う理由 => レアケースなだけにバグも発見しにくい。
    #
    # バッチによる clean_up 処理中に deletehash を行ったときこの状態になる。
    # NoMethodError を判定した retry を保証するためテストを行う。
    # 
    @st.opendb
    h={}
    10.times{|i|
      h[i.to_s]=[0,Time.now.to_i,0,Time.now.to_i].pack('NNNN')+'old data'
    }
    dmp=Marshal.dump(h)
    @st.load(dmp)

    # clean_up 中に closedb されると NoMethodError が発生する
    assert_raise NoMethodError do
      @st.clean_up(Time.now.to_i-10,true)
    end

    # 次のテストのために再度 open
    @st.opendb
  end

  def test_dump_file
    n=100
    n.times{|i|
      @st.set(0,"key#{i}",0,0x7fffffff,"val#{i}")
    }
    @st.dump_file('storage_test_dump')

    count = 0
    open("storage_test_dump/#{@st.hdiv[0]}.dump",'rb'){|f|
      until(f.eof?)
        b1 = f.read(5 * 4)
        vn, last, clk, expt, klen = b1.unpack('NNNNN')
        key = f.read(klen)
        b2 = f.read(4)
        vlen = b2.unpack('N')[0]
        val = f.read(vlen)
        
        count += 1
        assert_equal('key',key[0..2])
        assert_equal('val',val[0..2])
        assert_equal(val[3..-1],key[3..-1]  )
      end
    }
    assert_equal(n,count)
    rmtestdir('storage_test_dump')

    @st.dump_file('storage_test_dump',{0=>0})
    count = 0
    open("storage_test_dump/#{@st.hdiv[0]}.dump",'rb'){|f|
      until(f.eof?)
        b1 = f.read(5 * 4)
        vn, last, clk, expt, klen = b1.unpack('NNNNN')
        key = f.read(klen)
        b2 = f.read(4)
        vlen = b2.unpack('N')[0]
        val = f.read(vlen)
        
        count += 1
        assert_equal('key',key[0..2] )
        assert_equal('val',val[0..2] )
        assert_equal(val[3..-1],key[3..-1] )
      end
    }
    assert_equal(0,count )
    rmtestdir('storage_test_dump')    
  end

  def test_each_vn_dump
    n=100
    n.times{|i|
      @st.set(0,"key#{i}",0,0x7fffffff,"val#{i}")
    }
    (90..99).each{|i|
      @st.delete(0, "key#{i}", 0)
    }
    count = 0
    @st.each_vn_dump(0){|data|
      vn, last, clk, expt, klen = data.slice!(0..19).unpack('NNNNN')
      k = data.slice!(0..(klen-1))
      vlen, = data.slice!(0..3).unpack('N')
      v = data
      count += 1
#      puts "#{vn} #{last} #{clk} #{expt} #{klen} #{k} #{vlen} #{v}"
      assert_equal('key',k[0..2])
      assert_equal('val',v[0..2]) if k[3..-1].to_i < 90

      assert_nil( @st.load_stream_dump(vn, last, clk, expt, k, v) )
      @st.load_stream_dump(2, last, clk, expt, k, v)
    }
    assert_equal(100,count)
    
    count = 0
    @st.each_vn_dump(1){|data| count += 1 }
    assert_equal(0,count )

    count = 0
    @st.each_vn_dump(2){|data| count += 1 }
    assert_equal(100,count )
  end

end

class DbmStorageTest < TCStorageTest
  def setup
    rmtestdir('storage_test')
    @st=Roma::Storage::DbmStorage.new
    @st.vn_list = [0]
    @st.storage_path = 'storage_test'
    @st.opendb
  end

  #undef test_each_clean_up
  #undef test_each_clean_up2
end

class RubyHashStorageTest < TCStorageTest
  def setup
    @st=Roma::Storage::RubyHashStorage.new
    @st.vn_list = [0]
    @st.opendb
  end

  def teardown
  end

  def test_cmp_clk
    (0x001E00000..0x002000000).each{|clk|
      assert_equal(0, @st.send(:cmp_clk,clk, clk) )
    }

    (0x001E00000..0x002000000).each{|clk|
      assert_operator(0,:>, @st.send(:cmp_clk,clk-1, clk) )
      assert_operator(0,:<, @st.send(:cmp_clk,clk, clk-1) )
    }

    (0x001E00000..0x002000000).each{|clk|
      assert_operator(0,:<, @st.send(:cmp_clk,clk+1, clk) )
      assert_operator(0,:>, @st.send(:cmp_clk,clk, clk+1) )
    }
    # t1=0 t2=0 clk2=0b0000...
    clk1=0x00000010
    clk2=0x00000000
    assert_operator(0,:<, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0,:>, @st.send(:cmp_clk,clk2, clk1) )
    # t1=0 t2=1 clk2=0b0010...
    clk2=0x20000000
    assert_operator(0,:>, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0,:<, @st.send(:cmp_clk,clk2, clk1) )
    # t1=0 t2=2 clk2=0b0100...
    clk2=0x40000000
    assert_operator(0, :>, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0, :<, @st.send(:cmp_clk,clk2, clk1) )
    # t1=0 t2=3 clk2=0b0110...
    clk2=0x60000000
    assert_operator(0,:>, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0,:<, @st.send(:cmp_clk,clk2, clk1) )
    # t1=0 t2=4 clk2=0b1000...
    clk2=0x80000000
    assert_operator(0,:>, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0,:<, @st.send(:cmp_clk,clk2, clk1) )

    # t1=0 t2=5 clk2=0b1010...
    clk2=0xa0000000
    assert_operator(0,:<, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0,:>, @st.send(:cmp_clk,clk2, clk1) )
    # t1=0 t2=6 clk2=0b1100...
    clk2=0xc0000000
    assert_operator(0,:<, @st.send(:cmp_clk,clk1, clk2))
    assert_operator(0,:>, @st.send(:cmp_clk,clk2, clk1))
    # t1=0 t2=7 clk2=0b1110...
    clk2=0xe0000000
    assert_operator(0,:<, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0,:>, @st.send(:cmp_clk,clk2, clk1) )
  end

end

class SQLite3StorageTest < TCStorageTest
  def setup
    rmtestdir('storage_test')
    @st=Roma::Storage::SQLite3Storage.new
    @st.vn_list = [0]
    @st.storage_path = 'storage_test'
    @st.opendb
  end

  #undef test_out
end

class TCMemStorageTest < TCStorageTest
  def setup
    rmtestdir('storage_test')
    @st=Roma::Storage::TCMemStorage.new
    @st.vn_list = [0]
    @st.storage_path = 'storage_test'
    @st.opendb
  end
end
