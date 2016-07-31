#!/usr/bin/env ruby

module StorageTests
  def ndat
    1000
  end

  def rmtestdir(dirname)
    if File::directory?(dirname)
      File.delete(*Dir["#{dirname}/*"])
      Dir.rmdir(dirname)
    end
  end

  def test_set_get
    assert_equal( 'abc_data',@st.set(0,'abc',0,0xffffffff,'abc_data')[4])
    assert_equal( 'abc_data', @st.get(0,'abc',0) )
  end

  def test_set_delete
    @st.set(0,'abc',0,0xffffffff,'abc_data')
    # delete method returns a value
    assert_equal( 'abc_data', @st.delete(0,'abc',0)[4])
    assert_nil( @st.get(0,'abc',0) )
    # delete method returns :deletemark with deleted key
    assert_equal(:deletemark, @st.delete(0,'abc',0))
  end

  def test_set_exptime
    @st.set(0,'abc',0,Time.now.to_i,'abc_data')
    # returns a value within a fixed time limit
    assert_equal('abc_data', @st.get(0,'abc',0) )
    # expire time is a second ago
    @st.set(0,'abc',0,Time.now.to_i-1,'abc_data')
    # returns a nil when expired
    assert_nil( @st.get(0,'abc',0))
  end

  def test_set_get_raw
    n = 100
    n.times{|i|
      assert_equal('abc_data',@st.set(0,'abc',0,0xffffffff,'abc_data')[4])
      vn, t, clk, expt, val = @st.get_raw(0,'abc',0)
      assert_equal(vn,0)
      assert(Time.now.to_i - t <= 1)
      assert_equal(clk,i)
      assert_equal(expt,0xffffffff)
      assert_equal(val,'abc_data')
    }
  end

  def test_exp_delete
    assert_nil( @st.delete(0,'abc',0)[4])
    # expire time is a second ago
    assert_equal('abc_data' , @st.set(0,'abc',0,Time.now.to_i-1,'abc_data')[4])
    # delete method returns a nil in expired
    assert_nil( @st.delete(0,'abc',0)[4])
  end

  def test_rset
    # increase a logical clock
    assert_equal(0, @st.set(0,'abc',0,Time.now.to_i,'abc_data')[2] )
    assert_equal(1, @st.set(0,'abc',0,Time.now.to_i,'abc_data')[2] )
    assert_equal(2, @st.set(0,'abc',0,Time.now.to_i,'abc_data')[2] )
    # rset method returns a inputed clock value
    assert_equal(4, @st.rset(0,'abc',0,4,Time.now.to_i,'new_data')[2] )
    # but if input clock value is old then not store the data
    assert_nil( @st.rset(0,'abc',0,4,Time.now.to_i,'new_data') )
    assert_nil( @st.rset(0,'abc',0,3,Time.now.to_i,'new_data') )
  end

  def test_rdelete
    # save a clock value in the deletemark
    assert_equal(2, @st.rdelete(0,'abc',0,2)[2] )
    # reject a old clock value in rset method
    assert_nil( @st.rset(0,'abc',0,1,Time.now.to_i,'new_data'))
    assert_nil( @st.rset(0,'abc',0,2,Time.now.to_i,'new_data'))
    # also reject a old clock value in rdelete method
    assert_nil( @st.rdelete(0,'abc',0,1) )
    assert_nil( @st.rdelete(0,'abc',0,2) )
    # but input the new clock to allow
    assert_equal( 3, @st.rdelete(0,'abc',0,3)[2] )
  end

  def test_out
    assert( !@st.out(0,'abc',0) )
    @st.set(0,'abc',0,Time.now.to_i,'abc_data')
    assert( @st.out(0,'abc',0) )
  end

  # on boundary of a clock
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
    # deny a over write
    assert_nil( @st.add(0,'abc',0,Time.now.to_i+1,'abc_data') )
    assert_equal( 'abc_data', @st.delete(0,'abc',0)[4])
    assert_equal('abc_data', @st.add(0,'abc',0,Time.now.to_i,'abc_data')[4])
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
    # set to max value
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
    # set to max value
    assert_equal('18446744073709551615',  @st.set(0,'abc',0,Time.now.to_i,
      '18446744073709551615')[4])
    assert_equal('2',  @st.decr(0,'abc',0,-3)[4]) # max - (-2) = 2
  end

  def test_dump
    assert_nil( @st.dump(0) )
    @st.set(0,'abc',0,0xffffffff,'abc_data')
    assert_equal(1, Marshal.load(@st.dump(0)).length )
    @st.set(0,'def',0,0xffffffff,'def_data')
    assert_equal(2, Marshal.load(@st.dump(0)).length )
    assert_nil( @st.dump(1) ) # another vnode is empty

    n=ndat
    n.times{|i|
      @st.set(2,i.to_s,0,0xffffffff,'abc_data')
    }
    assert_equal(n, Marshal.load(@st.dump(2)).length )
  end

  def test_volume
    n=ndat
    n.times{|i|
      @st.set(0,i.to_s,0,0xffffffff,'abc_data')
    }
    n.times{|i|
      assert_equal('abc_data',  @st.get(0,i.to_s,0))
    }
    n.times{|i|
      assert_equal('abc_data',  @st.delete(0,i.to_s,0)[4])
    }
    # true_length value is included in number of deletemark
    assert_equal(n, @st.true_length )
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
    # all data within a fixed time limit
    @st.each_clean_up_sleep = 0
    @st.each_clean_up(Time.now.to_i+100,vnhash){|k,vn|
      puts "k=#{k} vn=#{vn}"
      assert(false)
    }

    # delete data in vn=0
    n.times{|i| @st.delete(0,"key0-#{i}",0) }
    # time is 100 second ago
    @st.each_clean_up(Time.now.to_i-100,vnhash){|k,vn|
      assert(false)
    }

    # time is 100 second later
    cnt=0
    @st.each_clean_up(Time.now.to_i+100,vnhash){|k,vn|
      assert_equal(0, vn)
      assert_match(/key0-/, k )
      cnt += 1
    }
    assert_equal(10,cnt )

    # delete data in vn=1
    n.times{|i| @st.delete(1,"key1-#{i}",0) }
    # set to :secondary in vn=1
    vnhash[1]=:secondary
    # :secondary was not deleted
    @st.each_clean_up(Time.now.to_i-100,vnhash){|k,vn|
      assert(false)
    }
    # set to :primary in vn=1
    vnhash[1]=:primary
    # in :primary data was deleted
    cnt=0
    @st.each_clean_up(Time.now.to_i+100,vnhash){|k,vn|
      assert_equal(1, vn)
      assert_match(/key1-/, k )
      cnt += 1
    }
    assert_equal(10,cnt)
    
    # deletemark was empty
    @st.each_clean_up(Time.now.to_i-100,vnhash){|k,vn|
      assert(false)
    }

    # vn=2 is not taken of charge
    vnhash.delete(2)
    # but still have a data in vn=2
    n.times{|i|
      assert_match(/val2-/,@st.get(2,"key2-#{i}",0) )
    }
    # data was deleted in vn=2
    cnt=0
    @st.each_clean_up(Time.now.to_i+100,vnhash){|k,vn|
      assert_equal(2, vn)
      assert_match(/key2-/, k )
      cnt += 1
    }
    assert_equal(10,cnt)
    # confirm it in vn=2
    n.times{|i|
      assert_nil( @st.get(2,"key2-#{i}",0) )
    }

    # time is 100 second ago in vn=3
    n.times{|i|
      @st.set(3,"key3-#{i}",0,Time.now.to_i-100,"val3-#{i}")
    }
    # 10 keys deleted
    cnt=0
    @st.each_clean_up(Time.now.to_i-100,vnhash){|k,vn|
      assert_equal(3, vn)
      assert_match(/key3-/, k )
      cnt += 1
    }
    assert_equal(10,cnt)
  end

  def test_each_clean_up2
    n=10

    # set and delete is repeated 100 times
    vnhash={}
    n.times{|i|
      n.times{|j|
        @st.set(i,"key#{i}-#{j}",0,0xffffffff,"val#{i}-#{j}")
        @st.delete(i,"key#{i}-#{j}",0)
      }
      vnhash[i]=:primary
    }

    # each waite is 10 msec
    cnt = 0
    th = Thread.new{
      @st.each_clean_up_sleep = 0.01
      @st.each_clean_up(Time.now.to_i+100,vnhash){|k,vn|
        cnt += 1
      }
    }
    # in 500msec later will stop 
    sleep 0.5
    @st.stop_clean_up
    th.join
    # should cnt is less than 100
    assert_operator(100, :>, cnt)
    # delete a remain keys
    @st.each_clean_up_sleep = 0
    @st.each_clean_up(Time.now.to_i+100,vnhash){|k,vn|
      cnt += 1
    }
    # after all cnt is 100
    assert_equal(100, cnt)
  end

  def test_dump_and_load
    n=10
    n.times{|i|
      # clock = 0
      @st.set(0,i.to_s,0,0xffffffff,'abc_data')
    }
    # not loaded
    assert_equal(0, @st.load(@st.dump(0)) )

    h={}
    n.times{|i|
      # clock = 1
      h[i.to_s]=[0,Time.now.to_i,1,0xffffffff].pack('NNNN')+'new data'  
    }
    dmp=Marshal.dump(h)

    # loaded
    assert_equal(n, @st.load(dmp) )
    assert_equal('new data',  @st.get(0,'0',0))
  end

  def test_dump_and_load2
    n=10
    # create a deletemark
    n.times{|i|
      assert_nil( @st.delete(0,i.to_s,0)[4] )
    }
    # dump a deletemark
    dmp=@st.dump(0)
    assert_equal(n, Marshal.load(dmp).length )
    # not loaded, it's same data
    assert_equal(0, @st.load(@st.dump(0)) )

    # create a old clock data
    h={}
    n.times{|i|
      h[i.to_s]=[0,Time.now.to_i,0xffffffff,0xffffffff].pack('NNNN')+'old data'
    }
    dmp=Marshal.dump(h)
    # not loaded
    assert_equal(0, @st.load(dmp) )
    assert_nil( @st.get(0,'0',0) )
  end

  # access after close
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

  end

  def test_close_after_each_clean_up
    h={}
    100.times{|i|
      h[i.to_s]=[i%10,Time.now.to_i,0,Time.now.to_i].pack('NNNN')+'old data'
    }
    dmp=Marshal.dump(h)
    @st.load(dmp)

    @st.each_clean_up(Time.now.to_i-100, Hash.new(:primary) ){|k,vn|
      @st.closedb
    }
  end

  def test_each_vn_dump_in_normal
    n=100
    n.times{|i|
      @st.set(0,"key#{i}",0,0x7fffffff,"val#{i}")
    }
    (90..99).each{|i|
      @st.delete(0, "key#{i}", 0)
    }
    count = 0
    res = @st.each_vn_dump(0){|data|
      vn, last, clk, expt, klen = data.slice!(0..19).unpack('NNNNN')
      k = data.slice!(0..(klen-1))
      data.slice!(0..3).umpack('N')
      v = data
      count += 1

      assert_equal('key',k[0..2])
      assert_equal('val',v[0..2]) if k[3..-1].to_i < 90

      assert_nil( @st.load_stream_dump(vn, last, clk, expt, k, v) )
      @st.load_stream_dump(2, last, clk, expt, k, v)
    }
    assert(res)
    assert_equal(100,count)

    count = 0
    @st.each_vn_dump(1){|data| count += 1 }
    assert_equal(0,count )

    count = 0
    @st.each_vn_dump(2){|data| count += 1 }
    assert_equal(100,count )
  end

  def test_each_vn_dump_not_normal
    dn = @st.instance_eval{ @hdiv[0] }

    n=100
    n.times{|i|
      @st.set(0,"key#{i}",0,0x7fffffff,"val#{i}")
    }

    # :normal -> :safecopy_flushing
    assert_equal(:safecopy_flushing, @st.set_db_stat(dn, :safecopy_flushing))
    # :safecopy_flushing -> :safecopy_flushed
    assert_equal(:safecopy_flushed, @st.set_db_stat(dn, :safecopy_flushed))

    (80..99).each{|i|
      @st.set(0,"key#{i}",0,0x7fffffff,"val#{i + 1}")
    }

    # :safecopy_flushed -> :cachecleaning
    assert_equal(:cachecleaning, @st.set_db_stat(dn, :cachecleaning))

    (90..99).each{|i|
      @st.delete(0, "key#{i}", 0)
    }

    count = 0
    res = @st.each_vn_dump(0) { |_| count += 1 }

    assert_equal(false, res)
    assert_equal(0, count)
  end

  def test_db_stat_with_each_vn_dump
    n = 100 # number of data
    m = 10 # number of vnode
    # set data
    n.times do |i|
      m.times do|vn|
        @st.set(vn,"key#{vn}_#{i}",0,0x7fffffff,"val#{i}")
      end
    end

    m.times do |vn|
      dn = @st.instance_eval{ @hdiv[vn] }
      count = 0
      res = @st.each_vn_dump(vn) do
        assert_equal(false, @st.each_vn_dump(vn){})
        assert_equal(:normal, @st.dbs[dn])
        assert_equal(false, @st.set_db_stat(dn, :safecopy_flushing))
        count += 1
      end
      assert_equal(n, count)
      assert(res)
    end
  end

  def test_db_stat
    assert_nil(@st.instance_eval{ @hdbc[0] })
    # :normal -> error case
    assert_equal(false, @st.set_db_stat(0, :safecopy_flushed))
    assert_equal(false, @st.set_db_stat(0, :cachecleaning))
    assert_equal(false, @st.set_db_stat(0, :normal))
    # :normal -> :safecopy_flushing
    assert_equal(:safecopy_flushing, @st.set_db_stat(0, :safecopy_flushing))
    assert_equal(:safecopy_flushing, @st.instance_eval{ @dbs[0] })
    assert(@st.instance_eval{ @hdbc[0] })

    # :safecopy_flushing -> error case
    assert_equal(false, @st.set_db_stat(0, :safecopy_flushing))
    assert_equal(false, @st.set_db_stat(0, :cachecleaning))
    assert_equal(false, @st.set_db_stat(0, :normal))
    # :safecopy_flushing -> :safecopy_flushed
    assert_equal(:safecopy_flushed, @st.set_db_stat(0, :safecopy_flushed))
    assert_equal(:safecopy_flushed, @st.instance_eval{ @dbs[0] })
    assert(@st.instance_eval{ @hdbc[0] })
    
    # :safecopy_flushed -> error case
    assert_equal(false, @st.set_db_stat(0, :safecopy_flushing))
    assert_equal(false, @st.set_db_stat(0, :safecopy_flushed))
    assert_equal(false, @st.set_db_stat(0, :normal))
    # :safecopy_flushed -> :cachecleaning
    assert_equal(:cachecleaning, @st.set_db_stat(0, :cachecleaning))
    assert_equal(:cachecleaning, @st.instance_eval{ @dbs[0] })
    assert(@st.instance_eval{ @hdbc[0] })

    # :cachecleaning -> error case
    assert_equal(false, @st.set_db_stat(0, :safecopy_flushed))
    assert_equal(false, @st.set_db_stat(0, :cachecleaning))
    # :cachecleaning -> :safecopy_flushing
    assert_equal(:safecopy_flushing, @st.set_db_stat(0, :safecopy_flushing))
    assert_equal(:safecopy_flushing, @st.instance_eval{ @dbs[0] })
    assert_equal(:safecopy_flushed, @st.set_db_stat(0, :safecopy_flushed))
    assert_equal(:cachecleaning, @st.set_db_stat(0, :cachecleaning))
    assert(@st.instance_eval{ @hdbc[0] })
    # :cachecleaning -> :normal
    assert_equal(:normal, @st.set_db_stat(0, :normal))
    assert_equal(:normal, @st.instance_eval{ @dbs[0] })
    assert_nil(@st.instance_eval{ @hdbc[0] })
  end

  def test_clock_along_status
    vn = 0
    dn = @st.instance_eval{ @hdiv[vn] }
    assert_equal('abc_data', @st.set(vn,'abc',0,0xffffffff,'abc_data')[4])
    assert( @st.get_context(vn, 'abc', 0)[2] == 0 )
    # :normal -> :safecopy_flushing
    assert_equal(:safecopy_flushing, @st.set_db_stat(dn, :safecopy_flushing))
    assert_equal('abc_data', @st.set(vn,'abc',0,0xffffffff,'abc_data')[4])
    assert( @st.get_context(vn, 'abc', 0)[2] == 1 )
    # :safecopy_flushing -> :safecopy_flushed
    assert_equal(:safecopy_flushed, @st.set_db_stat(dn, :safecopy_flushed))
    assert_equal('abc_data', @st.set(vn,'abc',0,0xffffffff,'abc_data')[4])
    assert( @st.get_context(vn, 'abc', 0)[2] == 2 )
    # :safecopy_flushed -> :cachecleaning
    assert_equal(:cachecleaning, @st.set_db_stat(dn, :cachecleaning))
    assert_equal('abc_data', @st.set(vn,'abc',0,0xffffffff,'abc_data')[4])
    assert( @st.get_context(vn, 'abc', 0)[2] == 3 )
    # :cachecleaning -> :normal
    assert_equal(:normal, @st.set_db_stat(dn, :normal))
    assert_equal('abc_data', @st.set(vn,'abc',0,0xffffffff,'abc_data')[4])
    assert( @st.get_context(vn, 'abc', 0)[2] == 4 )
  end

  def test_set_get_in_safecopy
    vn = 0
    dn = @st.instance_eval{ @hdiv[vn] }
    # :normal -> :safecopy_flushing
    assert_equal(:safecopy_flushing, @st.set_db_stat(dn, :safecopy_flushing))
    assert_equal(:safecopy_flushing, @st.instance_eval{ @dbs[dn] })
    assert_equal('abc_data', @st.set(vn,'abc',0,0xffffffff,'abc_data')[4])
    assert_equal('abc_data', @st.get(vn,'abc',0) )
    # not stored
    assert_nil(@st.instance_eval{ @hdb[dn].get('abc') })
    # stored
    assert(@st.instance_eval{ @hdbc[dn].get('abc') })

    # :safecopy_flushing -> :safecopy_flushed
    assert_equal(:safecopy_flushed, @st.set_db_stat(dn, :safecopy_flushed))
    assert_equal(:safecopy_flushed, @st.instance_eval{ @dbs[dn] })
    assert_equal('def_data', @st.set(vn,'def',0,0xffffffff,'def_data')[4])
    assert_equal('def_data', @st.get(vn,'def',0) )
    # not stored
    assert_nil(@st.instance_eval{ @hdb[dn].get('def') })
    # stored
    assert(@st.instance_eval{ @hdbc[dn].get('def') })
  end

  def test_set_get_in_cachecleaning
    vn = 0
    dn = @st.instance_eval{ @hdiv[vn] }
    assert_equal( 'abc_data',@st.set(vn,'abc',0,0xffffffff,'abc_data')[4])
    assert_equal( 'abc_data', @st.get(vn,'abc',0) ) # database
    # :normal -> :safecopy_flushing
    assert_equal(:safecopy_flushing, @st.set_db_stat(dn, :safecopy_flushing))
    # :safecopy_flushing -> :safecopy_flushed
    assert_equal(:safecopy_flushed, @st.set_db_stat(dn, :safecopy_flushed))
    assert_equal( 'abc_data1',@st.set(vn,'abc',0,0xffffffff,'abc_data1')[4])
    assert_equal( 'abc_data1', @st.get(vn,'abc',0) ) # cache
    # :safecopy_flushed -> :cachecleaning
    assert_equal(:cachecleaning, @st.set_db_stat(dn, :cachecleaning))
    assert_equal( 'abc_data1', @st.get(vn,'abc',0) ) # cache
    assert_equal( 'abc_data2',@st.set(vn,'abc',0,0xffffffff,'abc_data2')[4])
    assert_equal( 'abc_data2', @st.get(vn,'abc',0) ) # database
  end

  def test_out_cache
    vn = 0
    dn = @st.instance_eval{ @hdiv[vn] }
    # :normal -> :safecopy_flushing
    assert_equal(:safecopy_flushing, @st.set_db_stat(dn, :safecopy_flushing))
    assert_equal(:safecopy_flushing, @st.instance_eval{ @dbs[dn] })
    assert_equal('abc_data', @st.set(vn,'abc',0,0xffffffff,'abc_data')[4])
    assert_equal('abc_data', @st.get(vn,'abc',0) )
    # not stored
    assert_nil(@st.instance_eval{ @hdb[dn].get('abc') })
    # stored
    assert(@st.instance_eval{ @hdbc[dn].get('abc') })
    # out
    @st.out_cache(dn, 'abc')
    # not stored
    assert_nil(@st.instance_eval{ @hdb[dn].get('abc') })
    # not stored
    assert_nil(@st.instance_eval{ @hdbc[dn].get('abc') })
  end

  def test_each_cache_dump_pack
    vn = 0
    dn = @st.instance_eval{ @hdiv[vn] }
    # :normal -> :safecopy_flushing
    assert_equal(:safecopy_flushing, @st.set_db_stat(dn, :safecopy_flushing))
    keys = []
    10.times do |i|
      k = "key#{i}"
      v = "val#{i}"
      assert_equal(v, @st.set(vn,k,0,0xffffffff,v)[4])
      keys << k
    end
    @st.each_cache_dump_pack(dn, keys) do |data|
      vn, _last, _clk, _expt, klen = data.slice!(0..19).unpack('NNNNN')
      k = data.slice!(0..(klen-1))
      data.slice!(0..3).unpack('N')
      v = data

      assert_match(/key\d/, k)
      assert_match("val#{k[3..-1]}", v)
    end
  end

  def test_get_keys_in_cache
    vn = 0
    dn = @st.instance_eval{ @hdiv[vn] }
    # :normal -> :safecopy_flushing
    assert_equal(:safecopy_flushing, @st.set_db_stat(dn, :safecopy_flushing))
    keys = []
    100.times do |i|
      k = "key#{i}"
      v = "val#{i}"
      assert_equal(v, @st.set(vn,k,0,0xffffffff,v)[4])
    end

    assert(@st.instance_eval{ @hdbc[dn].rnum } == 100)
    10.times do
      keys = @st.get_keys_in_cache(dn, 10)
      assert(keys.length == 10)
      keys.each do |k|
        @st.out_cache(dn, k)
      end
    end
    assert(@st.instance_eval{ @hdbc[dn].rnum } == 0)
  end
end
