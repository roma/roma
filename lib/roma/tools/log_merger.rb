#!/usr/bin/env ruby
require 'optparse'

class ReadLog
  def initialize(fn)
    @f = open(fn)
    @buff = ''
    @prev = @f.gets
    @s_date = ''
  end

  def read_line
    begin
      if /^#\sLogfile\screated\son.+/ =~ @prev then
        @prev = @f.gets
      end

      @buff = @prev
      @prev = @f.gets

      while !(match_line(@prev)) && !(@prev.nil?) do
        @buff << @prev
        @prev = @f.gets
      end

      match_line(@buff)
    rescue
      unless @f.closed?
        @f.close
      end
    end
  end

  def get_line
    @buff
  end

  def get_date
    @s_date
  end

  def match_line(l)
    if /^[TDIWEFU],\s\[(\d{4})\-(\d{2})\-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d+)\s#\d+\].+/ =~ l then
      @s_date = $1 << $2 << $3 << $4 << $5 << $6 << $7
      true
    else
      @s_date = nil
      false
    end
  end
end

class WriteLog
  def initialize(fn)
    if fn == $stdout then
      @f = $stdout
    else
      @f = open(fn, "w")
    end
  end

  def write_line(th, l)
    buff = th << ',' << l
    @f.write(buff)
  end

  def close_file
    unless @f.closed?
      @f.close
    end
  end
end

$save_fn = $stdout
$log_l = 'TRACE'
$start_date = '00000000000000000000'
$end_date = '99999999999999999999'
o = OptionParser.new
o.banner = "Usage : #{__FILE__} [log_file1]..[log_fileN] [-o output_file] [-l log_level]"
o.on('-o output_file', '[output file name]', '(default=STDOUT)') {|v| $save_fn = v }
o.on('-l log_level', '[TRACE | DEBUG | INFO | WARN | ERROR | FATAL | UNKNOWN]', '(default=TRACE)') {|v| $log_l = v}
o.on('-s start_date', '[YYYYMMDDHHMMSS]') {|v| $start_date = v.ljust(20, "0").to_s}
o.on('-e end_date', '[YYYYMMDDHHMMSS]') {|v| $end_date = v.ljust(20, "9").to_s}
begin
  o.parse!
rescue
  STDERR.puts o.help
  puts  "ERROR : unrecognized option"
  exit
end

if ARGV.length == 0 then
  STDERR.puts o.help
  exit
end

r = Array.new()
w = WriteLog.new($save_fn)
h = Hash.new()
tmp = Hash.new()
e_lv = nil

case $log_l
  when 'DEBUG' then
    e_lv = 'DIWEFU'
  when 'INFO' then
    e_lv = 'IWEFU'
  when 'WARN' then
    e_lv = 'WEFU'
  when 'ERROR' then
    e_lv = 'EFU'
  when 'FATAL' then
    e_lv = 'FU'
  when 'UNKNOWN' then
    e_lv = 'U'
  else
    e_lv = 'TDIWEFU'
end

ARGV.length.times do |i|
  r[i] = ReadLog.new(ARGV[i])
  r[i].read_line
  if $start_date != '00000000000000000000' then
    while !(r[i].get_date.nil?) && $start_date > r[i].get_date do
      r[i].read_line
    end
  end
end

begin
  h.clear
  tmp.clear
  ARGV.length.times do |i|
    unless r[i].get_line.nil? then
      tmp[i] = r[i].get_date
    end
  end
  if tmp.length > 0 then
    h = tmp.sort_by{|a, b| b}
    if /^[#{e_lv}],.+/ =~ r[h[0][0]].get_line then
      if ($start_date <= h[0][1]) && ($end_date >= h[0][1]) then
        w.write_line(File.basename(ARGV[h[0][0]].dup), r[h[0][0]].get_line)
      end
    end
    r[h[0][0]].read_line
  end
end while tmp.length > 0

w.close_file
exit
