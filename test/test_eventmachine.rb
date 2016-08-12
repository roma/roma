require 'test_helper'

require 'eventmachine'

class EventMachineTest < Test::Unit::TestCase
  self.test_order = :defined
  LOOP_NUM = 5

  def test_context_switch
    EM.epoll
    t = Thread.new do
      LOOP_NUM.times do |i|
        100.times{ Thread.pass }
        sleep 1
        #puts i
      end
      EM.stop_event_loop
    end
    s = Time.now
    EventMachine::run
    e = Time.now
    assert((e - s) < (LOOP_NUM + 1))
  end
end
