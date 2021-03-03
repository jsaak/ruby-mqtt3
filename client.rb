require './ruby-mqtt3'
require 'pp'

backend = 'async'
# backend = 'libev'

if backend == 'async'
  require 'async'
else
  require 'libev_scheduler'
end
require 'fiber'
require 'socket'

if backend == 'async'
  reactor = Async::Reactor.new
  scheduler = Async::Scheduler.new(reactor)
else
  scheduler = Libev::Scheduler.new
end

Fiber.set_scheduler scheduler

m = Mqtt3.new('localhost',1883,true,30)

m.debug = true

m.on_disconnect do |reason|
  m.debug 'TCP disconnected'
end

m.on_connect do |session_present|
  m.debug 'on_connect'
  m.subscribe [['test',0]]
  m.publish 'test', 'demo'
  #m.invalid
  #puts session_present
  #m.publish('test','message')
end

m.on_tcp_connect_error do |e,counter|
  m.debug e.inspect + ", waiting #{counter} sec"
  true
end

m.run

#Fiber.schedule do
  #sleep 10
  #m.pingreq
#end

if backend == 'async'
  reactor.run
else
  scheduler.run
end

