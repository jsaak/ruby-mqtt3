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

m = Mqtt3.new(keepalive_sec: 30, persistence_filename: 'persist.data', clean_session: false)

m.debug = true

m.on_disconnect do |reason|
  m.debug 'TCP disconnected'
end

m.on_connect do |session_present|
  m.debug 'on_connect'
  m.subscribe [['test',0]]
  #m.invalid
  #puts session_present
  #m.publish('test','message')
end

m.on_tcp_connect_error do |e,counter|
  m.debug e.inspect + ", waiting #{counter} sec"
  true
end

m.run

Fiber.schedule do
  sleep 6
  m.publish 'test', 'demo', 1
end

if backend == 'async'
  reactor.run
else
  scheduler.run
end

