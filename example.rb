#backend = 'async_v1'
backend = 'async_v2'
# backend = 'libev'

require './lib/ruby-mqtt3'

if backend == 'libev'
  require 'libev_scheduler'
else
  require 'async'
end
require 'fiber'
require 'socket'

if backend == 'async_v1'
  reactor = Async::Reactor.new
  scheduler = Async::Scheduler.new(reactor)
elsif backend == 'async_v2'
  scheduler = Async::Scheduler.new
else
  scheduler = Libev::Scheduler.new
end

Fiber.set_scheduler scheduler

m = Mqtt3.new(keepalive_sec: 30,
              persistence_filename: 'persist.data',
              clean_session: true,
              reconnect: false,
              host: 'localhost',
              port: '1883',
             )

m.debug = true

m.on_disconnect do |reason|
  m.debug 'TCP disconnected'
end

m.on_connect do |session_present|
  m.debug 'on_connect'
  m.subscribe [['test',2],['test2',0]]
  #m.invalid
  #puts session_present
  #m.publish('test','message')
end

# custom sleep logic on_tcp_connect_error
m.on_tcp_connect_error do |e,counter|
  m.debug e.inspect + ", waiting #{counter} sec"
  sleep counter
  false
end

# default sleep logic on_tcp_connect_error
m.on_tcp_connect_error do |e,counter|
  m.debug e.inspect + ", waiting #{counter} sec"
  true
end

m.on_message do |topic, message, qos, packet_id|
  m.debug "Incoming topic: #{topic} message: #{message} qos: #{qos} packet_id: #{packet_id}"
end

m.on_publish_finished do |packet_id|
  m.debug "packet published #{packet_id}"
end

Signal.trap("INT") do
  m.save
  exit
end

Signal.trap("TERM") do
  m.save
  exit
end

m.run

Fiber.schedule do
  sleep 1
  packet_id = m.publish 'test', 'demo', 2
  #m.invalid
  sleep 1
  m.stop
end

if backend == 'async_v1'
  reactor.run
elsif backend == 'libev'
  scheduler.run
end

