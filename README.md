```
Ruby implementation of MQTT 3.1.1 protocol
Using Fibers and Fiber.scheduler, so needs ruby 3.

QoS 0,1,2 supported

reconnect supported

TLS supported

persistence is supported
there are two modes: :persistence_everytime and :persistence_manual
when manual, persistence data is in memory, and you have to initiate write to disk manually, for example in a signal handler (or periodically)
when everytime, the file will be written every time the state changes, this can be costly
you have to decide what type of guarantees you need
I think :persistence_manual is a good default

websocket is not supported, but should not be too hard using:
https://github.com/imanel/websocket-ruby
rewriting this to use ruby 3 Fibers:
https://github.com/imanel/websocket-eventmachine-client

MQTT 5.0 not supported, yet

there were literally no tests, so use it at your own risk :)


requirements: ruby 3 and a Fiber.scheduler implementation
```
