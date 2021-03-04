```
Ruby implementation of MQTT 3.1.1 protocol
Using Fibers, so needs ruby 3.

QoS 0,1,2 supported, persistence data is written to a file, not very robust, but better than nothing
reconnect supported
TLS supported

websocket is not supported, but should not be too hard using:
https://github.com/imanel/websocket-ruby
rewriting this to use ruby 3 Fibers:
https://github.com/imanel/websocket-eventmachine-client

MQTT 5.0 not supported

there were literally no tests, so use it at your own risk :)
```
