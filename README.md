# Ruby implementation of MQTT 3.1.1 protocol
Using Fibers and Fiber.scheduler, so needs ruby 3.

QoS 0,1,2 supported

## Reconnect

By default reconnect is set to true, and it reconnets after 0,1,2,4,8,16,32,64,128,256,300,300... seconds

If it does not suite you, you can pass a custom function.

## SSL

verify peer is always enabled

client cert is supported

ssl param can be true or one of these protocol versions:
```
OpenSSL::SSL::SSLContext::METHODS => [:SSLv23, :SSLv23_client, :SSLv23_server, :SSLv2, :SSLv2_client, :SSLv2_server, :SSLv3, :SSLv3_client, :SSLv3_server, :TLSv1, :TLSv1_client, :TLSv1_server, :TLSv1_1, :TLSv1_1_client, :TLSv1_1_server, :TLSv1_2, :TLSv1_2_client, :TLSv1_2_server]
```

## Persistence

There are two modes: `:persistence_everytime` and `:persistence_manual`

When `:persistence_manual`, persistence data is in memory, and you have to initiate write to disk manually, for example in a signal handler or periodically

When `:persistence_everytime`, the file will be written every time the state changes, this can be costly

You have to decide what guarantees you need, I think `:persistence_manual` is a good default

## Not supported

### websocket is not supported, but should not be too hard using:

https://github.com/socketry/protocol-websocket or

https://github.com/socketry/async-websocket or

https://github.com/imanel/websocket-ruby and
https://github.com/imanel/websocket-eventmachine-client


###MQTT 5.0 not supported, yet

## Notes

there were literally no tests, so use it at your own risk :)

