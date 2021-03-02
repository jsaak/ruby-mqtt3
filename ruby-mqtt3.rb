class Mqtt3TcpDisconnected < Exception
end

class Mqtt3
  attr_accessor :debug

  #connection params
  attr_accessor :host
  attr_accessor :ip
  attr_accessor :reconnect
  attr_accessor :keepalive_sec
  attr_accessor :clean_session
  attr_accessor :will_topic
  attr_accessor :will_payload
  attr_accessor :will_qos
  attr_accessor :will_retain

  #internal state
  attr_reader :last_packet_sent_at
  attr_reader :state

  MQTT_PACKET_TYPES = [
    'INVALID', #0
    'CONNECT', #1
    'CONNACK', #2
    'PUBLISH', #3
    'PUBACK', #4
    'PUBREC', #5
    'PUBREL', #6
    'PUBCOMP', #7
    'SUBSCRIBE', #8
    'SUBACK', #9
    'UNSUBSCRIBE', #10
    'UNSUBACK', #11
    'PINGREQ', #12
    'PINGRESP', #13
    'DISCONNECT', #14
    'RESERVED' ]

  def initialize(host,port,reconnect=true,keepalive_sec=30)
    @host = host
    @port = port
    @reconnect = reconnect
    @keepalive_sec = keepalive_sec
    @socket = nil
    @qos1store = []
    @qos2store = []
    @packet_id = 0
    @running = true

    #packets taken from strace -s 100 -x mosquitto_pub and _sub
    @packet_connect = "\x10\x1d\x00\x04\x4d\x51\x54\x54\x04\x02\x00\x3c\x00\x11\x6d\x6f\x73\x71\x70\x75\x62\x7c\x32\x35\x35\x31\x32\x2d\x78\x31\x65".force_encoding('ASCII-8BIT')
    @packet_publish_q0 = "\x30\x08\x00\x03\x31\x32\x33\x33\x34\x35".force_encoding('ASCII-8BIT')
    #packet_publish_q1 = 
    @packet_subscribe_test = "\x82\x09\x00\x01\x00\x04\x74\x65\x73\x74\x00".force_encoding('ASCII-8BIT')
    #TODO qos 1 and 2
  end

  def pingreq
    send_packet("\xc0\x00".force_encoding('ASCII-8BIT'))
  end

  def pingresp
    send_packet("\xd0\x00".force_encoding('ASCII-8BIT'))
  end

  def disconnect
    send_packet("\xe0\x00".force_encoding('ASCII-8BIT'))
  end

  def invalid
    send_packet("\xff\x00".force_encoding('ASCII-8BIT'))
  end

  def publish(topic,message,qos = 0,retain = false)
  end

  def subscribe(topic_list)
    topic_list.each do |x|
      #send_subscribe(topic,qos)
    end
  end

  def send_packet(p)
    debug 'sending packet: ' + p.unpack('H*').first
    begin
      @socket.send(p,0)
      @last_packet_sent_at = Time.now
    rescue => e
      puts e
      puts e.message
    end
  end

  def on_connect(&block)
    @on_connect_block = block
  end

  def on_tcp_connect_error(&block)
    @on_tcp_connect_error_block = block
  end

  def on_mqtt_connect_error(&block)
    @on_mqtt_connect_error_block = block
  end

  def on_disconnect(&block)
    @on_disconnect_block = block
  end

  def on_subscribe(&block)
    @on_subscribe_block = block
  end

  def on_publish_finished(&block)
    @on_publish_finished_block = block
  end

  def on_message(&block)
    @on_message = block
  end

  def handle_packet(type,flags,length,data)
    debug "incoming packet: #{MQTT_PACKET_TYPES[type]} (#{type})  flags: #{flags}  length: #{length}  data: #{data.unpack('H*').first}"
    case type
    when 2 #CONNACK
      return_code = data[1].ord
      if return_code == 0
        @state = :mqtt_connected
        session_present = (data[0].ord == 1)
        @on_connect_block.call(session_present) unless @on_connect_block.nil?
      else
        @on_mqtt_connect_error_block.call(return_code) unless @on_mqtt_connect_error_block.nil?
      end
    when 3 #PUBLISH
      #@on_message_block.call(topic, message) unless @on_message.nil?
    when 4 #PUBACK
      #@on_publish_finished_block.call(topic_name, packet_id, ret) unless @on_publish_finished_block.nil?
    when 6 #PUBREL
      #@on_publish_finished_block.call(topic_name, packet_id, ret) unless @on_publish_finished_block.nil?
    when 9 #SUBACK
      # for each topic
      #@on_subscribe_block.call(topic_name, packet_id, ret)
    when 12 #PINGREQ
      pingresp
    else
    end
  end

  def connect()
    counter = 0
    begin
      @socket = TCPSocket.new(@host, @port, connect_timeout: 1)
      @state = :tcp_connected
      debug 'TCP connected'
      return @socket
    rescue => e
      default = true
      default = @on_tcp_connect_error_block.call(e,counter) unless @on_tcp_connect_error_block.nil?
      if @reconnect
        if default
          if counter > 0
            sleep counter
          end

          if counter == 0
            counter = 1
          else
            counter *= 2
            counter = 300 if counter > 300
          end
        end

        retry
      else
        @running = false
      end
    end
  end

  def recv_bytes(count)
    buffer = ''
    while buffer.length != count
      #TODO rescue
      chunk = @socket.recv(count - buffer.length)
      if chunk == ''
        @state = :disconnected
        @on_disconnect_block.call unless @on_disconnect_block.nil?
        if @reconnect
          @socket = connect()
          send_packet(@packet_connect)
        else
          @running = false
          raise Mqtt3TcpDisconnected
        end
      end

      buffer += chunk
    end
    return buffer
  end

  def run
    Fiber.schedule do
      @state = :disconnected
      @socket = connect()

      Fiber.schedule do
        #puts 'entering recv fiber'
        while @running do
          begin
            x = recv_bytes(1).ord
            type = (x & 0xf0) >> 4
            flags = x & 0x0f

            # Read in the packet length
            multiplier = 1
            length = 0
            pos = 1

            loop do
              digit = recv_bytes(1).ord
              length += ((digit & 0x7F) * multiplier)
              multiplier *= 0x80
              pos += 1
              break if (digit & 0x80).zero? || pos > 4
            end

            data = recv_bytes(length)
            handle_packet(type, flags, length, data)
          rescue Mqtt3TcpDisconnected
          end
        end
        puts 'exiting recv fiber'
      end

      Fiber.schedule do
        #puts 'entering ping fiber' if @debug
        while @running do
          if @last_packet_sent_at.nil? || @state != :mqtt_connected
            #debug "sleeping for #{@keepalive_sec} sec"
            sleep @keepalive_sec
          else
            #only send when needed (store time, and adjust sleep with it)
            while ((t = @last_packet_sent_at + @keepalive_sec - Time.now) >= 0) do
              #debug "sleeping for #{t} sec"
              sleep t
            end
            pingreq
          end
        end
        puts 'exiting ping fiber'
      end

      if @running
        send_packet(@packet_connect)
      end
    end
  end

  def debug(x)
    if @debug
      print Time.now.strftime('%Y.%m.%d %H:%M:%S.%L ')
      puts x
    end
  end
end
