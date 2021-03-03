class Mqtt3TcpDisconnected < Exception
end

class Mqtt3
  attr_accessor :debug

  #connection params
  attr_accessor :host
  attr_accessor :ip
  attr_accessor :reconnect
  attr_accessor :keepalive_sec
  attr_accessor :client_id
  attr_accessor :clean_session
  attr_accessor :will_topic
  attr_accessor :will_payload
  attr_accessor :will_qos
  attr_accessor :will_retain
  attr_accessor :username
  attr_accessor :password


  #internal state
  attr_reader :last_packet_sent_at
  attr_reader :packet_id
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
    'RESERVED' ].freeze

  MQTT_CONNECT = 0x01
  MQTT_PUBLISH = 0x03
  MQTT_SUBSCRIBE = 0x08

  def initialize(host,port,reconnect=true,keepalive_sec=30,client_id=nil,clean_session=true,will_topic=nil, will_payload=nil, will_qos=0, will_retain=false, username=nil, password=nil)
    @host = host
    @port = port
    @reconnect = reconnect
    @keepalive_sec = keepalive_sec
    @client_id = client_id
    if @client_id.nil?
      @client_id = File.basename($0)[0..10]
      charset = Array('A'..'Z') + Array('a'..'z') + Array('0'..'9')
      @client_id += '-' + Array.new(8) { charset.sample }.join
			puts @client_id
      #TODO insert fiber id
    end
    @clean_session = clean_session
    @will_topic = will_topic
    @will_payload = will_payload
    @will_qos = will_qos
    @will_retain = will_retain
    @username = username
    @password = password

    @socket = nil
    @packet_id = 0
    @qos1store = []
    @qos2store = []
    @packet_id = 0
    @running = true
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

  def connect
		body = encode_string('MQTT')
    body += encode_bytes 0x04

    flags = 0
    flags |= 0x02 if @clean_session
    flags |= 0x04 unless @will_topic.nil?
    flags |= ((@will_qos & 0x03) << 3)
    flags |= 0x20 if @will_retain
    flags |= 0x40 unless @password.nil?
    flags |= 0x80 unless @username.nil?
    body += encode_bytes(flags)

		body += encode_short(@keepalive_sec)
		body += encode_string(@client_id)
		unless will_topic.nil?
			body += encode_string(@will_topic)
			body += encode_string(@will_payload)
		end
		body += encode_string(@username) unless @username.nil?
		body += encode_string(@password) unless @password.nil?

    packet = encode_bytes(MQTT_CONNECT << 4)
    packet += encode_length(body.length)
    packet += body

    send_packet(packet)
  end

  def subscribe(topic_list)
    body = encode_short(next_packet_id())
    topic_list.each do |x|
      body += encode_string(x[0])
      body += encode_bytes(x[1])
    end

    flags = 2
    packet = encode_bytes((MQTT_SUBSCRIBE << 4) + flags)
    packet += encode_length(body.length)
    packet += body

    send_packet(packet)

    @packet_subscribe_test = "\x82\x09\x00\x01\x00\x04\x74\x65\x73\x74\x00".force_encoding('ASCII-8BIT')
  end

  def publish(topic,message,qos = 0,retain = false)
    raise 'Invalid topic name' if topic.nil? || topic.to_s.empty?
    raise 'Invalid QoS' if qos < 0 || qos > 2

    if qos == 1
      @qos1store.push()
    elsif qos == 2
      @qos2store.push()
    end

    flags = 0
    flags += 1 if retain
    flags += qos << 1
    #flags += 8 if duplicate

    body = encode_string(topic)
    body += encode_short(next_packet_id()) if qos > 0
    body += message

    packet = encode_bytes((MQTT_PUBLISH << 4) + flags)
    packet += encode_length(body.length)
    packet += body

    send_packet(packet)
  end

  def next_packet_id
    @packet_id += 1
    @packet_id = 0 if @packet_id > 0xffff
    return @packet_id
  end

	def encode_bytes(*bytes)
		bytes.pack('C*')
	end

	def encode_bits(bits)
		[bits.map { |b| b ? '1' : '0' }.join].pack('b*')
	end

	def encode_short(val)
		raise 'Value too big for short' if val > 0xffff
		[val.to_i].pack('n')
	end

	def encode_string(str)
		str = str.to_s.encode('UTF-8')

		# Force to binary, when assembling the packet
		str.force_encoding('ASCII-8BIT')
		encode_short(str.bytesize) + str
	end

  def encode_length(body_length)
    if body_length > 268_435_455
      raise 'Error serialising packet: body is more than 256MB'
    end

    x = ''
    loop do
      digit = (body_length % 128)
      body_length = body_length.div(128)
      # if there are more digits to encode, set the top bit of this digit
      digit |= 0x80 if body_length > 0
      x += digit.chr
      break if body_length <= 0
    end
    return x
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

  def tcp_connect()
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
          @socket = tcp_connect()
          connect()
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
      @socket = tcp_connect()

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
        connect()
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
