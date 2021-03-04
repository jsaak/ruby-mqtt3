require 'openssl'

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
  attr_accessor :persistence_filename

  attr_accessor :ssl
  attr_accessor :ssl_cert
  attr_accessor :ssl_cert_file
  attr_accessor :ssl_key
  attr_accessor :ssl_key_file
  attr_accessor :ssl_ca_file
  attr_accessor :ssl_passphrase

  #internal state
  attr_reader :last_packet_sent_at
  attr_reader :packet_id
  attr_reader :state
  attr_reader :ssl_context

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

  CONNECT = 1
  CONNACK = 2
  PUBLISH = 3
  PUBACK = 4
  PUBREC = 5
  PUBREL = 6
  PUBCOMP = 7
  SUBSCRIBE = 8
  SUBACK = 9
  UNSUBSCRIBE = 10
  UNSUBACK = 11
  PINGREQ = 12
  PINGRESP = 13
  DISCONNECT = 14

  def initialize(host: 'localhost',
                 port: 1883,
                 reconnect: true,
                 keepalive_sec: 30,
                 client_id: nil,
                 clean_session: true,
                 will_topic: nil,
                 will_payload: nil,
                 will_qos: 0,
                 will_retain: false,
                 username: nil,
                 password: nil,
                 persistence_filename: nil,
                 ssl: nil,
                 ssl_cert: nil,
                 ssl_cert_file: nil,
                 ssl_key: nil,
                 ssl_key_file: nil,
                 ssl_ca_file: nil,
                 ssl_passphrase: nil)
    @host = host
    @port = port
    @reconnect = reconnect
    @keepalive_sec = keepalive_sec
    @client_id = client_id
    if @client_id.nil?
      @client_id = File.basename($0)[0..10]
      charset = Array('A'..'Z') + Array('a'..'z') + Array('0'..'9')
      @client_id += '-' + Array.new(8) { charset.sample }.join
      #TODO insert fiber id
    end
    @clean_session = clean_session
    @will_topic = will_topic
    @will_payload = will_payload
    @will_qos = will_qos
    @will_retain = will_retain
    @username = username
    @password = password
    @persistence_filename = persistence_filename

    @ssl = ssl
    @ssl_cert = ssl_cert
    @ssl_cert_file = ssl_cert_file
    @ssl_key = ssl_key
    @ssl_key_file = ssl_key_file
    @ssl_ca_file = ssl_ca_file
    @ssl_passphrase = ssl_passphrase

    init_ssl() if @ssl

    @socket = nil
    @packet_id = 0
    @qos1store = Hash.new
    @qos2store = Hash.new
    @packet_id = 0
    @running = true
  end

  def pingreq
    send_packet("\xc0\x00".force_encoding('ASCII-8BIT')) #PINGREQ
  end

  def pingresp
    send_packet("\xd0\x00".force_encoding('ASCII-8BIT')) #PINGRESP
  end

  def disconnect
    send_packet("\xe0\x00".force_encoding('ASCII-8BIT')) #DISCONNECT
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

    packet = encode_bytes(CONNECT << 4)
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
    packet = encode_bytes((SUBSCRIBE << 4) + flags)
    packet += encode_length(body.length)
    packet += body

    send_packet(packet)
  end

  def publish(topic,message,qos = 0,retain = false)
    publish_dup(topic,message,qos,retain,false,nil)
    if @state != :mqtt_connected
      save()
    end
  end

  def publish_dup(topic,message,qos = 0,retain = false, dup = false, packet_id = nil)
    raise 'Invalid topic name' if topic.nil? || topic.to_s.empty?
    raise 'Invalid QoS' if qos < 0 || qos > 2

    # first publish
    if packet_id.nil?
      packet_id = next_packet_id()

      if qos == 1
        @qos1store[packet_id] = [topic,message,qos,retain]
      elsif qos == 2
        @qos2store[packet_id] = [topic,message,qos,retain,PUBLISH]
      end
    end


    flags = 0
    flags += 1 if retain
    flags += qos << 1
    flags += 8 if dup

    body = encode_string(topic)
    body += encode_short(packet_id) if qos > 0
    body += message

    packet = encode_bytes((PUBLISH << 4) + flags)
    packet += encode_length(body.length)
    packet += body

    send_packet(packet)
    return packet_id
  end

  def puback(packet_id)
    packet = "\x42\x02".force_encoding('ASCII-8BIT') #PUBACK
    packet += encode_short(packet_id)
    send_packet(packet)
  end

  def pubrec(packet_id)
    packet = "\x52\x02".force_encoding('ASCII-8BIT') #PUBREC
    packet += encode_short(packet_id)
    send_packet(packet)
  end

  def pubrel(packet_id)
    packet = "\x62\x02".force_encoding('ASCII-8BIT') #PUBREL
    packet += encode_short(packet_id)
    send_packet(packet)
  end

  def pubcomp(packet_id)
    packet = "\x72\x02".force_encoding('ASCII-8BIT') #PUBCOMP
    packet += encode_short(packet_id)
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

  def decode_short(bytes)
    bytes.unpack('n').first
  end

  def send_packet(p)
    return if state == :disconnected
    return if state == :tcp_connected && ((p[0].ord >> 4) != CONNECT)

    debug '--- ' + MQTT_PACKET_TYPES[p[0].ord >> 4] + ' flags: ' + (p[0].ord & 0x0f).to_s + '  ' + p.unpack('H*').first

    begin
      @socket.write(p)
      @last_packet_sent_at = Time.now
    rescue => e
      raise e
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
    @on_message_block = block
  end

  def handle_packet(type,flags,length,data)
    debug "+++ #{MQTT_PACKET_TYPES[type]} flags: #{flags}  length: #{length}  data: #{data.unpack('H*').first}"
    case type
    when CONNACK
      return_code = data[1].ord
      if return_code == 0
        @state = :mqtt_connected
        session_present = (data[0].ord == 1)
        @on_connect_block.call(session_present) unless @on_connect_block.nil?

        #sending QoS 1 and Qos2 messages
        @qos1store.each do |packet_id,m|
          publish_dup(m[0],m[1],m[2],m[3],true,packet_id)
        end
      else
        @on_mqtt_connect_error_block.call(return_code) unless @on_mqtt_connect_error_block.nil?
      end

    when PUBLISH
      qos = (flags & 6) >> 1
      topic_length = decode_short(data[0..1])
      topic = data[2..topic_length+2]
      packet_id = decode_short(data[topic_length+2..topic_length+3])
      message = data[topic_length+4..-1]

      if qos == 1
        puback(packet_id)
      elsif qos == 2
        pubrec(packet_id)
      end
      @on_message_block.call(topic, message, qos, packet_id) unless @on_message_block.nil?

    when PUBACK
      packet_id = decode_short(data)
      if @qos1store.has_key? (packet_id)
        @qos1store.delete(packet_id)
        @on_publish_finished_block.call(packet_id) unless @on_publish_finished_block.nil?
      else
        debug "WARNING: PUBACK #{packet_id} not found"
      end

    when PUBREC
      packet_id = decode_short(data)
      p = @qos2store[packet_id]
      unless p.nil?
        if p[4] == PUBLISH
          @qos2store[packet_id][4] = PUBREL
          pubrel(packet_id)
        else
          debug "WARNING: PUBREC #{packet_id} not in PUBLISH state"
        end
      else
        debug "WARNING: PUBREC #{packet_id} not found"
      end

    when PUBREL
      packet_id = decode_short(data)
      pubcomp(packet_id)

    when PUBCOMP
      packet_id = decode_short(data)
      p = @qos2store[packet_id]
      unless p.nil?
        if p[4] == PUBREL
          @qos2store.delete(packet_id)
        else
          debug "WARNING: PUBCOMP #{packet_id} not in PUBLISH state"
        end
      else
        debug "WARNING: PUBCOMP #{packet_id} not found"
      end

    when SUBACK
      # for each topic
      #@on_subscribe_block.call(topic_name, packet_id, ret)

    when PINGREQ
      pingresp
    when PINGRESP
    else
      debug "WARNING: packet type: #{type} is not handled"
    end
  end

  def tcp_connect()
    counter = 0
    begin
      tcp_socket = TCPSocket.new(@host, @port, connect_timeout: 1)

			if @ssl
				@socket = OpenSSL::SSL::SSLSocket.new(tcp_socket, @ssl_context)
				@socket.sync_close = true
				# Set hostname on secure socket for Server Name Indication (SNI)
				#TODO ??? @socket.hostname = @host if @socket.respond_to?(:hostname=)
				@socket.connect
			else
				@socket = tcp_socket
			end

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

  def save
    if @persistence_filename
      File.open(@persistence_filename,"w+") do |f|
        debug "saving state to " + @persistence_filename
        f.write Marshal.dump([@qos1store,@qos2store])
      end
    end
  end

  def recv_bytes(count)
    buffer = ''
    while buffer.length != count
      #TODO rescue
      chunk = @socket.read(count - buffer.length)
      if chunk == ''
        @state = :disconnected

        save()

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

  def init_ssl
    if (@ssl &&
        (@ssl_cert or @ssl_cert_file) &&
        (@ssl_ca_file) &&
        (@ssl_key or @ssl_key_file))

      @ssl_cert = File.read(@ssl_cert_file) if @ssl_cert_file
      @ssl_key = File.read(@ssl_key_file) if @ssl_key_file

      @ssl_context = OpenSSL::SSL::SSLContext.new

      unless @ssl.is_a?(TrueClass)
        @ssl_context.ssl_version = @ssl
      end

      @ssl_context.cert = OpenSSL::X509::Certificate.new(@ssl_cert)
      @ssl_context.key  = OpenSSL::PKey::RSA.new(@ssl_key, @ssl_passphrase)
      @ssl_context.ca_file  = @ssl_ca_file
      @ssl_context.verify_mode = OpenSSL::SSL::VERIFY_PEER
    else
      raise "missing ssl param"
    end
  end

  def run
    #persistence
    if @persistence_filename
      if @clean_session
        if File.exist?(@persistence_filename)
          debug "removing file " + @persistence_filename
          File.delete(@persistence_filename)
        end
      else
        if File.exist?(@persistence_filename)
          debug "loading state from " + @persistence_filename
          @qos1store, @qos2store = Marshal.load(File.read(@persistence_filename))
        end
      end
    end

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
