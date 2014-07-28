
12 分布式访问

12.1 OPI 消息

    OPI.AMessage <: Metadata::Composite -owns- {
        VERSION = 0,
        VALUE = 1,
        HEADER = 2,
        METHOD = 3,
        trait -owns- {
            is_valid : Function -spec- {
                -parameter- ():Bool
                -process- begin
                    (self[VERSION].is_mdtype(Element) and self[VERSION].size = sizeof(int32)
                    and self[VALUE].is_mdtype(Composite) and self[VALUE].size = sizeof(int32)
                    and self[HEADER].is_valid()
                    and self[METHOD].is_valid());
                    end
                },
            version : Function -spec- {
                -parameter- ():int32
                -process- begin
                    self[VERSION].data.asType(int32);
                    end
                },
            message_value : Function -spec- {
                -parameter- ():{"ADV","CAN","INF","REQ"}
                -process- begin
                    self[VALUE].data.asType({"ADV","CAN","INF","REQ"});
                    end
                },
            header : Function -spec- {
                -parameter- ():OPI.AMessage.Header
                -process- begin
                    self[HEADER].asType(OPI.AMessage.Header);
                    end
                },
            method : Function -spec- {
                -parameter- ():OPI.AMessage.Method
                -process- begin
                    self[METHOD].asType(OPI.AMessage.Method);
                    end
                },
            },
        }

    OPI.AMessage.Header <: Metadata.Composite -owns- {
        MSGID := int64,
        MESSAGEID = 0,
        CORRELATIONID = 1,
        RECEIVERS = 2,
        SENDER = 3,
        SENDTIME = 4,
        REPLYTO = 5,
        OPTIONS = 6,
        trait -owns- {
            is_valid : Function -spec- {
                -parameter- ():Bool
                -process- begin
                    (self[MESSAGEID].is_mdtype(Element) and self[MESSAGEID].size = sizeof(MSGID)
                    and self[CORRELATIONID].is_mdtype(Element) and self[CORRELATIONID].size = sizeof(MSGID)
                    and self[SENDTIME].is_mdtype(Element) and self[SENDTIME].size = sizeof(int64)
                    and self.receivers().is_valid()
                    and self.sender().is_valid()
                    and self.reply_to().is_valid()
                    and self.options().is_valid());
                    end
                },
            message_id : Function -spec- {
                -parameter- ():MSGID
                -process- begin
                    self[MESSAGEID].data.asType(int64);
                    end
                },
            correlation_id : Function -spec- {
                -parameter- ():MSGID
                -process- begin
                    self[CORRELATIONID].data.asType(int64);
                    end
                },
            receivers : Function -spec- {
                -parameter- ():OPI.Pentry
                -process- begin
                    self[RECEIVERS].asType(OPI.Pentry);
                    end
                },
            sender : Function -spec- {
                -parameter- ():OPI.Pentry
                -process- begin
                    self[SENDER].asType(OPI.Pentry);
                    end
                },
            send_time : Function -spec- {
                -parameter- ():int64
                -process- begin
                    self[SENDTIME].data.asType(int64);
                    end
                },
            reply_to : Function -spec- {
                -parameter- ():OPI.Pentry
                -process- begin
                    self[REPLYTO].asType(OPI.Pentry);
                    end
                },
            options : Function -spec- {
                -parameter- ():OPI.AMessage.Options
                -process- begin
                    self[OPTIONS].asType(OPI.AMessage.Options);
                    end
                },
            },
        }

    OPI.AMessage.Options : Metadata.Composite -owns- {
        model -owns- {
            acknowledge : Function -spec- {
                -parameter- ():Bool
                -process- begin
                    self[0].data.asType(Bool);
                    end
                },
            relay : Function -spec- {
                -parameter- ():Bool
                -process- begin
                    self[1].data.asType(Bool);
                    end
                },
            expire : Function -spec- {
                -parameter- ():int64
                -process- begin
                    self[2].data.asType(int64);
                    end
                },
            },
        }

    OPI.advertise_message = "ADV"
    OPI.cancel_message = "CAN"
    OPI.inform_message = "INF"
    OPI.request_message = "REQ"

    message of actor command:
    message : OPI.AMessage
        amessage.method.name : {"run", "stop"} 
        amessage.method.data : ActorCommand

    ActorCommand -owns- {
        model -owns- {
            action : List(String) or OID
            arguments : List()
            },
        }
    ActorCommand.model.action.first => space

12.2 Netcom

    Socket : Class -owns- {
        Address,
        fd_set,

        model -owns- {
            },
        trait -owns- {
            accept,
            bind,
            connect,
            listen,
            recv,
            recvfrom,
            select,
            send,
            sendto,
            select_read,
            select_write
            }
        }

    SendMessageRequest : Class -spec- {
        model -owns- {
            message : Ref(OPI.AMessage),
            state : UDPCom.State,
            expire : int32,
            callback,
            },
        }

    SendAckRequest : Class -owns- {
        model -owns- {
            message : Ref(AckPacket),
            state : UDPCom.State,
            to_addr : Socket.Address,
            }
        }

    UDPCom <: NetCom -owns- {
        model -owns- {
            size : int32,
            cache : Heap,
            socket : Socket,
            timeout : int32,
            sender : OPI.Pentry;
            performer : Native.Actor.Performer,
            send_queue : (Queue;)[3],
            wait_queue : (Queue;)[3],
            in_worker : Task,
            out_worker : Task,
            },
        trait -owns- {
            construct : Function -spec- {
                -parameter- (cache_size:int32,
                             sender:String)
                -process- begin
                    self.in_worker.construct();
                    self.out_worker.construct(self.out_worker_handler);
                    self.size << size;
                    self.cache.construct(memory_address_of(self)+sizeof(self),cache_size-sizeof(self));
                    self.sender.set(sender,0,PETYPE_UDP,0);
                    end
                },
            allocate : Function -spec- {
                -parameter- (Ty)(n:int32):Ref()
                -process- begin
                    self.cache.allocate(n*sizeof(Ty));
                    end
                },
            deallocate : Function -spec- {
                -parameter- (base:Ref(),
                             size:int32)
                -process- begin
                    self.cache.deallocate(base,size);
                    end
                },
            close : Function -spec- {
                -parameter- ()
                -process- begin
                    self.socket.close();
                    self.in_worker.stop();
                    self.out_worker.stop();
                    end
                },
            open : Function -spec- {
                -parameter- (addr:Socket.Address,
                             timeout:int32)
                -process- begin
                    self.sender.host << addr.ip_address;
                    self.sender.entry << addr.ip_port;
                    self.timeout << timeout;
                    self.socket.open(AF_INET,SOCK_DGRAM,IPPROTO_UDP);
                    self.socket.bind(addr);
                    self.in_worker.activate();
                    self.out_worker.activate();
                    end
                },
            out : Function -spec- {
                -parameter- (message:OPI.AMessage,
                             callback)
                -process- begin
                    when message.size > max_packet_size then
                        throw Error("overflow");
                    when not message.is_valid() then
                        throw Error("invalid message");
                    else
                        let req = SendMessageRequest.make(message,state_wait_send[try_1],callback) in
                            req.expire << null;
                            self.send_queue[try_1].enqueue(req);
                    end
                },
            input_worker_run : Function -spec- {
                -parameter- ()
                -process- begin
                    let read_fd = self.socket.make_fd_set(),
                        read_buf = SBlock.make(self.allocator.allocate(max_packet_size),max_packet_size),
                        from_addr = Socket.Address.make()
                     in
                        (λ.-process- begin
                            let recv_res = self.read(read_buf,from_addr,read_fd) in
                                if recv_res > 0 then
                                    let packet = read_buf.asType(Metadata.Element) in
                                        when packet.type = PACKET_ACK then
                                            self.recv_ack(packet.data.asType(OPI.AMessage.Header.MSGID));
                                        when packet.is_mdtype(Metadata.Composite) then
                                            if packet.size <= recv_res then
                                                self.parse(packet,from_addr);
                                            else
                                                let header = packet.asType(OPI.AMessage).header() in
                                                    if header = null or not header.is_valid() then
                                                        self.response_error(from_addr,null,"invalid message");
                                                    else self.response_error(from_addr,header,"lost data");
                                    this();
                            end )();
                    end
                },
            output_worker_run : Function -spec- {
                -parameter- ()
                -process- begin
                    let write_fd = self.socket.make_fd_set() in
                        self.handle_try_1(write_fd);
                        self.handle_try_2(write_fd);
                        self.handle_try_3(write_fd);
                    end
                },
            handle_try_1 : Function -spec- {
                -parameter- (write_fd)
                -process- begin
                    (λq.-process- begin
                        if not q.empty() then
                            let req = q.peek() then
                                let packet = req.message in
                                    when packet.type = PACKET_ACK then
                                        if self.write(packet,Element.size_of(packet.size),req.to_addr,self.write_fd) = success then
                                            req.state << state_ok;
                                            req.callback(packet,SUCCESS);
                                        else
                                            req.state << state_wait_send[try_2];
                                            self.send_queue[try_2].enqueue(req);
                                    when packet.is_mdtype(Metadata.Composite) then
                                        let header = packet.asType(OPI.AMessage).header() in
                                            self.config_packet(packet.asType(OPI.AMessage));
                                            let to_addr = Sokcet.Address.make(header.receiver().host_id(),header.receiver().entry()) in
                                                let send_res = self.write(packet,packet.block_size(),to_addr,self.write_fd) in
                                                    if send_res = success then
                                                        req.state << state_wait_ack[try_1];
                                                        req.expire << System.Time.now()+self.timeout;
                                                        self.wait_queue[try_1].enqueue(req);
                                                    else
                                                        req.state << state_wait_send[try_2];
                                                        self.send_queue[try_2].enqueue(req);
                                q.dequeue();
                        end )(self.send_queue[try_1]);
                    (λq.-process- begin
                        if not q.empty() then
                            let req = q.peek() in
                                when req.state = state_ok then
                                    req.callback(req.message,success);
                                    q.dequeue();
                                    this();
                                when req.state = state_wait_ack[try_1] then
                                    if req.expire <= System.Time.now() then
                                        req.state << state_wait_send[try_2];
                                        self.send_queue[try_2].enqueue(req);
                                        q.dequeue();
                                        this();
                        end )(self.wait_queue[try_1]);
                    end
                },
            handle_try_2 : Function -spec- {
                -parameter- (write_fd)
                -process- begin
                    (λq.-process- begin
                        if not q.empty() then
                            let req = q.peek() then
                                let packet = req.message in
                                    if req.state = state_ok then
                                        req.callback(packet,success);
                                    else
                                        when packet.type = PACKET_ACK then
                                            if self.write(packet,Element.size_of(packet.size),req.to_addr,self.write_fd) = success then
                                                req.state << state_ok;
                                                req.callback(packet,SUCCESS);
                                            else
                                                req.state << state_wait_send[try_3];
                                                self.send_queue[try_3].enqueue(req);
                                        when packet.is_mdtype(Metadata.Composite) then
                                            let header = req.message.header() in
                                                header.update(SENDTIME,System.Time.now());
                                                let to_addr = Sokcet.Address.make(header.receiver().host_id(),header.receiver().entry()) in
                                                    let send_res = self.write(packet,packet.block_size(),to_addr,self.write_fd) in
                                                        if send_res = success then
                                                            req.state << state_wait_ack[try_2];
                                                            req.expire << System.Time.now()+self.timeout;
                                                            self.wait_queue[try_2].enqueue(req);
                                                        else
                                                            req.state << state_wait_send[try_3];
                                                            self.send_queue[try_3].enqueue(req);
                                q.dequeue();
                        end )(self.send_queue[try_2]);
                    (λq.-process- begin
                        if not q.empty() then
                            let req = q.peek() in
                                when req.state = state_ok then
                                    req.callback(req.message,success);
                                    q.dequeue();
                                    this();
                                when req.state = state_wait_ack[try_2] then
                                    if req.expire <= System.Time.now() then
                                        req.state << state_wait_send[try_3];
                                        self.send_queue[try_3].enqueue(req);
                                        q.dequeue();
                                        this();
                        end )(self.wait_queue[try_2]);
                    end
                },
            handle_try_3 : Function -spec- {
                -parameter- (write_fd)
                -process- begin
                    (λq.-process- begin
                        if not q.empty() then
                            let req = q.peek() then
                                let packet = req.message in
                                    if req.state = state_ok then
                                        req.callback(packet,success);
                                    else
                                        when packet.type = PACKET_ACK then
                                            if self.write(packet,Element.size_of(packet.size),req.to_addr,self.write_fd) = success then
                                                req.state << state_ok;
                                                req.callback(packet,SUCCESS);
                                            else
                                                req.state << state_fail;
                                                req.callback(req.message,fail);
                                        when packet.is_mdtype(Metadata.Composite) then
                                            let header = req.message.header() in
                                                header.update(SENDTIME,System.Time.now());
                                                let to_addr = Sokcet.Address.make(header.receiver().host_id(),header.receiver().entry()) in
                                                    let send_res = self.write(packet,packet.block_size(),to_addr,self.write_fd) in
                                                        if send_res = success then
                                                            req.state << state_wait_ack[try_3];
                                                            req.expire << System.Time.now()+self.timeout;
                                                            self.wait_queue[try_3].enqueue(req);
                                                        else
                                                            req.state << state_fail;
                                                            req.callback(req.message,send_res);
                                q.dequeue();
                        end )(self.send_queue[try_3]);
                    (λq.-process- begin
                        if not q.empty() then
                            let req = q.peek() in
                                when req.state = state_ok then
                                    req.callback(req.message,success);
                                    q.dequeue();
                                    this();
                                when req.state = state_wait_ack[try_3] then
                                    if req.expire <= System.Time.now() then
                                        req.state << state_fail;
                                        req.callback(req.message,(fail,no_ack));
                                        q.dequeue();
                                        this();
                        end )(self.wait_queue[try_3]);
                    end
                },
            parse : Function -spec- {
                -parameter- (message:OPI.AMessage,
                             from:Socket.Address)
                -process- begin
                    let header = message.header() in
                        if message.is_valid() then
                            self.send_ack(from,header.message_id());
                            header.sender().update(OPI.Pentry.HOST_ID,from.ip_address);
                            header.sender().update(OPI.Pentry.ENTRY_TYPE,PETYPE_UDP);
                            header.sender().update(OPI.Pentry.ENTRY,from.ip_port);
                            if success != self.transfer(message) then
                                self.response_error(from,header,"declined");
                        else
                            if header = null or not header.is_valid() then 
                                self.response_error(from,null,"invalid message");
                            else self.response_error(from,header,"invalid message");
                    end
                },
            transfer : Function -spec- {
                -parameter- (message:OPI.AMessage):{success,fail}
                -process- begin
                    let transfer_msg = self.performer.allocate(OPI.AMessage) in
                        copy message to transfer_msg;
                        this.result << self.performer.request(transfer_msg);
                    end
                },
            response_error : Function -spec- {
                -parameter- (netaddr:Socket.Address,
                             header:OPI.AMessageHeader,
                             content:Metadata)
                -process- begin
                    let send_msg = self.allocate(def_message_size).asType(OPI.AMessage) then
                        send_msg.assign(OPI.inform_message,
                            OPI.new_message_id(),
                            header.message_id(),
                            if header.reply_to() != null then header.reply_to()
                            else OPI.Pentry.make("",netaddr.ip_address,PETYPE_UDP,netaddr.ip_port),
                            self.sender,
                            System.Time.now(),
                            null,
                            null,
                            "error",
                            content);
                        self.out(send_msg,(λ(msg,res).-process- begin self.deallocate(msg,def_message_size) end));
                    end
                },
            recv_ack : Function -spec- {
                -parameter- (ack_id:OPI.AMessage.MSGID):{success,fail}
                -process- begin
                    let queue = self.send_queue[try_2] in
                        (λit:Queue.iterator.-process- begin
                            if it != queue.end() then
                                if it.message.header().message_id() = ack_id then
                                    it.state << state_ok;
                                else this(it.next);
                            end)(queue.first);
                    let queue = self.send_queue[try_3] in
                        (λit:Queue.iterator.-process- begin
                            if it != queue.end() then
                                if it.message.header().message_id() = ack_id then
                                    it.state << state_ok;
                                else this(it.next);
                            end)(queue.first);
                    let queue = self.wait_queue[try_3] in
                        (λit:Queue.iterator.-process- begin
                            if it != queue.end() then
                                if it.message.header().message_id() = ack_id then
                                    it.state << state_ok;
                                else this(it.next);
                            end)(queue.first);
                    let queue = self.wait_queue[try_2] in
                        (λit:Queue.iterator.-process- begin
                            if it != queue.end() then
                                if it.message.header().message_id() = ack_id then
                                    it.state << state_ok;
                                else this(it.next);
                            end)(queue.first);
                    let queue = self.wait_queue[try_1] in
                        (λit:Queue.iterator.-process- begin
                            if it != queue.end() then
                                if it.message.header().message_id() = ack_id then
                                    it.state << state_ok;
                                else this(it.next);
                            end)(queue.first);
                    end
                },
            send_ack : Function -spec- {
                -parameter- (netaddr:Socket.Address,
                             msg_id):{success,fail}
                -process- begin
                    let packet = self.allocate(def_ack_size).asType(AckPacket) in
                        packet.construct();
                        packet.assign(msg_id);
                        let req = AckRequest.make(packet,netaddr) in
                            self.out(req,(λ(msg,res).-process- begin self.deallocate(msg,def_ack_size) end));
                    end
                },
            read : Function -spec- {
                -parameter- (buf:Ref(),
                             buf_size:int32,
                             from_addr:Socket.Address,
                             read_fd:Socket.fd_set):Number
                -process- begin
                    let select_res = self.socket.select_read(read_fd,self.timeout) in
                        if select_res = success then
                            let recv_res = self.socket.recvfrom(buf,buf_size,0,from_addr) in
                                when recv_res = SOCKET_ERROR then
                                    throw Error();
                                else this.result << recv_res;
                        else this.result << fail;
                    end
                },
            write : Function -spec- {
                -parameter- (packet:OPI.AMessage,
                             packet_size:int32,
                             to_addr:Socket.Address,
                             write_fd:Socket.fd_set):Number
                -process- begin
                    let select_res = self.socket.select_write(write_fd,self.timeout) in
                        if select_res = success then
                            let send_res = self.socket.sendto(packet,packet_size,to_addr) in
                                when send_res = SOCKET_ERROR then
                                    throw Error();
                                else this.result << send_res;
                        else this.result << fail;
                    end
                },
            config_packet : Function -spec- {
                -parameter- (packet:OPI.AMessage)
                -process- begin
                    packet.header().update(OPI.AMessageHeader.SENDER,self.sender);
                    packet.header().update(OPI.AMessageHeader.SENDTIME,System.Time.now();
                    end
                },
            },

        new : Function -spec- {
            -parameter- (cache_size:int32,
                         sender:String):UDPCom
            -process- begin
                let instance = System.Memory.allocate(cache_size).asType(UDPCom) >>
                    instance.construct(cache_size,sender);
                end
            },
        delete : Function -spec- {
            -parameter- (this_:Ref(UDPCom))
            -process- begin
                System.Memory.deallocate(memory_address_of(this_));
                end
            }
        }

    TCPCom <: NetCom

12.3 HTTPCom

    交互数据的格式
        OID data
            ["OID", id] <=> Metadata.Composite
            id:OID

        name path
            ["name", n1, n2, ...] <=> Metadata.Composite
            ni:String

        一般数据
            "string" <=> Metadata.Element
            number <=> Metadata.Element

    http daemon for romp::actor
        http_listener
            listen
            accept -> (http_socket,sockaddr) -> http_handler(Action)

        http_handler
            read from http_socket -> http_request
            parse http_request -> opi_request(application,method,arguments,invoker)
            send opi_request to romp::actor
                if actor has alive tcp channel then send by actor's alive channel
                else send by udpcom
            receive result from romp::actor
                read opi_response from netcom 
            parse result -> http_response
            send http_response by http_socket
