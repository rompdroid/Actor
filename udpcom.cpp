////////////////////////////////////////////////////////////////////////////////////////////////
//
// udpcom.cpp
// Copyright (c) 2012 by Xie Yun .
//
////////////////////////////////////////////////////////////////////////////////////////////////

#include <rompdefs.h>
#include <native/actor/actor.h>
#include <native/actor/perform/perform.inl>
using namespace ROMP::Net;

static UString method_error = L"error";

// UDPCom::InputWork and UDPCom::OutputWorker
//
UDPCom::InputWorker::InputWorker()
    : Super("UDPCom::input"),
    m_owner()
{
}

void
UDPCom::InputWorker::construct( UDPCom& owner )
{
    this->m_owner = &owner;
}

Int32
UDPCom::InputWorker::run( ActInstance& act_ins )
{
    this->m_owner->input_worker_run();
    return SUCCESS;
}

UDPCom::OutputWorker::OutputWorker()
    : Super("UDPCom::output"),
    m_owner()
{
}

void
UDPCom::OutputWorker::construct( UDPCom& owner )
{
    this->m_owner = &owner;
}

Int32
UDPCom::OutputWorker::run( ActInstance& act_ins )
{
    this->m_owner->output_worker_run();
    return SUCCESS;
}

void
UDPCom::OutputWorker::stop()
{
    if (this->instance()) {
        this->m_admin->kill(*const_cast<ActInstance*>(this->instance()), FAIL);
    }
}

// UDPCom
//
UDPCom::UDPCom()
    : m_size(0),
    m_cache(null, 0),
    m_socket(),
    m_performer(),
    m_in_worker(),
    m_out_worker()
{
    this->m_in_worker.construct(*this);
    this->m_out_worker.construct(*this);
}

UDPCom::~UDPCom()
{
    Self::delete$(this);
}

UDPCom::pointer
UDPCom::new$( size_type cache_size, const UString& sender )
{
    pointer instance = static_cast<pointer>(System::Memory::allocate(cache_size));
    ::new (instance) Self();
    instance->construct(cache_size, sender);
    return instance;
}

void
UDPCom::delete$( Self::pointer this_ )
{
    System::Memory::deallocate(this_);
}

void
UDPCom::construct( size_type size, const UString& sender )
{
    this->m_size = size;
    MemoryAddress base = (MemoryAddress)(this) + sizeof(Self);
    base += cache_increment - 1;
    base &= ~(cache_increment - 1);
    size -= base - (MemoryAddress)this;
    this->m_cache.construct((void*)base, size);
    this->m_sender = Pentry::new$(*this, sender, 0L, PETYPE_UDP, 0L);
}

void
UDPCom::open( Socket::Address& addr, Int32 timeout )
{
    this->m_sender->update(Pentry::HOST_ID, ::ntohl(addr.sin_addr.s_addr));
    this->m_sender->update(Pentry::ENTRY, (UInt32)::ntohs(addr.sin_port));
    this->m_timeout = timeout;
    this->m_socket.open(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (this->m_socket.bind(addr) == SOCKET_ERROR) {
        throw sysError(::WSAGetLastError());
    }
    this->m_in_worker.activate();
    this->m_out_worker.activate();
//////////
    printf("UPDCom opened at %s:%u\n",
        ::inet_ntoa(addr.sin_addr),
        ::htons(addr.sin_port));
}

void
UDPCom::close()
{
    this->m_socket.close();
    this->m_in_worker.stop();
    this->m_out_worker.stop();
//////////
    printf("UPDCom closed\n");
}

void
UDPCom::attach( Performer& perform )
{
    this->m_performer = &perform;
}

Int32
UDPCom::out( AMessage& msg, SendRequest::Callback& callback )
{
    if (!msg.is_valid()) {
        XTHROW("invalid message");
    }
    if (msg.block_size() > max_packet_size) {
        XTHROW("overflow");
    }
    SendRequest req = {&msg, state_wait_send_1, 0LL, 0};
    req.m_callback = ::new SendRequest::Callback(callback);
    this->m_send_queue[try_1].enqueue(req);
    return SUCCESS;
}

void
UDPCom::config_packet( AMessage& msg )
{
    AMessageHeader& header = msg.header();
    Int32 delta = this->m_sender->block_size() - header.sender().block_size();
    void* pos_1 = msg.item<Metadata::Element>(AMessage::METHOD);
    void* pos_2 = (void*)((UInt32)pos_1 + delta);
    ::memmove(pos_2, pos_1, msg.block_size());
    msg.size += delta;
    header.update(AMessageHeader::SENDER, *this->m_sender);
    header.update(AMessageHeader::SEND_TIME, getTime());
}

void
UDPCom::input_worker_run()
{
    Socket::Address from_addr;
    void* read_buf = this->allocate<void>(max_packet_size);
    Socket::fd_set* read_fd =
        this->allocate<Socket::fd_set>(sizeof(Socket::fd_set));
    while (true) {
        Int32 res = this->read(read_buf, max_packet_size, from_addr, read_fd);
        if (res > 0) {
            Metadata::Element::pointer packet =
                static_cast<Metadata::Element::pointer>(read_buf);
            switch (packet->type) {
                case PACKET_ACK:
                    this->recv_ack(*packet->data<AMessage::MSGID>());
                    break;
                case MDTYPE_COMPOSITE:
                    if ((UInt32)res >= packet->block_size()) {
                        this->parse(static_cast<AMessage&>(*packet), from_addr);
                    }
                    else {
                        AMessageHeader::pointer p_head =
                            static_cast<AMessage::pointer>(packet)
                                ->item<AMessageHeader>(AMessage::HEADER);
                        if (p_head && p_head->is_valid()) {
                            this->response_error(from_addr,
                                p_head,
                                UString(L"lost data"));
                        }
                        else {
                            this->response_error(from_addr,
                                0,
                                UString(L"invalid message"));
                        }
                    }
                    break;
                case MDTYPE_ELEMENT:
                    break;
            }
        }
    }
}

void
UDPCom::output_worker_run()
{
    Socket::fd_set* write_fd = this->allocate<Socket::fd_set>(sizeof(Socket::fd_set));
    while (true) {
        this->handle_try_1(write_fd);
        this->handle_try_2(write_fd);
        this->handle_try_3(write_fd);
        //::SwitchToThread();
        ::sleep(1);
    }
}

void
UDPCom::handle_try_1( Socket::fd_set* write_fd )
{
    Int32 send_res = 0;
    Queue& send_q = this->m_send_queue[try_1];
    Socket::Address to_addr;
    to_addr.sin_family = AF_INET;
    while (!send_q.empty()) {
        SendRequest& req = send_q.peek();
        Metadata::Composite::pointer p_packet = req.m_message;
        switch (p_packet->type) {
            case PACKET_ACK:
                send_res = this->write(p_packet,
                    Metadata::Element::size_of(p_packet->size),
                    req.m_u.m_addr,
                    write_fd);
                if (send_res > 0) {
                    req.m_state = state_ok;
                    this->callback(req, send_res);
                }
                else {
                    req.m_state = state_wait_send_2;
                    this->m_send_queue[try_2].enqueue(req);
                }
                break;
            case MDTYPE_COMPOSITE: {
                AMessage& msg = static_cast<AMessage&>(*p_packet);
                this->config_packet(msg);
                Pentry& receiver = msg.header().receiver();
                to_addr.sin_addr.s_addr = ::htonl(receiver.host_id());
                to_addr.sin_port = ::htons((UInt16)receiver.entry());
                send_res = this->write(p_packet,
                    p_packet->block_size(),
                    to_addr,
                    write_fd);
                if (send_res > 0) {
                    req.m_state = state_wait_ack_1;
                    req.m_u.m_expire = timeout2Expire(this->m_timeout);
                    this->m_wait_queue[try_1].enqueue(req);
                }
                else {
                    req.m_state = state_wait_send_2;
                    this->m_send_queue[try_2].enqueue(req);
                }
                } break;
        }
        send_q.dequeue();
    }
    Queue& wait_q = this->m_wait_queue[try_1];
    Int64 now = getTime();
    bool finish = false;
    while (!wait_q.empty() && !finish) {
        SendRequest& req = wait_q.peek();
        switch (req.m_state) {
            case state_ok:
                this->callback(req, SUCCESS);
                wait_q.dequeue();
                break;
            case state_wait_ack_1:
                if (req.m_u.m_expire <= now) {
                    req.m_state = state_wait_send_2;
                    this->m_send_queue[try_2].enqueue(req);
                    wait_q.dequeue();
                }
                else {
                    finish = true;
                }
                break;
            default:
                finish = true;
                break;
        }
    }
}

void
UDPCom::handle_try_2( Socket::fd_set* write_fd )
{
    Int32 send_res = 0;
    Queue& send_q = this->m_send_queue[try_2];
    Socket::Address to_addr;
    to_addr.sin_family = AF_INET;
    while (!send_q.empty()) {
        SendRequest& req = send_q.peek();
        if (req.m_state == state_ok) {
            this->callback(req, SUCCESS);
        }
        else {
            Metadata::Composite::pointer p_packet = req.m_message;
            switch (p_packet->type) {
                case PACKET_ACK:
                    send_res = this->write(p_packet,
                        Metadata::Element::size_of(p_packet->size),
                        req.m_u.m_addr,
                        write_fd);
                    if (send_res > 0) {
                        req.m_state = state_ok;
                        this->callback(req, send_res);
                    }
                    else {
                        req.m_state = state_wait_send_3;
                        this->m_send_queue[try_3].enqueue(req);
                    }
                    break;
                case MDTYPE_COMPOSITE: {
                    AMessage& msg = static_cast<AMessage&>(*p_packet);
                    msg.header().update(AMessageHeader::SEND_TIME, getTime());
                    Pentry& receiver = msg.header().receiver();
                    to_addr.sin_addr.s_addr = ::htonl(receiver.host_id());
                    to_addr.sin_port = ::htons((UInt16)receiver.entry());
                    send_res = this->write(p_packet,
                        p_packet->block_size(),
                        to_addr,
                        write_fd);
                    if (send_res > 0) {
                        req.m_state = state_wait_ack_2;
                        req.m_u.m_expire = timeout2Expire(this->m_timeout);
                        this->m_wait_queue[try_2].enqueue(req);
                    }
                    else {
                        req.m_state = state_wait_send_3;
                        this->m_send_queue[try_3].enqueue(req);
                    }
                    } break;
                default:
                    break;
            }
        }
        send_q.dequeue();
    }
    Queue& wait_q = this->m_wait_queue[try_2];
    Int64 now = getTime();
    bool finish = false;
    while (!wait_q.empty() && !finish) {
        SendRequest& req = wait_q.peek();
        switch (req.m_state) {
            case state_ok:
                this->callback(req, SUCCESS);
                wait_q.dequeue();
                break;
            case state_wait_ack_2:
                if (req.m_u.m_expire <= now) {
                    req.m_state = state_wait_send_3;
                    this->m_send_queue[try_3].enqueue(req);
                    wait_q.dequeue();
                }
                else {
                    finish = true;
                }
                break;
            default:
                finish = true;
                break;
        }
    }
}

void
UDPCom::handle_try_3( Socket::fd_set* write_fd )
{
    Int32 send_res = 0;
    Queue& send_q = this->m_send_queue[try_3];
    Socket::Address to_addr;
    to_addr.sin_family = AF_INET;
    while (!send_q.empty()) {
        SendRequest& req = send_q.peek();
        if (req.m_state == state_ok) {
            this->callback(req, SUCCESS);
        }
        else {
            Metadata::Composite::pointer p_packet = req.m_message;
            switch (p_packet->type) {
                case PACKET_ACK:
                    send_res = this->write(p_packet,
                        Metadata::Element::size_of(p_packet->size),
                        req.m_u.m_addr,
                        write_fd);
                    if (send_res > 0) {
                        req.m_state = state_ok;
                    }
                    else {
                        req.m_state = state_fail;
                    }
                    this->callback(req, send_res);
                    break;
                case MDTYPE_COMPOSITE: {
                    AMessage& msg = static_cast<AMessage&>(*p_packet);
                    msg.header().update(AMessageHeader::SEND_TIME, getTime());
                    Pentry& receiver = msg.header().receiver();
                    to_addr.sin_addr.s_addr = ::ntohl(receiver.host_id());
                    to_addr.sin_port = ::ntohs((UInt16)receiver.entry());
                    send_res = this->write(p_packet,
                        p_packet->block_size(),
                        to_addr,
                        write_fd);
                    if (send_res > 0) {
                        req.m_state = state_wait_ack_3;
                        req.m_u.m_expire = timeout2Expire(this->m_timeout);
                        this->m_wait_queue[try_3].enqueue(req);
                    }
                    else {
                        req.m_state = state_fail;
                        this->callback(req, send_res);
                    }
                    } break;
                default:
                    break;
            }
        }
        send_q.dequeue();
    }
    Queue& wait_q = this->m_wait_queue[try_3];
    Int64 now = getTime();
    bool finish = false;
    while (!wait_q.empty() && !finish) {
        SendRequest& req = wait_q.peek();
        switch (req.m_state) {
            case state_ok:
                this->callback(req, SUCCESS);
                wait_q.dequeue();
                break;
            case state_wait_ack_3:
                if (req.m_u.m_expire <= now) {
                    req.m_state = state_fail;
                    this->callback(req, FAIL);
                    wait_q.dequeue();
                }
                else {
                    finish = true;
                }
                break;
            default:
                finish = true;
                break;
        }
    }
}

void
UDPCom::parse( AMessage& msg, Socket::Address& from )
{
    AMessageHeader::pointer p_head =
        msg.item<AMessageHeader>(AMessage::HEADER);
    if (msg.is_valid()) {
        this->send_ack(from, p_head->message_id());
        Pentry& sender = p_head->sender();
        sender.update(Pentry::HOST_ID, ::ntohl(from.sin_addr.s_addr));
        sender.update(Pentry::ENTRY_TYPE, PETYPE_UDP);
        sender.update(Pentry::ENTRY, (UInt32)::ntohs(from.sin_port));
        this->transfer(msg);
    }
    else {
        if (p_head && p_head->is_valid()) {
            this->response_error(from, p_head, UString(L"invalid message"));
        }
        else {
            this->response_error(from, 0, UString(L"invalid message"));
        }
    }
}

void
UDPCom::response_error( Socket::Address& from,
    AMessageHeader::pointer p_head,
    UString& what )
{
    AMessage::pointer new_msg = 0;
    Pentry::pointer p_pentry = 0;
    try {
        AMessage::MSGID coid = 0;
        Self& self = *this;
        p_pentry = Pentry::new$(self,
            "",
            ::ntohl(from.sin_addr.s_addr),
            PETYPE_UDP,
            ::ntohs(from.sin_port));
        if (p_head) {
            coid = p_head->message_id();
            Metadata::Composite& mdc = p_head->reply_to();
            if (mdc.size) {
                Pentry& reply = static_cast<Pentry&>(mdc);
                p_pentry->update(Pentry::HOST_ID, reply.host_id());
                p_pentry->update(Pentry::ENTRY_TYPE, PETYPE_UDP);
                p_pentry->update(Pentry::ENTRY, reply.entry());
            }
        }
        BString method((const char*)method_error.c_str(),
            sizeof(UString::value_type) * method_error.size());
        new_msg = AMessage::new$(self,
            Enum::AMSG_INFORM,
            0LL,
            coid,
            p_pentry,
            self.m_sender,
            getTime(),
            NULL,
            NULL,
            method,
            what.c_str(),
            sizeof(UString::value_type) * what.size());
        this->out(*new_msg, SendRequest::Callback(self, &Self::on_send_complete));
    }
    catch (...) {
        this->deallocate(new_msg, def_message_size);
    }
    if (p_pentry) {
        this->deallocate(p_pentry, p_pentry->block_size());
    }
}

Int32
UDPCom::transfer( AMessage& msg )
{
    if (!this->m_performer) {
        return FAIL;
    }
    AMessage::pointer new_msg = 0;
    size_type size = msg.block_size();
    try {
        new_msg = static_cast<AMessage::pointer>(
            Metadata::Composite::new$(size, *this->m_performer));
        ::memcpy(new_msg, &msg, size);
        this->m_performer->request(*new_msg);
        return SUCCESS;
    }
    catch (...) {
        this->m_performer->deallocate(new_msg, size);
        Socket::Address from;
        AMessageHeader& header = msg.header();
        from.sin_addr.s_addr = ::htonl(header.sender().host_id());
        from.sin_port = ::htons((UInt16)header.sender().entry());
        this->response_error(from, &header, UString(L"request failure"));
        return FAIL;
    }
    //catch (char* ex) {
    //    this->m_performer->deallocate(new_msg, size);
    //    Socket::Address from;
    //    AMessageHeader& header = msg.header();
    //    from.sin_addr.s_addr = ::htonl(header.sender().host_id());
    //    from.sin_port = ::htons((UInt16)header.sender().entry());
    //    this->response_error(from, &header, BString(ex));
    //    return FAIL;
    //}
}

void
UDPCom::recv_ack( Int64 ackid )
{
    this->recv_ack(this->m_send_queue[try_2], ackid);
    this->recv_ack(this->m_send_queue[try_3], ackid);
    this->recv_ack(this->m_wait_queue[try_3], ackid);
    this->recv_ack(this->m_wait_queue[try_2], ackid);
    this->recv_ack(this->m_wait_queue[try_1], ackid);
}

void
UDPCom::recv_ack( Queue& Q, Int64 ackid )
{
    Queue::iterator end = Q.end();
    for (Queue::iterator it = Q.begin(); it != end; it++) {
        if (ackid == static_cast<AMessage::pointer>(it->m_message)->header().message_id()) {
            it->m_state = state_ok;
        };
    }
}

Int32
UDPCom::send_ack( Socket::Address& addr, Int64 msgid )
{
    Metadata::Element::pointer new_ack =
        Metadata::Element::new$(def_ack_size, *this);
    new_ack->assign(msgid);
    new_ack->type = PACKET_ACK;
    SendRequest req;
    req.m_message = static_cast<Metadata::Composite::pointer>(new_ack);
    req.m_state = state_wait_send_1;
    ::memcpy(&req.m_u.m_addr, &addr, sizeof(addr));
    req.m_callback = 
        ::new SendRequest::Callback(*this, &Self::on_send_complete);
    this->m_send_queue[try_1].enqueue(req);
    return SUCCESS;
}

Int32
UDPCom::read( void* base,
    size_type size,
    Socket::Address& from_addr,
    Socket::fd_set* read_fd )
{
    Int32 res = this->m_socket.select_read(read_fd, this->m_timeout);
    if (res == SOCKET_ERROR) {
        throw OPIException(sysError(::WSAGetLastError()));
    }
    if (res && FD_ISSET(this->m_socket.m_socket, read_fd)) {
        res = this->m_socket.recvfrom(base, size, 0, from_addr);
        if (res == SOCKET_ERROR) {
            Int32 error = ::WSAGetLastError();
            switch (error) {
                case WSAECONNRESET:
                    printf("send failure : port %u unreachable\n",
                        ::ntohs(from_addr.sin_port));
                    break;
                default:
                    throw OPIException(sysError(error));
                    break;
            }
        }
    }
    return res;
}

Int32
UDPCom::write( void* base,
    size_type size,
    Socket::Address& to_addr,
    Socket::fd_set* write_fd )
{
    Int32 res = this->m_socket.select_write(write_fd, this->m_timeout);
    if (res == SOCKET_ERROR) {
        throw OPIException(sysError(::WSAGetLastError()));
    }
    if (res && FD_ISSET(this->m_socket.m_socket, write_fd)) {
        res = this->m_socket.sendto(base, size, 0, to_addr);
        if (res == SOCKET_ERROR) {
            throw OPIException(sysError(::WSAGetLastError()));
        }
    }
    return res;
}

void
UDPCom::callback( SendRequest& req, Int32 res )
{
    if (req.m_callback) {
        (*req.m_callback)(req.m_message, res);
        ::delete req.m_callback;
    }
}

Int32
UDPCom::on_send_complete( Metadata::Composite::pointer p_msg, Int32 res )
{
    printf("send completed : ");
    Int64 msgid;
    if (p_msg->type == PACKET_ACK) {
        msgid = *static_cast<Metadata::Element&>(*p_msg).data<AMessage::MSGID>();
        printf("ACK %llx\n", msgid);
        this->deallocate(p_msg, def_ack_size);
    }
    else {
        msgid = static_cast<AMessage::pointer>(p_msg)->header().message_id();
        printf("%s %llx\n",
            static_cast<AMessage::pointer>(p_msg)->item<Metadata::Element>(AMessage::VALUE),
            msgid);
        this->deallocate(p_msg, def_message_size);
    }
    return res;
}
