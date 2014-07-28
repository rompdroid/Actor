////////////////////////////////////////////////////////////////////////////////////////////////
//
// udpcom.h
// Copyright (c) 2012 by Xie Yun .
//
////////////////////////////////////////////////////////////////////////////////////////////////

#ifndef NETCOM_UDPCOM_H
#define NETCOM_UDPCOM_H

#include <native/basic/heap/heap.h>
#include <system/memory/memory.h>
#include <native/actor/perform/perform.h>
#include <netcom/socket/socket.h>

namespace ROMP {
    namespace Net {

struct SendRequest {
    typedef Functor2<Int32, Metadata::Composite::pointer, Int32> Callback;

    union _U {
        Int64 m_expire;
        Socket::Address m_addr;
    };

    Metadata::Composite::pointer m_message;
    Int32 m_state;
    _U m_u;
    Callback* m_callback;
};

class UDPCom
{
public:
    typedef UDPCom Self;
    typedef Self* pointer;
    typedef OPI::LQueue<SendRequest> Queue;

    struct InputWorker
        : public OPI::ActionSingle
    {
        typedef InputWorker Self;
        typedef OPI::ActionSingle Super;

        UDPCom::pointer m_owner;

        InputWorker();

        void construct( UDPCom& );

        virtual Int32 run( ActInstance & ins );
    };

    struct OutputWorker
        : public OPI::ActionSingle
    {
        typedef OutputWorker Self;
        typedef OPI::ActionSingle Super;

        UDPCom::pointer m_owner;

        OutputWorker();

        void construct( UDPCom& );

        virtual Int32 run( ActInstance & ins );

        virtual void stop();
    };

    enum {
        cache_increment = 5,
        max_packet_size = (1<<16),
        def_message_size = 1024,
        def_ack_size = 32,
        def_timeout = 7000,
        try_times = 3,
        try_1 = 0,
        try_2 = 1,
        try_3 = 2,
    };
    enum State {
        state_ok = 0,
        state_fail,
        state_wait_send_1,
        state_wait_ack_1,
        state_wait_send_2,
        state_wait_ack_2,
        state_wait_send_3,
        state_wait_ack_3,
    };
    typedef Memory::Heap<cache_increment> Heap;

public:
// UDPCom.model
    size_type m_size;
    Heap m_cache;
    Socket m_socket;
    UInt32 m_timeout;
    Pentry::pointer m_sender;
    Performer::pointer m_performer;
    Queue m_send_queue[try_times];
    Queue m_wait_queue[try_times];
    InputWorker m_in_worker;
    OutputWorker m_out_worker;

public:
    UDPCom();

    ~UDPCom();

    static pointer new$( size_type cache_size, const UString& sender );

    static void delete$( pointer );

// UDPCom.trait
    void construct( size_type size, const UString& sender );

    void open( Socket::Address& addr, Int32 timeout );

    void close();

    //Int32 out( AMessage& msg );

    Int32 out( AMessage& msg, SendRequest::Callback& callback );

    void attach( Performer& perform );

    template<typename T> T* allocate( size_type );

    void deallocate( void*, size_type );

protected:
    void config_packet( AMessage& );

    void handle_try_1( Socket::fd_set* write_fd );

    void handle_try_2( Socket::fd_set* write_fd );

    void handle_try_3( Socket::fd_set* write_fd );

    void input_worker_run();

    void output_worker_run();

    void parse( AMessage& msg, Socket::Address& from );

    void response_error( Socket::Address& from, AMessageHeader::pointer, UString& what );

    Int32 transfer( AMessage& );

    void recv_ack( Int64 ackid );

    void recv_ack( Queue& Q, Int64 ackid );

    Int32 send_ack( Socket::Address& addr, Int64 msgid );

    Int32 read( void* base, size_type size, Socket::Address& from_addr, Socket::fd_set* read_fd );

    Int32 write( void* base, size_type size, Socket::Address& to_addr, Socket::fd_set* write_fd );

    void callback( SendRequest&, Int32 );

    Int32 on_send_complete( Metadata::Composite::pointer, Int32 );
};

#include "udpcom.inl"
    } // namespace Net
} // namespace ROMP

using namespace ROMP::Net;

#endif //NETCOM_UDPCOM_H
