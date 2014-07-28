
#ifndef OPI_H_AMESSAGE
#define OPI_H_AMESSAGE

#include "pentry.h"

namespace OPI {

    // AMessageOptions
    ////////////////////////////////////////////////////////////////////////////////////////////////
    class AMessageOptions
        : public Metadata::Composite
    {
    public:
        typedef AMessageOptions Self;
        typedef Metadata::Composite Super;
        typedef Self* pointer;
	    enum {
		    ACKNOWLEDGE = 0,
		    RELAY = 1,
		    EXPIRE = 2,
            fields = 3,
	    };

    public:
	    bool acknowledge() const;
	    Int64 expire() const;
	    bool relay() const;
        Self& assign( bool ack, Int64 expire, bool relay );
        bool is_valid() const throw();
    };

    // AMessageHeader
    ////////////////////////////////////////////////////////////////////////////////////////////////
    class AMessageHeader
        : public Metadata::Composite
    {
    public:
        typedef AMessageHeader Self;
        typedef Metadata::Composite Super;
        typedef Self* pointer;
        typedef Int64 MSGID;
	    enum {
		    MESSAGE_ID = 0,
		    CORRELATION_ID = 1,
		    RECEIVER = 2,
		    SENDER = 3,
		    SEND_TIME = 4,
		    REPLY_TO = 5,
		    OPTIONS = 6,
            fields = 7,
	    };

    public:
        static size_type size_of( Pentry::pointer receiver,
            Pentry::pointer sender,
            Pentry::pointer replyto,
            AMessageOptions::pointer options );

	    MSGID message_id() const;
        MSGID correlation_id() const;
	    Pentry& receiver() const;
	    Pentry& sender() const;
	    Int64 send_time() const;
	    Composite& reply_to() const;
	    AMessageOptions& options() const;
        Self& assign( MSGID msgid,
            MSGID coid,
            Pentry::pointer receiver,
            Pentry::pointer sender,
            Int64 sendtime,
            Pentry::pointer replyto = 0,
            AMessageOptions::pointer options = 0 );
        bool is_valid() const throw();
    };

    // AMessageMethod
    ////////////////////////////////////////////////////////////////////////////////////////////////
    class AMessageMethod
        : public Metadata::Composite
    {
    public:
        typedef AMessageMethod Self;
        typedef Metadata::Composite Super;
        typedef Self* pointer;
	    enum {
		    NAME = 0,
		    CONTENT = 1,
            fields = 2,
	    };

    public:
        static size_type size_of( const std::string& method, UInt32 content_size );
        std::string name() const;
	    void* content() const;
        Self& assign( const std::string& name, const void* content, size_type content_size );
        bool is_valid() const throw();
    };

    // AMessage
    ////////////////////////////////////////////////////////////////////////////////////////////////
    class AMessage
        : public Metadata::Composite
    {
    public:
        typedef AMessage Self;
        typedef Metadata::Composite Super;
        typedef Self* pointer;
        typedef AMessageHeader::MSGID MSGID;
	    enum {
		    VERSION = 0,
		    VALUE = 1,
		    HEADER = 2,
		    METHOD = 3,
            fields = 4,
	    };

    public:
        template<class Al> static pointer new$( Al&,
            UInt32 msgval,
            MSGID msgid,
            MSGID coid,
            Pentry::pointer receiver,
            Pentry::pointer sender,
            Int64 sendtime,
            Pentry::pointer replyto,
            AMessageOptions::pointer options,
            const std::string& method,
            const void* content,
            UInt32 content_size );
        
        static size_type size_of( Pentry::pointer receiver,
            Pentry::pointer sender,
            Pentry::pointer replyto,
            AMessageOptions::pointer options,
            const std::string& method,
            UInt32 content_size );

	    UInt32 version() const;

        UInt32 message_value() const;

        AMessageHeader& header() const;

        AMessageMethod& method() const;

        Self& assign( UInt32 msgval,
            const AMessageHeader& header,
            const AMessageMethod& method );

        Self& assign( UInt32 msgval,
            MSGID msgid,
            MSGID coid,
            Pentry::pointer receiver,
            Pentry::pointer sender,
            Int64 sendtime,
            Pentry::pointer replyto,
            AMessageOptions::pointer options,
            const std::string& method,
            const void* content,
            UInt32 content_size );

        bool is_valid() const throw();
    };

#include "amessage.hpp"
} // namespace OPI

#endif //OPI_H_AMESSAGE
