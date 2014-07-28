
#ifndef COMMON_METADATA_H
#define COMMON_METADATA_H

#include "opitypes.h"
using namespace OPI::Enum;

namespace Common {
    namespace Metadata {

    enum {
        max_size = 0xFFFF0000L,
    };

    // Metadata::Element
    ///////////////////////////////////////////////////////////////////////////////
    struct ElementHead
    {
        UInt32 type;
        size_type size;
    };

    class Element
        : public ElementHead
    {
    public:
        typedef Element Self;
        typedef Element* pointer;

        enum {
            type$ = 0x0045444DL,
            size$ = sizeof(ElementHead),
        };

    public:
        Element( size_type );

        template<class Al> static pointer new$( size_type, Al& );

        static size_type size_of( size_type ) throw();

    // Element.trait
        void construct( size_type ) throw();

        template<typename Ty> Self& assign( const Ty& rhs ) throw();

        Self& assign( const void* base, size_type size ) throw();

        Self& copy( Self& ) throw();

        size_type block_size() const throw();

        void* data() const throw();

        template<typename Ty> Ty* data() const throw();

        std::string to_string() const throw();

        std::wstring to_wstring() const throw();

        template<typename Ty> bool is_mdtype() const throw();
    };

    // Metadata::Composite
    ///////////////////////////////////////////////////////////////////////////////
    class Composite
        : public Element
    {
    public:
        typedef Composite Self;
        typedef Element Super;
        typedef Self* pointer;

        class Iterator;

        enum {
            type$ = 0x0043444DL,
            size$ = Super::size$ + 4,
        };

    public:
        size_type count;

    public:
        Composite( size_type );

        template<class Al> static pointer new$( size_type, Al& );

        static size_type size_of( size_type ) throw();

    // Composite.trait
        void construct( size_type ) throw();

        Self& append();

        Self& append( const void* base, size_type size );

        template<typename Ty> Self& append( const Ty& item );

        template<typename Ty> Self&
            append_named_item( const std::string& name, const Ty& item );

        template<typename Ty> Self&
            append_named_item( const std::wstring& name, const Ty& item );

        pointer new_append();

        pointer lookup_named_item( const std::string& name ) const;

        pointer lookup_named_item( const std::wstring& name ) const;

        template<typename Ty> Self& update( UInt32, const Ty& item );

        Self& update( UInt32 i, const void* base, size_type size );

        Iterator begin() const throw();

        Iterator end() const throw();

        void* data() const throw();

        void* bottom() const throw();

        template<typename Ty> typename Ty::pointer item( UInt32 ) const throw();

        bool is_valid() const throw();

        void inc( size_type );

    private:
        using Super::assign;
    };

    class Composite::Iterator
    {
    public:
        typedef Composite::Iterator Self;
        typedef Element::pointer pointer;

    protected:
        pointer m_ptr;

    public:
        Iterator( void* rhs = 0 );

        Iterator( const Self& rhs );

        pointer get() const throw();

        pointer operator ->() const throw();

        bool operator ==( const void* rhs ) const throw();

        bool operator ==( const Self& rhs ) const throw();

        bool operator !=( const void* rhs ) const throw();

        bool operator !=( const Self& rhs ) const throw();

        Self& next() throw();
    };

#include "metadata.hpp"
    } // namespace Metadata
} // namespace Common

#endif //COMMON_METADATA_H
