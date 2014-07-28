////////////////////////////////////////////////////////////////////////////////////////////////
//
// transaction.h
// Copyright (c) 2012 by Xie Yun .
//
////////////////////////////////////////////////////////////////////////////////////////////////

#ifndef NATIVE_TRANSACTION_H
#define NATIVE_TRANSACTION_H

#include <list>
#include <map>
#include <native/model/allocator/buffer_alloc.h>

namespace ROMP {
    namespace Native {

class Transaction
{
public:
    struct ShadowEntry;
    class ShadowIndex;
    typedef Transaction Self;
    typedef Self* pointer;
    typedef RSpace* space_pointer;
    typedef std::list<SBlock, BufferAllocator<SBlock> > BlockList;

    struct ShadowEntry
    {
        typedef ShadowEntry* pointer;
        enum {
            commit_mask = 0x80000000,
            operation_mask = 0x7F000000,
            refer_mask = 0x00FFFFFF,
        };
    public:
        UInt32 m_control;
        RelationalObject* m_image;
        SpaceAddress m_storage;

    public:
        bool committed() const;

        SN operation() const;

        SN refering() const;

        SN acquire_refer();

        SN release_refer();

        void set_committed();

        void set_operation( SN );

        void unset_committed();

        void unset_refer();
    };

    class ShadowIndex
    {
    public:
        typedef ShadowIndex Self;
        typedef BufferAllocator<std::pair<const OID, ShadowEntry::pointer> >
            InBuffer;
        typedef std::map<OID, ShadowEntry::pointer, std::less<OID>, InBuffer>
            Map;
        typedef Map::iterator iterator;
        enum {
            entry_size = sizeof(ShadowEntry),
        };
    private:
        InBuffer m_buffer;
        Map m_map;

    public:
        ShadowIndex( Buffer& );

        Buffer& buffer() const;

        void clear();

        Int32 insert( const OID& id, const ShadowEntry& entry );

        ShadowEntry::pointer lookup( const OID& id ) const;

        iterator begin();

        iterator end();
    };

    enum State {
        state_commit = 0,
        state_abort = 1,
        state_active = 2,
    };

    enum Operation {
        op_refer = (1 << 24),
        op_new = (2 << 24),
        op_delete = (3 << 24),
        op_update = (4 << 24),
    };

private:
// Transaction.model
    UInt32 m_state;
    space_pointer m_space;
    bool m_auto_buffer;
    ShadowIndex m_index;

public:
    ~Transaction();

    template<class Alloc> static pointer new$( RSpace&, Alloc& );

    template<class Alloc> static void delete$( pointer, Alloc& );

// Transaction.trait
    void construct( RSpace&, Buffer& );

    void construct( RSpace&, SpaceAllocator& );

    RSpace& space() const;

    Buffer& buffer() const;

    bool is_active() const;

    Error abort();

    Self& begin();

    Self& begin_once();

    Error commit();

    Error derefer( const OID& );

    RelationalObject* refer( const OID& );

    template<typename Ty> typename Ty::pointer refer( const OID& );

    RelationalObject* refer_included( const OID& );

    template<typename Ty> typename Ty::pointer refer_included( const OID& );

    //RelationalObject& refer_inline( SN );
    OID& inline$( SN ) const;

    Relationship& R( UInt8 rsn ) const;

    R::Join& Join( SN as1, SN as2 );

    R::Join& Join( const OID& r1, const OID& r2 );

    template<class Ty> typename Ty::pointer new$();

    template<class Ty, class A> typename Ty::pointer new$( A& );

    template<class Ty, class A1, class A2> typename Ty::pointer new$( A1&, A2& );

    Error delete$( OID& );

    SBlock allocate( size_type );

    RelationalObject* shadow( const OID&, Int32 ext = 0 );

    template<typename Ty>
        typename Ty::pointer shadow( const OID&, Int32 ext = 0 );

    void* shadow( void* base, size_type size );

    Error push( RelationalObject& o_new, SN op );

protected:
    Transaction();

    Transaction( const Self& );

    bool delete_conflicted( RelationalObject* current,
        RelationalObject* shadow ) const;

    bool update_conflicted( RelationalObject* current,
        RelationalObject* shadow ) const;

    Error abort_refer( ShadowEntry::pointer );

    Error abort_new( ShadowEntry::pointer );

    Error abort_delete( ShadowEntry::pointer );

    Error abort_update( ShadowEntry::pointer );

    Error act_commit();

    Error commit_refer( ShadowEntry::pointer );

    Error commit_new( ShadowEntry::pointer );

    Error commit_delete( ShadowEntry::pointer );

    Error commit_update( ShadowEntry::pointer );

    Error stable_refer( ShadowEntry::pointer );

    Error stable_new( ShadowEntry::pointer );

    Error stable_delete( ShadowEntry::pointer );

    Error stable_update( ShadowEntry::pointer );

    RelationalObject* make_shadow( const OID&, bool to_delete, Int32 ext );

    SBlock shadow_ext( void* base, size_type size, Int32 ext );

    SpaceAddress write_update( RelationalObject* );
};

    } // namespace Native
} // namespace ROMP

#endif //NATIVE_TRANSACTION_H
