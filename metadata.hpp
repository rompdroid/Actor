////////////////////////////////////////////////////////////////////////////////////////////////
//
// metadata.hpp
// Copyright (c) 2004, 2012 by Xie Yun .
//
////////////////////////////////////////////////////////////////////////////////////////////////

#ifdef COMMON_METADATA_H

// Metadata::Element
///////////////////////////////////////////////////////////////////////////////
inline
Element::Element( size_type size )
{
    this->type = Self::type$;
    this->size = size;
}

template<class Al> inline
Element::pointer
Element::new$( size_type size, Al& alloc )
{
    pointer instance = alloc.allocate<Self>(size);
    instance->construct(size - Self::size$);
    return instance;
}

inline
size_type
Element::size_of( size_type size )
{
    return (Self::size$ + size);
}

inline
void
Element::construct( size_type size )
{
    ::new (this) Self(size);
}

inline
Element&
Element::assign( const void* base, size_type size )
{
    if (base && size) {
        if (size > this->size) {
            size = this->size;
        }
        ::memcpy(this->data(), base, size);
        this->size = size;
    }
    return *this;
}

template<typename Ty> inline
Element&
Element::assign( const Ty& rhs )
{
    return this->assign(&rhs, sizeof(rhs));
}

template<> inline
Element&
Element::assign<Element>( const Self& rhs )
{
    return this->assign(rhs.data(), rhs.size);
}

template<> inline
Element&
Element::assign<std::string>( const std::string& rhs )
{
    return this->assign(rhs.c_str(), rhs.size());
}

template<> inline
Element&
Element::assign<std::wstring>( const std::wstring& rhs )
{
    return this->assign(rhs.c_str(), sizeof(wchar_t) * rhs.size());
}

inline
Element&
Element::copy( Self& rhs )
{
    ::memcpy(&rhs, this, this->block_size());
    return *this;
}

inline
size_type
Element::block_size() const
{
    switch (this->type) {
        case Self::type$:
            return Self::size_of(this->size);
        case Composite::type$:
            return Composite::size_of(this->size);
        default:
            return 0;
    }
}

inline
void*
Element::data() const
{
    return (const_cast<pointer>(this) + 1);
}

template<typename Ty> inline
Ty*
Element::data() const
{
    return static_cast<Ty*>(this->data());
}

inline
std::string
Element::to_string() const
{
    return std::string(this->data<char>(), this->size);
}

inline
std::wstring
Element::to_wstring() const
{
    return std::wstring(this->data<wchar_t>(), this->size / sizeof(wchar_t));
}

template<typename Ty> inline
bool
Element::is_mdtype() const
{
    return (this->type == Ty::type$);
}

// Metadata::Composite
///////////////////////////////////////////////////////////////////////////////
inline
Composite::Composite( size_type size )
    : Super(size)
{
    this->type = Self::type$;
    this->count = 0;
}

template<class Al> inline
Composite::pointer
Composite::new$( size_type size, Al& alloc )
{
    pointer instance = alloc.allocate<Self>(size);
    instance->construct(size - Self::size$);
    return instance;
}

inline
size_type
Composite::size_of( size_type size )
{
    return (Self::size$ + size);
}

inline
void
Composite::construct( size_type size )
{
    ::new (this) Self(size);
}

template<typename Ty> inline
Composite&
Composite::append( const Ty& item )
{
    const pointer item_ptr = (const pointer)&item;
    if (item_ptr->is_mdtype<Element>() || item_ptr->is_mdtype<Self>()) {
        return this->append<Element>(item);
    }
    else {
        return this->append(&item, sizeof(item));
    }
}

template<> inline
Composite&
Composite::append<Int64>( const Int64& item )
{
    return this->append(&item, sizeof(item));
}

template<> inline
Composite&
Composite::append<std::string>( const std::string& item )
{
    if (item.size()) {
        return this->append(item.c_str(), item.size());
    }
    else {
        return this->append(0, 0);
    }
}

template<> inline
Composite&
Composite::append<std::wstring>( const std::wstring& item )
{
    if (item.size()) {
        return this->append(item.c_str(), sizeof(wchar_t) * item.size());
    }
    else {
        return this->append(0, 0);
    }
}

template<> inline
Composite&
Composite::append<Element>( const Element& item )
{
    size_type size = item.block_size();
    if (!size) {
        throw "invalid parameter";
    }
    if (this->block_size() + size >= max_size) {
        throw "overflow";
    }
    if (this->bottom() != &item) {
        ::memcpy(this->bottom(), &item, size);
    }
    this->size += size;
    ++this->count;
    return *this;
}

template<> inline
Composite&
Composite::append<Composite>( const Composite& item )
{
    return this->append<Element>(item);
}

inline
Composite&
Composite::append( const void* base, size_type size )
{
    if (this->block_size() + Element::size_of(size) >= max_size) {
        throw "overflow";
    }
    Element::pointer item_ptr = static_cast<Element::pointer>(this->bottom());
    item_ptr->construct(size);
    if (base && size) {
        ::memcpy(item_ptr->data(), base, size);
    }
    this->size += item_ptr->block_size();
    ++this->count;
    return *this;
}

inline
Composite&
Composite::append()
{
    if (this->block_size() + size$ >= max_size) {
        throw "overflow";
    }
    pointer item_ptr = static_cast<pointer>(this->bottom());
    item_ptr->construct(0);
    this->size += item_ptr->block_size();
    ++this->count;
    return *this;
}

template<typename Ty> inline
Composite&
Composite::append_named_item( const std::string& name, const Ty& item )
{
    pointer item_ptr = this->new_append();
    item_ptr->append(name);
    item_ptr->append(item);
    this->append(*item_ptr);
    return *this;
}

template<> inline
Composite&
Composite::append_named_item<SBlock>( const std::string& name,
    const SBlock& item )
{
    pointer item_ptr = this->new_append();
    item_ptr->append(name);
    item_ptr->append(item.base, item.size);
    this->append(*item_ptr);
    return *this;
}

template<typename Ty> inline
Composite&
Composite::append_named_item( const std::wstring& name, const Ty& item )
{
    pointer item_ptr = this->new_append();
    item_ptr->append(name);
    item_ptr->append(item);
    this->append(*item_ptr);
    return *this;
}

template<> inline
Composite&
Composite::append_named_item<SBlock>( const std::wstring& name,
    const SBlock& item )
{
    pointer item_ptr = this->new_append();
    item_ptr->append(name);
    item_ptr->append(item.base, item.size);
    this->append(*item_ptr);
    return *this;
}

inline
Composite::pointer
Composite::new_append()
{
    pointer p_new = static_cast<pointer>(this->bottom());
    p_new->construct(0);
    return p_new;
}

inline
Composite::pointer
Composite::lookup_named_item( const std::string& name ) const
{
    if (this->count) {
        Iterator it = this->begin();
        Iterator end = this->end();
        for (; it != end; it.next()) {
            pointer ep = static_cast<pointer>(it.get());
            Element::pointer dp = ep->item<Element>(0);
            if (name == dp->to_string()) {
                return ep;
            }
        }
    }
    return 0;
}

inline
Composite::pointer
Composite::lookup_named_item( const std::wstring& name ) const
{
    if (this->count) {
        Iterator it = this->begin();
        Iterator end = this->end();
        for (; it != end; it.next()) {
            pointer ep = static_cast<pointer>(it.get());
            Element::pointer dp = ep->item<Element>(0);
            if (name == dp->to_wstring()) {
                return ep;
            }
        }
    }
    return 0;
}

template<typename Ty> inline
Composite&
Composite::update( UInt32 i, const Ty& item )
{
    const pointer item_ptr = (const pointer)&item;
    if (item_ptr->is_mdtype<Element>() || item_ptr->is_mdtype<Self>()) {
        return this->update<Element>(i, item);
    }
    else {
        return this->update(i, &item, sizeof(item));
    }
}

template<> inline
Composite&
Composite::update<Int64>( UInt32 i, const Int64& item )
{
    return this->update(i, &item, sizeof(item));
}

template<> inline
Composite&
Composite::update<std::string>( UInt32 i, const std::string& item )
{
    return this->update(i, item.c_str(), item.size());
}

template<> inline
Composite&
Composite::update<std::wstring>( UInt32 i, const std::wstring& item )
{
    return this->update(i, item.c_str(), sizeof(wchar_t) * item.size());
}

template<> inline
Composite&
Composite::update<Element>( UInt32 i, const Element& item )
{
    if (i >= this->count) {
        throw "out of range";
    }
    Iterator it = this->item<Element>(i);
    size_type size = item.block_size();
    Int32 delta = size - it->block_size();
    if (this->block_size() + delta >= max_size) {
        throw "overflow";
    }
    if (delta && (i < this->count - 1)) {
        void* pos_1 = Iterator(it).next().get();
        void* pos_2 = (void*)((UInt32)pos_1 + delta);
        size_type size_1 = (UInt32)this->bottom() - (UInt32)pos_1;
        ::memmove(pos_2, pos_1, size_1);
    }
    ::memcpy(it.get(), &item, size);
    this->size += delta;
    return *this;
}

template<> inline
Composite&
Composite::update<Composite>( UInt32 i, const Composite& item )
{
    return this->update<Element>(i, item);
}

inline
Composite&
Composite::update( UInt32 i, const void* base, size_type size )
{
    if (i >= this->count) {
        throw "out of range";
    }
    Iterator it = this->item<Element>(i);
    Int32 delta = Element::size_of(size) - it->block_size();
    if (this->block_size() + delta >= max_size) {
        throw "overflow";
    }
    if (delta && (i < this->count - 1)) {
        void* pos_1 = Iterator(it).next().get();
        void* pos_2 = (void*)((UInt32)pos_1 + delta);
        size_type size_1 = (UInt32)this->bottom() - (UInt32)pos_1;
        ::memmove(pos_2, pos_1, size_1);
    }
    it->construct(size);
    ::memcpy(it->data(), base, size);
    this->size += delta;
    return *this;
}

inline
Composite::Iterator
Composite::begin() const
{
    return Self::Iterator(this->data());
}

inline
Composite::Iterator
Composite::end() const
{
    return Self::Iterator(this->bottom());
}

inline
void*
Composite::data() const
{
    return (const_cast<pointer>(this) + 1);
}

inline
void*
Composite::bottom() const
{
    return (void*)((UInt32)this + Self::size$ + this->size);
}

template<typename Ty> inline
typename Ty::pointer
Composite::item( UInt32 i ) const
{
    if (i >= this->count) {
        return 0;
    }
	Iterator it = this->begin();
	for (; i && it.get(); i--) {
        it.next();
    }
    return static_cast<Ty::pointer>(it.get());
}

inline
bool
Composite::is_valid() const
{
    size_type size = this->block_size();
    if (!size || size > max_size) {
        return false;
    }
    size = this->size;
    Iterator end = this->end();
    for(Iterator it = this->begin(); it != end; it.next()) {
        size_type item_size = it->block_size();
        if (!item_size || item_size > size) {
            return false;
        }
        size -= item_size;
        if (it->is_mdtype<Composite>()
            && !static_cast<pointer>(it.get())->is_valid()) {
            return false;
        }
    }
    return (!size);
}

inline
void
Composite::inc( size_type size )
{
    this->size += size;
    this->count++;
}

// Metadata::Composite::Iterator
///////////////////////////////////////////////////////////////////////////////
inline
Composite::Iterator::Iterator( void* rhs )
    : m_ptr(static_cast<pointer>(rhs))
{
}

inline
Composite::Iterator::Iterator( const Self& rhs )
    : m_ptr(rhs.m_ptr)
{
}

inline
Composite::Iterator::pointer
Composite::Iterator::get() const
{
    return this->m_ptr;
}

inline
Composite::Iterator::pointer
Composite::Iterator::operator ->() const
{
    return this->get();
}

inline
bool
Composite::Iterator::operator ==( const void* rhs ) const
{
    return (this->get() == rhs);
}

inline
bool
Composite::Iterator::operator ==( const Self& rhs ) const
{
    return (this->get() == rhs.get());
}

inline
bool
Composite::Iterator::operator !=( const void* rhs ) const
{
    return !(*this == rhs);
}

inline
bool
Composite::Iterator::operator !=( const Self& rhs ) const
{
    return !(*this == rhs);
}

inline
Composite::Iterator&
Composite::Iterator::next()
{
    if (this->m_ptr) {
        size_type size = this->m_ptr->block_size();
        if (size) {
            this->m_ptr = (pointer)((UInt32)(this->m_ptr) + size);
        }
        else {
            this->m_ptr = 0;
        }
    }
    return *this;
}

#endif
