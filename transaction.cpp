
#include <native/space/space.h>

// Transaction::ShadowEntry
//
inline
bool
Transaction::ShadowEntry::committed() const
{
    return ((this->m_control & commit_mask) != 0);
}

inline
SN
Transaction::ShadowEntry::operation() const
{
    return (this->m_control & operation_mask);
}

inline
SN
Transaction::ShadowEntry::refering() const
{
    return (this->m_control & refer_mask);
}

inline
SN
Transaction::ShadowEntry::acquire_refer()
{
    SN rc = this->refering();
    rc = ((++rc) & refer_mask);
    this->m_control = rc | (this->m_control & ~refer_mask);
    return rc;
}

inline
SN
Transaction::ShadowEntry::release_refer()
{
    SN rc = this->refering();
    if (rc) {
        this->m_control--;
    }
    return rc;
}

inline
void
Transaction::ShadowEntry::set_committed()
{
    this->m_control |= commit_mask;
}

inline
void
Transaction::ShadowEntry::set_operation( SN op )
{
    op &= operation_mask;
    this->m_control = op | (this->m_control & ~operation_mask);
}

inline
void
Transaction::ShadowEntry::unset_committed()
{
    this->m_control &= ~commit_mask;
}

inline
void
Transaction::ShadowEntry::unset_refer()
{
    this->m_control &= ~refer_mask;
}

// Transaction::ShadowIndex
//
inline
Transaction::ShadowIndex::ShadowIndex( Buffer& alloc )
    : m_buffer(alloc),
    m_map(Map::key_compare(), m_buffer)
{
}

inline
Buffer&
Transaction::ShadowIndex::buffer() const
{
    return (*this->m_buffer.get());
}

inline
void
Transaction::ShadowIndex::clear()
{
    this->m_map.clear();
}

inline
Error
Transaction::ShadowIndex::insert( const OID& id, const ShadowEntry& entry )
{
    ShadowEntry::pointer ep =
        static_cast<ShadowEntry::pointer>(this->m_buffer.alloc(entry_size));
    ::copy(entry, *ep);
    Map::value_type val(id, ep);
    std::pair<Map::iterator, bool> insert_res = this->m_map.insert(val);
    if (!insert_res.second) {
        return FAIL;
    }
    return SUCCESS;
}

inline
Transaction::ShadowEntry::pointer
Transaction::ShadowIndex::lookup( const OID& id ) const
{
    Map::const_iterator it = this->m_map.find(id);
    if (it == this->m_map.end()) {
        return 0;
    }
    return it->second;
}

inline
Transaction::ShadowIndex::iterator
Transaction::ShadowIndex::begin()
{
    return (this->m_map.begin());
}

inline
Transaction::ShadowIndex::iterator
Transaction::ShadowIndex::end()
{
    return (this->m_map.end());
}

// Transaction
//
Transaction::~Transaction()
{
    if (this->m_auto_buffer) {
        this->m_auto_buffer = false;
        this->buffer().allocator().deallocate(&this->buffer(), sizeof(Buffer));
    }
}

void
Transaction::construct( RSpace& space, Buffer& buf )
{
    this->m_state = state_commit;
    this->m_space = &space;
    ::new (&this->m_index) ShadowIndex(buf);
    this->m_auto_buffer = false;
}

void
Transaction::construct( RSpace& space, SpaceAllocator& alloc )
{
    this->m_state = state_commit;
    this->m_space = &space;
    Buffer* buf = static_cast<Buffer*>(alloc.allocate(sizeof(Buffer)));
    buf->construct(Config::def_transaction_size, alloc);
    ::new (&this->m_index) ShadowIndex(*buf);
    this->m_auto_buffer = true;
}

RSpace&
Transaction::space() const
{
    return (*this->m_space);
}

Buffer&
Transaction::buffer() const
{
    return (this->m_index.buffer());
}

bool
Transaction::is_active() const
{
    return (this->m_state >= state_active);
}

SBlock
Transaction::allocate( size_type size )
{
    return (this->space().allocate(size));
}

Transaction&
Transaction::begin()
{
    switch (this->m_state) {
        case state_commit:
        case state_abort:
            this->m_index.clear();
            this->m_state = state_active;
            break;
        default:
            this->m_state++;
            break;
    }
    return (*this);
}

Transaction&
Transaction::begin_once()
{
    if (!this->is_active()) {
        return (this->begin());
    }
    return (*this);
}

Error
Transaction::abort()
{
    if (state_abort == this->m_state) {
        return SUCCESS;
    }
    this->m_state = state_abort;
    ShadowIndex::iterator it = this->m_index.begin();
    ShadowIndex::iterator end = this->m_index.end();
    for (; it != end; it++) {
        ShadowEntry::pointer ep = it->second;
        switch (ep->operation()) {
            case op_refer:
                this->abort_refer(ep);
                break;
            case op_new:
                this->abort_new(ep);
                break;
            case op_delete:
                this->abort_delete(ep);
                break;
            case op_update:
                this->abort_update(ep);
                break;
        }
    }
    return SUCCESS;
}

Error
Transaction::commit()
{
    switch (this->m_state) {
        case state_commit:
        case state_abort:
            return Error("transaction completed");
        case state_active:
            this->m_state = state_commit;
            return this->act_commit();
        default:
            this->m_state--;
            return SUCCESS;
    }
}

Error
Transaction::derefer( const OID& x )
{
    if (x == 0) {
        return SUCCESS;
    }
    ShadowEntry::pointer ep = this->m_index.lookup(x);
    if (!ep) {
        return FAIL;
    }
    ep->release_refer();
    if (!ep->refering()) {
        this->commit_refer(ep);
        this->stable_refer(ep);
    }
    return SUCCESS;
}

RelationalObject::pointer
Transaction::refer( const OID& x )
{
    RelationalObject::pointer xp = this->refer_included(x);
    if (!xp) {
        xp = this->m_space->refer(x, true);
        if (xp && !xp->control().in_delete()) {
            this->push(*xp, op_refer);
        }
    }
    return xp;
}

RelationalObject::pointer
Transaction::refer_included( const OID& x )
{
    ShadowEntry::pointer ep = this->m_index.lookup(x);
    if (!ep) {
        return ASNULL(RelationalObject::pointer);
    }
    switch (ep->operation()) {
        case op_delete:
            return ASNULL(RelationalObject::pointer);
        case op_new:
        case op_update:
            break;
        default:
            if (ep->m_image) {
                ep->m_image = this->m_space->refer(x);
            }
            else {
                ep->m_image = this->m_space->refer(x, true);
            }
            ep->set_operation(op_refer);
            ep->acquire_refer();
            break;
    }
    return (ep->m_image);
}

OID&
Transaction::inline$( SN n ) const
{
    return this->m_space->inline$(n);
}

Relationship&
Transaction::R( UInt8 rsn ) const
{
    return (this->m_space->R(rsn));
}

R::Join&
Transaction::Join( SN as1, SN as2 )
{
    return (this->Join(this->R((UInt8)as1).id(), this->R((UInt8)as2).id()));
}

R::Join&
Transaction::Join( const OID& r1, const OID& r2 )
{
    Relationship::SelectResult& select_res =
        this->R(as_join).select(*this, r1, r2, Term::tag_none);
    if (SUCCESS != select_res.second) {
        throw select_res.second;
    }
    if (select_res.first.empty()) {
        XTHROW(ERROR_NOT_FOUND);
    }
    OID& x = select_res.first.front();
    Relement::pointer rp = this->refer<Relement>(x);
    if (!rp) {
        XTHROW(ERROR_NOT_FOUND);
    }
    OID& spec_id = rp->first_of<N>(this->R(as_property).rid());
    if (spec_id == 0) {
        XTHROW("missing spec of R::Join");
    }
    R::Join::pointer jp = this->refer<R::Join>(spec_id);
    if (!jp) {
        XTHROW(ERROR_NOT_FOUND);
    }
    return *jp;
}

Error
Transaction::delete$( OID& x )
{
    if (x == 0) {
        return FAIL;
    }
    RelationalObject::pointer x_shadow = this->make_shadow(x, true, 0);
    if (!x_shadow) {
        return (Error)ERROR_NOT_FOUND;
    }
    if (x.is_type<Relement>()
        || x.is_type<RelementVector>()) {
        return Relement::delete$(*this,
            static_cast<Relement::pointer>(x_shadow));
    }
    else {
        return RelationalObject::delete$(*this, x_shadow);
    }
}

RelationalObject::pointer
Transaction::shadow( const OID& x, Int32 ext )
{
    return this->make_shadow(x, false, ext);
}

void*
Transaction::shadow( void* base, size_type size )
{
    SBlock& block = this->shadow_ext(base, size, 0);
    return block.base;
}

Error
Transaction::push( RelationalObject& o_new, SN op )
{
    ShadowEntry entry = {0L, &o_new, 0LL};
    entry.unset_committed();
    entry.set_operation(op);
    if (op == op_refer) {
        entry.acquire_refer();
    }
    return (this->m_index.insert(o_new.id(), entry));
}

// Transaction protected method
//
inline
bool
Transaction::delete_conflicted( RelationalObject::pointer current,
    RelationalObject::pointer shadow  ) const
{
    return (current->version() != shadow->version()
            || current->control().state() == Control::state_update_commit);
}

inline
bool
Transaction::update_conflicted( RelationalObject::pointer current,
    RelationalObject::pointer shadow  ) const
{
    Control& ctrl = current->control();
    return (current->version() != shadow->version()
            || ctrl.in_delete()
            || ctrl.state() == Control::state_deleted
            || ctrl.state() == Control::state_update_commit);
}

Error
Transaction::abort_refer( ShadowEntry::pointer ep )
{
    if (ep->committed()) {
        return SUCCESS;
    }
    ep->unset_refer();
    RelationalObject::pointer shadow = ep->m_image;
    RelationalObject::pointer current = this->m_space->refer(shadow->id());
    Control& ctrl = shadow->control();
    ctrl.release_refer();
    if (!ctrl.in_refer()
        && (ctrl.state() == Control::state_deleted
            || shadow->version() != current->version())
        ) {
        this->m_space->free_shadowed_image(shadow, current);
    }
    return SUCCESS;
}

Error
Transaction::abort_new( ShadowEntry::pointer ep )
{
    Error res = SUCCESS;
    this->m_space->free_image(ep->m_image);
    if (ep->committed()) {
        res = this->m_space->delete_object(ep->m_image->id());
    }
    return res;
}

Error
Transaction::abort_delete( ShadowEntry::pointer ep )
{
    this->m_space->free_image(ep->m_image);
    RelationalObject::pointer current = this->m_space->refer(ep->m_image->id());
    if (current) {
        Control& ctrl = current->control();
        if (!ep->committed()) {
            ctrl.release_refer();
        }
        else if (ctrl.in_delete()) {
            ctrl.transit(Control::state_stable);
            ctrl.release_delete();
        }
    }
    return SUCCESS;
}

Error
Transaction::abort_update( ShadowEntry::pointer ep )
{
    this->m_space->free_image(ep->m_image);
    RelationalObject::pointer current = this->m_space->refer(ep->m_image->id());
    if (current) {
        Control& ctrl = current->control();
        if (!ep->committed()) {
            ctrl.release_refer();
        }
        else if (ctrl.state() == Control::state_update_commit) {
            ctrl.transit(Control::state_stable);
        }
    }
    return SUCCESS;
}

Error
Transaction::act_commit()
{
    ShadowIndex::iterator it = this->m_index.begin();
    ShadowIndex::iterator end = this->m_index.end();
    for (; it != end; it++) {
        RelationalObject::pointer xp = this->m_space->refer(it->first);
        if (!xp) {
            this->abort();
            return Error("transaction confliction");
        }
        ShadowEntry::pointer ep = it->second;
        RelationalObject::pointer shadow = ep->m_image;
        SN op = ep->operation();
        if ((op_delete == op && this->delete_conflicted(xp, shadow))
            || (op_update == op && this->update_conflicted(xp, shadow))
            ) {
            this->abort();
            return Error("transaction confliction");
        }
    }
    it = this->m_index.begin();
    end = this->m_index.end();
    for (; it != end; it++) {
        ShadowEntry::pointer ep = it->second;
        switch (ep->operation()) {
            case op_refer:
                this->commit_refer(ep);
                break;
            case op_new:
                this->commit_new(ep);
                break;
            case op_delete:
                this->commit_delete(ep);
                break;
            case op_update:
                this->commit_update(ep);
                break;
        }
    }
    it = this->m_index.begin();
    end = this->m_index.end();
    for (; it != end; it++) {
        ShadowEntry::pointer ep = it->second;
        switch (ep->operation()) {
            case op_refer:
                this->stable_refer(ep);
                break;
            case op_new:
                this->stable_new(ep);
                break;
            case op_delete:
                this->stable_delete(ep);
                break;
            case op_update:
                this->stable_update(ep);
                break;
        }
    }
    this->m_space->commit_write();
    return SUCCESS;
}

Error
Transaction::commit_refer( ShadowEntry::pointer ep )
{
    ep->m_image->control().release_refer();
    ep->unset_refer();
    ep->set_committed();
    return SUCCESS;
}

Error
Transaction::commit_new( ShadowEntry::pointer ep )
{
    RelationalObject::pointer shadow = ep->m_image;
    shadow->upgrade();
    if (shadow->is_sliced()) {
        SliceTable::pointer sltb = shadow->slice_table();
        for (SN i = 0, n = sltb->length(); i < n; i++) {
            SliceEntry& item = sltb->items()[i];
            item.m_storage = this->m_space->write(item.m_image, item.m_size);
            item.unset_shadowed();
        }
    }
    ep->m_storage = this->m_space->write(shadow, shadow->size());
    ep->set_committed();
    return SUCCESS;
}

Error
Transaction::commit_delete( ShadowEntry::pointer ep )
{
    RelationalObject::pointer shadow = ep->m_image;
    if (!shadow->version()) {
        shadow->control().acquire_delete();
        ep->set_committed();
        return SUCCESS;
    }
    RelationalObject::pointer current = this->m_space->refer(shadow->id());
    if (current->version() != shadow->version()) {
        this->abort();
        return Error("delete confliction");
    }
    Control& ctrl = current->control();
    switch (ctrl.state()) {
        case Control::state_update_commit:
            this->abort();
            return Error("delete confliction");
        case Control::state_stable:
            ctrl.acquire_delete();
        case Control::state_deleted:
            ctrl.release_refer();
            ep->set_committed();
            break;
    }
    return SUCCESS;
}

Error
Transaction::commit_update( ShadowEntry::pointer ep )
{
    RelationalObject::pointer shadow = ep->m_image;
    RelationalObject::pointer current = this->m_space->refer(shadow->id());
    if (shadow->version()) {
        if (current->version() != shadow->version()) {
            this->abort();
            return Error("update confliction");
        }
        Control& ctrl = current->control();
        if (ctrl.state() != Control::state_stable
            || ctrl.in_delete()
            ) {
            this->abort();
            return Error("update confliction");
        }
        ctrl.transit(Control::state_update_commit);
        ctrl.release_refer();
    }
    shadow->control().reset();
    ep->m_storage = this->write_update(shadow);
    ep->set_committed();
    return SUCCESS;
}

Error
Transaction::stable_refer( ShadowEntry::pointer ep )
{
    RelationalObject::pointer shadow = ep->m_image;
    if (shadow->control().in_refer()) {
        return SUCCESS;
    }
    RelationalObject::pointer current = ASNULL(RelationalObject::pointer);
    switch (shadow->control().state()) {
        case Control::state_deleted:
            current = this->m_space->refer(shadow->id());
            if (shadow == current) {
                this->m_space->free_image(shadow);
            }
            break;
        case Control::state_update_commit:
            current = this->m_space->refer(shadow->id());
            if (shadow != current) {
                this->m_space->free_shadowed_image(shadow, current);
            }
            break;
    }
    return SUCCESS;
}

Error
Transaction::stable_new( ShadowEntry::pointer ep )
{
    this->m_space->update_object(ep->m_image, ep->m_storage);
    ep->m_image->control().transit(Control::state_stable);
    return SUCCESS;
}

Error
Transaction::stable_delete( ShadowEntry::pointer ep )
{
    RelationalObject::pointer shadow = ep->m_image;
    OID& id = shadow->id();
    UInt16 ver = shadow->version();
    this->m_space->free_image(shadow);
    if (ver) {
        RelationalObject::pointer current = this->m_space->refer(id);
        if (current && !Control::state_deleted == current->control().state()) {
            current->control().transit(Control::state_deleted);
            this->m_space->delete_object(id);
            if (!current->control().in_refer()) {
                this->m_space->free_image(current);
            }
        }
    }
    else {
        this->m_space->delete_object(id);
    }
    return SUCCESS;
}

Error
Transaction::stable_update( ShadowEntry::pointer ep )
{
    RelationalObject::pointer shadow = ep->m_image;
    RelationalObject::pointer current = this->m_space->refer(shadow->id());
    this->m_space->update_object(shadow, ep->m_storage);
    if (shadow->version() > 1) {
        if (current && !current->control().in_refer()) {
            this->m_space->free_shadowed_image(current, shadow);
        }
    }
    return SUCCESS;
}

RelationalObject::pointer
Transaction::make_shadow( const OID& x, bool to_delete, Int32 ext )
{
    ShadowEntry::pointer ep = this->m_index.lookup(x);
    RelationalObject::pointer xp = ep->m_image;
    SN op = (to_delete? op_delete : op_update);
    if (ep) {
        if (ext) {
            SBlock& block = this->shadow_ext(xp, xp->size(), ext);
            ep->m_image = static_cast<RelationalObject::pointer>(block.base);
            ep->m_image->resize(block.size);
        }
        else if (op_refer == ep->operation()) {
            ep->m_image = static_cast<RelationalObject::pointer>(
                this->shadow(xp, xp->size()));
        }
        ep->set_operation(op);
    }
    else {
        xp = this->m_space->refer(x, true);
        if (!xp) {
            return xp;
        }
        if (ext) {
            SBlock& block = this->shadow_ext(xp, xp->size(), ext);
            ep->m_image = static_cast<RelationalObject::pointer>(block.base);
            ep->m_image->resize(block.size);
        }
        else {
            ep->m_image = static_cast<RelationalObject::pointer>(
                this->shadow(xp, xp->size()));
        }
        this->push(*ep->m_image, op);
    }
    return ep->m_image;
}

SBlock
Transaction::shadow_ext( void* base, size_type size, Int32 ext )
{
    size_type sz = size + ext;
    SBlock& block = this->allocate(sz);
    sz = min(size, sz);
    ::memcpy(block.base, base, sz);
    ::memset((void*)((MemoryAddress)(block.base) + sz), 0, block.size - sz);
    return block;
}

SpaceAddress
Transaction::write_update( RelationalObject::pointer shadow )
{
    shadow->upgrade();
    if (shadow->is_sliced()) {
        SliceTable::pointer sltb = shadow->slice_table();
        for (SN i = 0, n = sltb->length(); i < n; i++) {
            SliceEntry& item = sltb->items()[i];
            if (item.shadowed()) {
                item.m_storage =
                    this->m_space->write(item.m_image, item.m_size);
                item.unset_shadowed();
            }
        }
    }
    return (this->m_space->write(shadow, shadow->size()));
}
