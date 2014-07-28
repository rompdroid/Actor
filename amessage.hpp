
#ifdef OPI_H_AMESSAGE

// AMessageOptions
////////////////////////////////////////////////////////////////////////////////////////////////
inline
bool
AMessageOptions::acknowledge() const
{
    Element::pointer item_ptr = this->item<Element>(ACKNOWLEDGE);
    if (!item_ptr) {
        throw "malformed";
    }
    return *item_ptr->data<bool>();
}

inline
Int64
AMessageOptions::expire() const
{
    Element::pointer item_ptr = this->item<Element>(EXPIRE);
    if (!item_ptr) {
        throw "malformed";
    }
    return *item_ptr->data<Int64>();
}

inline
bool
AMessageOptions::relay() const
{
    Element::pointer item_ptr = this->item<Element>(RELAY);
    if (!item_ptr) {
        throw "malformed";
    }
    return *item_ptr->data<bool>();
}

inline
AMessageOptions&
AMessageOptions::assign( bool ack, Int64 expire, bool relay )
{
    this->size = 0;
    this->count = 0;
    this->append((UInt32)ack);
    this->append(expire);
    this->append((UInt32)relay);
    return *this;
}

inline
bool
AMessageOptions::is_valid() const
{
    if (this->count != Self::fields) {
        return false;
    }
    size_type size = this->size;
    // acknowledge
    Iterator it = this->begin();
    if (!it->is_mdtype<Element>() || it->size != sizeof(UInt32)) {
        return false;
    }
    size -= it->block_size();
    // expire
    it.next();
    if (!it->is_mdtype<Element>() || it->size != sizeof(Int64)) {
        return false;
    }
    size -= it->block_size();
    // relay
    if (!it->is_mdtype<Element>() || it->size != sizeof(UInt32)) {
        return false;
    }
    size -= it->block_size();
    return (!size);
}

// AMessageHeader
////////////////////////////////////////////////////////////////////////////////////////////////
inline
size_type
AMessageHeader::size_of( Pentry::pointer receiver,
    Pentry::pointer sender,
    Pentry::pointer replyto,
    AMessageOptions::pointer options )
{
    size_type size = Element::size_of(sizeof(MSGID))
        + Element::size_of(sizeof(MSGID))
        + Element::size_of(sizeof(Int64))
        + receiver->block_size();
    if (sender) {
        size += sender->block_size();
    }
    else {
        size += Pentry::size_of("");
    }
    if (replyto) {
        size += replyto->block_size();
    }
    else {
        size += Composite::size_of(0);
    }
    if (options) {
        size += options->block_size();
    }
    else {
        size += Composite::size_of(0);
    }
    return Super::size_of(size);
}

inline
AMessageHeader::MSGID
AMessageHeader::message_id() const
{
    Element::pointer item_ptr = this->item<Element>(MESSAGE_ID);
    if (!item_ptr) {
        throw "malformed";
    }
    return *item_ptr->data<MSGID>();
}

inline
AMessageHeader::MSGID
AMessageHeader::correlation_id() const
{
    Element::pointer item_ptr = this->item<Element>(CORRELATION_ID);
    if (!item_ptr) {
        throw "malformed";
    }
    return *item_ptr->data<MSGID>();
}

inline
Pentry&
AMessageHeader::receiver() const
{
    Pentry::pointer item_ptr = this->item<Pentry>(RECEIVER);
    if (!item_ptr) {
        throw "malformed";
    }
    return *item_ptr;
}

inline
Pentry&
AMessageHeader::sender() const
{
    Pentry::pointer item_ptr = this->item<Pentry>(SENDER);
    if (!item_ptr) {
        throw "malformed";
    }
    return *item_ptr;
}

inline
Int64
AMessageHeader::send_time() const
{
    Element::pointer item_ptr = this->item<Element>(SEND_TIME);
    if (!item_ptr) {
        throw "malformed";
    }
    return *item_ptr->data<Int64>();
}

inline
Metadata::Composite&
AMessageHeader::reply_to() const
{
    Composite::pointer item_ptr = this->item<Composite>(REPLY_TO);
    if (!item_ptr) {
        throw "malformed";
    }
    return *item_ptr;
}

inline
AMessageOptions&
AMessageHeader::options() const
{
    AMessageOptions::pointer item_ptr = this->item<AMessageOptions>(OPTIONS);
    if (!item_ptr) {
        throw "malformed";
    }
    return *item_ptr;
}

inline
AMessageHeader&
AMessageHeader::assign( MSGID msgid,
    MSGID coid,
    Pentry::pointer receiver,
    Pentry::pointer sender,
    Int64 sendtime,
    Pentry::pointer replyto,
    AMessageOptions::pointer options )
{
    this->size = 0;
    this->count = 0;
    this->append(msgid);
    this->append(coid);
    this->append(*receiver);
    if (sender) {
        this->append(*sender);
    }
    else {
        Pentry::pointer pent = static_cast<Pentry::pointer>(this->bottom());
        pent->construct(0);
        pent->assign("");
        this->append(*pent);
    }
    this->append(sendtime);
    if (replyto) {
        this->append(*replyto);
    }
    else {
        this->append();
    }
    if (options) {
        this->append(*options);
    }
    else {
        this->append();
    }
    return *this;
}

inline
bool
AMessageHeader::is_valid() const
{
    if (this->count != Self::fields) {
        return false;
    }
    size_type size = this->size;
    // message_id
    Iterator it = this->begin();
    if (!it->is_mdtype<Element>() || it->size != sizeof(MSGID)) {
        return false;
    }
    size -= it->block_size();
    // correlation_id
    it.next();
    if (!it->is_mdtype<Element>() || it->size != sizeof(MSGID)) {
        return false;
    }
    size -= it->block_size();
    // receiver
    it.next();
    if (!static_cast<Pentry::pointer>(it.get())->is_valid()) {
        return false;
    }
    size -= it->block_size();
    // sender
    it.next();
    if (!static_cast<Pentry::pointer>(it.get())->is_valid()) {
        return false;
    }
    size -= it->block_size();
    // send_time
    it.next();
    if (!it->is_mdtype<Element>() || it->size != sizeof(Int64)) {
        return false;
    }
    size -= it->block_size();
    // reply_to
    it.next();
    if (!it->is_mdtype<Composite>()) {
        return false;
    }
    if (it->size && !static_cast<Pentry::pointer>(it.get())->is_valid()) {
        return false;
    }
    size -= it->block_size();
    // options
    it.next();
    if (!it->is_mdtype<Composite>()) {
        return false;
    }
    if (it->size && !static_cast<Composite::pointer>(it.get())->is_valid()) {
        return false;
    }
    size -= it->block_size();
    return (!size);
}

// AMessageMethod
////////////////////////////////////////////////////////////////////////////////////////////////
inline
size_type
AMessageMethod::size_of( const std::string& method, UInt32 content_size )
{
    size_type size = Element::size_of(method.size()) + content_size;
    return Super::size_of(size);
}

inline
std::string
AMessageMethod::name() const
{
    Element::pointer item_ptr = this->item<Element>(NAME);
    if (!item_ptr) {
        throw "malformed";
    }
    return std::string(item_ptr->data<char>(), item_ptr->size);
}

inline
void*
AMessageMethod::content() const
{
    return this->item<Element>(CONTENT);
}

inline
AMessageMethod&
AMessageMethod::assign( const std::string& name, const void* content, size_type content_size )
{
    this->size = 0;
    this->count = 0;
    this->append(name);
    if (content) {
        const Element::pointer item_ptr = (const Element::pointer)(content);
        if (item_ptr->is_mdtype<Element>() || item_ptr->is_mdtype<Composite>()) {
            this->append(*item_ptr);
        }
        else {
            this->append(content, content_size);
        }
    }
    else {
        this->append(0, 0);
    }
    return *this;
}

inline
bool
AMessageMethod::is_valid() const
{
    return (this->count >= fields && Super::is_valid());
}

// AMessage
////////////////////////////////////////////////////////////////////////////////////////////////
template<class Al> inline
AMessage::pointer
AMessage::new$( Al& alloc,
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
    UInt32 content_size )
{
    size_type size = Self::size_of(receiver, sender, replyto, options, method, content_size);
    pointer instance = static_cast<pointer>(Super::new$(size, alloc));
    instance->assign(msgval,
        msgid,
        coid,
        receiver,
        sender,
        sendtime,
        replyto,
        options,
        method,
        content,
        content_size);
    return instance;
}

inline
size_type
AMessage::size_of( Pentry::pointer receiver,
    Pentry::pointer sender,
    Pentry::pointer replyto,
    AMessageOptions::pointer options,
    const std::string& method,
    UInt32 content_size )
{
    size_type size = Element::size_of(sizeof(UInt32))
        + Element::size_of(sizeof(Enum::AMSGValue))
        + AMessageHeader::size_of(receiver, sender, replyto, options)
        + AMessageMethod::size_of(method, content_size);
    return Super::size_of(size);
}

inline
UInt32
AMessage::version() const
{
    Element::pointer item_ptr = this->item<Element>(VERSION);
    if (!item_ptr) {
        throw "malformed";
    }
    return *item_ptr->data<UInt32>();
}

inline
UInt32
AMessage::message_value() const
{
    Element::pointer item_ptr = this->item<Element>(VALUE);
    if (!item_ptr) {
        throw "malformed";
    }
    return *item_ptr->data<UInt32>();
}

inline
AMessageHeader&
AMessage::header() const
{
    return *this->item<AMessageHeader>(HEADER);
}

inline
AMessageMethod&
AMessage::method() const
{
    return *this->item<AMessageMethod>(METHOD);
}

inline
AMessage&
AMessage::assign( UInt32 msgval, const AMessageHeader& header, const AMessageMethod& method )
{
    this->size = 0;
    this->count = 0;
    this->append(0); //version
    this->append(msgval);
    this->append(header);
    this->append(method);
    return *this;
}

inline
AMessage&
AMessage::assign( UInt32 msgval,
    MSGID msgid,
    MSGID coid,
    Pentry::pointer receiver,
    Pentry::pointer sender,
    Int64 sendtime,
    Pentry::pointer replyto,
    AMessageOptions::pointer options,
    const std::string& method,
    const void* content,
    UInt32 content_size )
{
    if (!msgid) {
        msgid = random64();
    }
    this->size = 0;
    this->count = 0;
    this->append(PAIR_VERSION);
    this->append(msgval);
    AMessageHeader::pointer head_ptr = static_cast<AMessageHeader::pointer>(this->bottom());
    head_ptr->construct(0);
    head_ptr->assign(msgid,
        coid,
        receiver,
        sender,
        sendtime,
        replyto,
        options);
    this->append(*head_ptr);
    AMessageMethod::pointer meth_ptr = static_cast<AMessageMethod::pointer>(this->bottom());
    meth_ptr->construct(0);
    meth_ptr->assign(method, content, content_size);
    this->append(*meth_ptr);
    return *this;
}

inline
bool
AMessage::is_valid() const
{
    if (this->count != Self::fields) {
        return false;
    }
    size_type size = this->size;
    // version
    Iterator it = this->begin();
    if (!it->is_mdtype<Element>() || it->size != sizeof(UInt32)) {
        return false;
    }
    size -= it->block_size();
    // message_value
    it.next();
    if (!it->is_mdtype<Element>() || it->size != sizeof(UInt32)) {
        return false;
    }
    size -= it->block_size();
    // header
    it.next();
    if (!static_cast<AMessageHeader::pointer>(it.get())->is_valid()) {
        return false;
    }
    size -= it->block_size();
    // method
    it.next();
    if (!static_cast<AMessageMethod::pointer>(it.get())->is_valid()) {
        return false;
    }
    size -= it->block_size();
    return (!size);
}

#endif
