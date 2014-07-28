
Appendix B. Metadata

    Metadata -owns- {,
        max_size = 0xffffff00,

        Element : Class,
        Composite : Class,
        MStream : Class
        }

    Metadata.Element : Class -owns- {
        type$ = "MDE",
        size$ = 8,
        model -owns- {
            type : int32,
            size : int32,
            data : (byte;)[this.size]
            },
        trait -owns- {
            size_of : Function -spec- {
                -parameter- (size:int32):int32
                -process- begin
                    (Element.size$+size);
                    end
                },
            construct : Function -spec- {
                -parameter- (size:int32)
                -process- begin
                    self.size << Element.type$;
                    self.size << size;
                    end
                },
            block_size : Function -spec- {
                -parameter- ():int32
                -process- begin
                    when self.is_mdtype(Composite) then Composite.size_of(self.size)
                    when self.is_mdtype(Element) then Element.size_of(self.size)
                    else 0;
                    end
                },
            is_mdtype : Function -spec- {
                -parameter- (Ty<:Element):Bool
                -process- begin
                    (self.type = Ty.type$);
                    end
                },
            assign : Function -spec- {
                -parameter- (rhs:String)
                -process- begin
                    self.assign(rhs.data,rhs.size);
                    end
                },
            assign : Function -spec- {
                -parameter- (rhs)
                -process- begin
                    self.assign(memory_address_of(rhs),sizeof(rhs));
                    self.size << sizeof(rhs);
                    end
                },
            assign : Function -spec- {
                -parameter- (base:MemoryAddress,
                             size:int32)
                -process- begin
                    memory_copy(base,self.data,size);
                    self.size << size;
                    end
                },
            }
        }

    Metadata.Composite <: Metadata.Element -owns- {
        type$ = "MDC",
        size$ = 12,
        model -owns- {
            count : int32,
            data : (byte;)[this.size]
            },
        trait -owns- {
            size_of : Function -spec- {
                -parameter- (size:int32):int32
                -process- begin
                    (Composite.size$+size);
                    end
                },
            construct : Function -spec- {
                -parameter- (size:int32)
                -process- begin
                    self.type << Composite.type$;
                    self.size << size - Composite.size$;
                    self.count << 0;
                    end
                },
            item : Function -spec- {
                -parameter- (i:int32):Ref()
                -process- begin
                    if i >= self.count then null;
                    (λ(j,it:Iterator).-process- begin
                        if j>0 then this(j-1,it.next);
                        else this.result << it.get;
                        end)(i,self.begin_iterator)
                    end
                },
            bottom : Function -spec- {
                -parameter- ():MemoryAddress
                -process- begin
                    (memory_address_of(self)+self.block_size());
                    end
                },
            begin_iterator : Function -spec- {
                -parameter- ():Composite.Iterator
                -process- begin
                    Composite.Iterator.make(memory_address_of(self.data));
                    end
                },
            end_iterator : : Function -spec- {
                -parameter- ():Composite.Iterator
                -process- begin
                    Composite.Iterator.make(self.bottom());
                    end
                },
            is_valid : Function -spec- {
                -parameter- ():Bool
                -process- begin
                    (λ(it:Iterator,size:int32).-process- begin
                        if it = self.end_iterator then
                            this.result << (size = 0);
                        else
                            let item = it.get() in
                                if item.block_size() = 0 or item.block_size() > size then false
                                else
                                    if item.is_mdtype(Composite) and not item.is_valid() then false
                                    else this(it.next,size-item.block_size());
                        end)(self.begin_iterator,self.size)
                    end
                },
            append : Function -spec- {
                -parameter- (item:Ty)
                -process- begin
                    if item.asType(Element).is_mdtype(Element) or item.asType(Element).is_mdtype(Composite) then
                        self.append(item.asType(Element));
                    else self.append(memory_address_of(item),sizeof(item));
                    end
                },
            append : Function -spec- {
                -parameter- (item:String)
                -process- begin
                    if item.empty() then self.append(null,0)
                    else self.append(item.data,item.size);
                    end
                },
            append : Function -spec- {
                -parameter- (item:Element∨Composite)
                -process- begin
                    let size = item.block_size() in
                        if size = 0 then throw Error("invalid parameter");
                        if self.block_size()+size > max_size then throw Error("overflow");
                        if memory_address_of(item) != self.bottom() then
                            memory_copy(memory_address_of(item),self.bottom(),size);
                        self.size << self.size+size;
                        self.count << self.count+1;
                    end
                },
            append : Function -spec- {
                -parameter- (base:MemoryAddress,
                             size:int32)
                -process- begin
                    if self.block_size()+Element.size_of(size) > max_size then throw Error("overflow");
                    let elem = self.bottom().asType(Element) in
                        elem.construct(size));
                        if base != 0 and size != 0) then 
                            memory_copy(base,elem.data,size);
                        self.size << self.size+elem.block_size();
                    self.count << self.count+1;
                    end
                },
            append_named_item : Function -spec- {
                -parameter- (name:String,
                             item:Ty)
                },
            update : Function -spec- {
                -parameter- (i:int32,
                             item:Ty)
                -process- begin
                    if item.asType(Element).is_mdtype(Element) or item.asType(Element).is_mdtype(Composite) then
                        self.update(i,item.asType(Element));
                    else self.update(i,memory_address_of(item),sizeof(Ty));
                    end
                },
            update : Function -spec- {
                -parameter- (i:int32,
                             item:String)
                -process- begin
                    self.update(i,item.data,item.size);
                    end
                },
            update : Function -spec- {
                -parameter- (i:int32,
                             item:Element∨Composite)
                -process- begin
                    if i >= self.count then throw Error("out of range");
                    let delta = item.block_size()-self[i].block_size() in
                        if self.block_size()+delta > max_size then throw Error("overflow");
                        if delta != 0 and i < self.count-1 then
                            let pos = memory_address_of(self[i+1]) in
                                memory_move(pos,pos+delta,self.bottom()-pos);
                        memory_copy(memory_address_of(item),memory_address_of(self[i]),item.block_size());
                        self.size << self.size+delta;
                    end
                },
            update : Function -spec- {
                -parameter- (i:int32,
                             base:MemoryAddress,
                             size:int32)
                -process- begin
                    if i >= self.count then throw Error("out of range");
                    let delta = Element.size_of(size)-self[i].block_size() in
                        if self.block_size()+delta > max_size then throw Error("overflow");
                        if delta != and i < self.count-1 then
                            let pos = memory_address_of(self[i+1]) in
                                memory_move(pos,pos+delta,self.bottom()-pos);
                        let elem = self[i].asType(Element) in
                            elem.construct(size);
                            memory_copy(base,elem.data,size);
                        self.size << self.size+delta;
                    end
                },
            },
        Iterator : Class -owns- {
            model -owns- {
                item : Ref(Element)
                },
            trait -owns- {
                get : Function -spec- {
                    -parameter- ():Ref(Element)
                    -process- begin
                        self.pos;
                        end
                    },
                next : Function -spec- {
                    -process- begin
                        if self.item.block_size() = 0 then self.item << null
                        else self.item << memory_address_of(self.item)+self.item.block_size();
                        end
                    },
                },
            },
        }

    Metadata.MStream(Allocator) <: Buffer(Allocator) -owns- {
        trait -owns- {
            block_size : Function -spec- {
                -parameter- ():int32
                -process- begin
                    Metadata.Composite.sizeof(self.head().size);
                    end
                },
            count : Function -spec- {
                -parameter- ():int32
                -process- begin
                    self.head().count;
                },
            item : Function -spec- {
                -parameter- (i:int32):Ref()
                -process- begin
                    if self.empty() then
                        this.result << null;
                    else
                        let it = self.begin_iterator() in
                            (λj.-process- begin
                                if j = 0 then
                                    this.result << it.get();
                                else
                                    if it.ended() then
                                        this.result << null;
                                    else
                                        it.next();
                                        this.Λ(j-1);
                                end) (i);
                    end
                },
            begin_iterator : Function -spec- {
                -parameter- ():MStream.Iterator
                -process- begin
                    let pos = self.head().asType(Composite).data in
                        this.result << MStream.Iterator.make(self.chunk_capacity,self.chunk_list.first,pos,self.block_size());
                    end
                },
            is_valid : Function -spec- {
                -parameter- ():Bool
                -process- begin
                    let it = self.begin_iterator() in
                        (λ.-process- begin
                            if it.ended() then
                                this.result << true;
                            else
                                let mdata = it.get().asType(Composite) in
                                    if mdata.is_valid() then
                                        it.next();
                                        this.Λ();
                                    else
                                        this.result << false;
                            end) ();
                    end
                },
            append : Function -spec- {
                -parameter- (item:Ty)
                -process- begin
                    if item.asType(Element).is_mdtype(Element) or item.asType(Element).is_mdtype(Composite) then
                        self.append(item.asType(Element));
                    else self.append(memory_address_of(item),sizeof(item));
                    end
                },
            append : Function -spec- {
                -parameter- (item:String)
                -process- begin
                    if item.empty() then self.append(null,0)
                    else self.append(item.data,item.size);
                    end
                },
            append : Function -spec- {
                -parameter- (item:Element∨Composite)
                -process- begin
                    let mdata = self.available_chunk().asType(Composite) in
                        mdata.append(item);
                    end
                },
            append : Function -spec- {
                -parameter- (base:MemoryAddress,
                             size:int32)
                -process- begin
                    let bsize = Element.size_of(size),
                        free_size = self.free_size
                     in
                        if bsize > free_size and free_size > 0 then
                            let pos_1 = self.allocate(free_size),
                                pos_2 = self.allocate(bsize-free_size)
                             in
                                if free_size >= Element.size$ then
                                    let md_elem = pos_1.as(Element) in
                                        md_elem.construct(size);
                                        let size_1 = free_size-Element.size$ in
                                            if base != null then
                                                memory_copy(base,md_elem.data,size_1);
                                                memory_copy(base+size_1,pos_2,size-size_1);
                                else
                                    let hd = (Element.type$,size),
                                        size_1 = Element.size$-free_size
                                     in
                                        memory_copy(hd,pos_1,free_size);
                                        memory_copy(hd+free_size,pos_2,size_1);
                                        if base != null then
                                            memory_copy(base,pos_2+size_1,size);
                        else
                            let pos = self.allocate(bsize) in
                                pos.asType(Element).construct(size);
                                if base != null then
                                    memory_copy(base,pos+Element.size$,size);
                        self.inc(bsize);
                    end
                },
            attach : Function -spec- {
                -parameter- (item:Element)
                -process- begin
                    let base = memory_address_of(item),
                        size = item.block_size(),
                        free_size = self.free_size
                     in
                        if size > free_size and free_size > 0 then
                            let pos_1 = self.allocate(free_size),
                                pos_2 = self.allocate(size-free_size)
                             in
                                memory_copy(base,pos_1,free_size);
                                memory_copy(base+free_size,pos_2,size-free_size);
                        else
                            let pos = self.allocate(size) in
                                memory_copy(base,pos,size);
                        self.inc(size);
                    end
                },
            append_named_item : Function -spec- {
                -parameter- (name:String,
                             item:Ty)
                -process- begin
                    let new_md = self.allocate(Composite.size_of(0)) in
                        new_md.construct(0);
                        self.new_append(new_md,name);
                        self.new_append(new_md,item);
                        self.inc(new_md.block_size());
                    end
                },
            new_append : Function -spec- {
                -parameter- (md:Composite,
                             item:Element∨Composite)
                -process- begin
                    self.allocate(item.block_size());
                    md.append(item);
                    end
                },
            new_append : Function -spec- {
                -parameter- (md:Composite,
                             item:Ty)
                -process- begin
                    self.allocate(sizeof(item));
                    md.append(item);
                    end
                },
            serialize : Function -spec- {
                -parameter- (buf:Ref())
                },
            head : Function -spec- {
                -parameter- ():MemoryAddress
                -process- begin
                    let chunk = self.chunk_list.first in
                        this.result << memory_address_of(chunk);
                },
            inc : Function -spec- {
                -parameter- (size:int32)
                -process- begin
                    let head = self.head() in
                        head.size << head.size+size;
                        head.count << head.count+1;
                    end
                },
            },
        Iterator : Class -owns- {
            model -owns- {
                chunk_capacity : int32,
                chunk : Ref(MStream.Chunk),
                offset : MemoryAddress,
                end_offset : MemoryAddress,
                },
            trait -owns- {
                get : Function -spec- {
                    -parameter- ():Ref(Element)
                    -process- begin
                        let pos = memory_address_of(self.chunk) + MOD(self.offset,self.chunk_capacity) in
                            this.result << pos.asType(Element);
                        end
                    },
                ended : Function -spec- {
                    -parameter- ():Bool
                    -process- begin
                        self.offset >= self.end_offset;
                        end
                    },
                next : Function -spec- {
                    -process- begin
                        if not self.ended() then
                            let item = self.get() in
                                let next_offset = self.offset + item.block_size() in
                                    if next_offset / self.chunk_capacity > self.offset / self.chunk_capacity then
                                        self.chunk.next();
                                    self.offset << next_offset;
                        end
                    },
                },
            },
        }
