
11 事务 Transaction

11.1 算法
    事务机制原则: 读操作不阻塞写操作, 写操作不阻塞读操作.

11.1.1 版本机制
    采用Read-Copy-Update(RCU) 延迟版本更新方式.
    事务活动过程中, 不直接修改对象, 而是分配新的对象页作为事务私有的最新版本.
    私有版本仅对拥有的事务可见, 其它事务不可见.

    冲突分析:
        不同 Space 的事务无冲突.
        同一 Space 的事务, 作用不同对象的事务(正交)不冲突.
        作用相同对象的事务, 若作用于不同Slot且不同页则不冲突.
        其它情况则可能冲突.
        冲突操作有,
            * update vs update, delete
            * delete vs update

    一致性检测
        如果事务中被访问对象的版本不变, 则对象一致; 反之则对象不一致.

    当事务提交时, 对事务中被修改的所有对象进行处理,
    (1) 位于事务缓冲内的被修改页写入持久存储介质.
    (2) 切换对象的目录索引, 形成新的最后只读版本.
    (3) 释放上一最后只读版本的对应页. 如果对象正在被读, 则延迟至读事务完成后释放.

    事务活动中, 如果对象一致, 则继续执行事务. 如果访问对象不一致, 则中止事务, 然后重新执行事务.

    当事务中止时, 释放事务缓冲区以抛弃所有修改, 而不改变最后只读版本.

11.1.2 状态分析

    delete-commit   进入删除提交, 可重入, 可 abort
    deleted         删除成功, 不能 abort
    update-commit   进入修改提交, 重入即冲突, 可 abort

    stable(V0)
        --commit delete--> delete_commit
        --commit update--> update_commit

    delete_commit
        --stable delete--> deleted
        --abort--> stable(V0)

    update_commit
        --stable update--> stable(V1)
        --abort--> stable(V0)

    --new,refer--> stable

    --when conflicted --> stable

11.1.3 Commit

    Step-1 冲突检测
    Step-2 保存更新版本
    Step-3 提交版本
    Step-4 后处理

11.1.4 Abort

    Step-1 恢复旧版本
    Step-2 后处理

11.1.5 事务嵌套


11.2 事务类型

    Transaction(Allocator) : Class -owns- {
        ShadowEntry : Class -owns- {
            model -owns- {
                (committed,operation,refering) : (bit(31),{op_refer,op_new,op_delete,op_update},int24),
                image : Ref(RelationalObject),
                storage : SpaceAddress,
                },
            }

        model -owns- {
            level : int32,
            space : Ref(RSpace),
            index : ShadowIndex,
            auto_buffer : Bool,
            },
        trait -owns- {
            initialize : Function -spec- {
                -parameter- (space:RSpace,
                             buffer:Buffer):Transaction
                -process- begin
                    self.level << 0;
                    self.space << space;
                    self.index.construct(buffer);
                    self.auto_buffer << false;
                    end
                },
            initialize : Function -spec- {
                -parameter- (space:RSpace,
                             alloc:Allocator):Transaction
                -process- begin
                    self.level << 0;
                    self.space << space;
                    let buffer = alloc.allocate(sizeof(Buffer)).asType(Buffer) in
                        buffer.construct(def_buffer_size,alloc);
                        self.index.construct(buffer);
                        self.auto_buffer << true;
                    end
                },
            destroy : Function -spec- {
                -parameter- ()
                -process- begin
                    if self.auto_buffer then
                        let alloc = self.index.allocator() in
                            alloc.deallocate(self.index.buffer,sizeof(Buffer));
                    end
                },
            allocate : Function -spec- {
                -parameter- (size:int32):SBlock
                -process- begin
                    self.space.allocate(size);
                    end
                },
            begin : Function -spec- {
                -process- begin
                    if self.state = state_commit or self.state = state_abort then
                        self.index.clear();
                        self.state << state_active;
                    else
                        self.state << self.state+1;
                    end
                },
            begin_once : Function -spec- {
                -process- begin
                    if self.state < self.state_active then
                        self.begin();
                    end
                },
            commit : Function -spec- {
                -parameter- (retry:int32)
                -process- begin
                    (when self.state = state_commit or self.state = state_abort then
                        throw Error("transaction completed");
                     when self.state = state_active then
                        self.state = state_commit;
                        self.act_commit();
                     else
                        self.state << self.state-1;
                    );
                    end
                },
            act_commit : Function -spec- {
                -parameter- ():Error
                -process- begin
                    self.index.for_each(
                        λx:OID,entry:ShadowEntry.-process- begin
                            let x_ = self.space.refer(x),
                                shadow = entry.image
                             in
                                if (entry.operation = op_delete and self.delete_conflicted(x_,shadow))
                                    or (entry.operation = op_update and self.update_conflicted(x_,shadow))
                                then
                                    self.abort();
                                    throw Error.make("confliction",self);
                            end );
                    self.index.for_each(
                        λx:OID,entry:ShadowEntry.-process- begin
                            (when entry.operation = op_refer then
                                self.commit_refer(entry);
                             when entry.operation = op_new then
                                self.commit_new(entry);
                             when entry.operation = op_delete then
                                self.commit_delete(entry);
                             when entry.operation = op_update then
                                self.commit_update(entry);
                            );
                            end );
                    self.index.for_each(
                        λx:OID,entry:ShadowEntry.-process- begin
                            (when entry.operation = op_refer then
                                self.stable_refer(entry);
                             when entry.operation = op_new then
                                self.stable_new(entry);
                             when entry.operation = op_delete then
                                self.stable_delete(entry);
                             when entry.operation = op_update then
                                self.stable_update(entry);
                            );
                            end );
                    self.space.commit_write();
                    end
                },
            commit_refer : Function -spec- {
                -parameter- (entry:ShadowEntry)
                -process- begin
                    if entry.image != null then
                        entry.image.control.release_refer();
                    entry.refering << 0;
                    entry.set_committed();
                    end
                },
            stable_refer : Function -spec- {
                -parameter- (entry:ShadowEntry)
                -process- begin
                    let shadow = entry.image in
                        if shadow != null and not shadow.control.in_refer() then
                            when shadow.control.state = state_deleted then
                                let current = self.space.refer(shadow.id) in
                                    if shadow = current then
                                        self.space.free_image(shadow);
                            when shadow.control.state = state_update_commit then
                                let current = self.space.refer(shadow.id) in
                                    if shadow != current then
                                        self.space.free_shadowed_image(shadow,current);
                    end
                },
            commit_new : Function -spec- {
                -parameter- (entry:ShadowEntry)
                -process- begin
                    let shadow = entry.image in
                        shadow.upgrade();
                        if shadow.is_sliced() then
                            shadow.slice_table().for_each(
                                λ(se,sln):(SliceEntry,int32).-process- begin
                                    se.storage << self.space.write(se.image,se.size);
                                    se.unset_shadowed();
                                    end );
                        entry.storage << self.space.write(shadow,shadow.size);
                        entry.set_committed();
                    end
                },
            stable_new : Function -spec- {
                -parameter- (entry:ShadowEntry)
                -process- begin
                    self.space.update_object(entry.image,entry.storage);
                    entry.image.control.transit(state_stable);
                    end
                },
            commit_delete : Function -spec- {
                -parameter- (entry:ShadowEntry)
                -process- begin
                    let shadow = entry.image in
                        if shadow.version = 0 then
                            shadow.control.acquire_delete();
                            entry.set_committed();
                        else
                            let current = self.space.refer(shadow.id) in
                                if current.version != shadow.version then
                                    self.abort();
                                    throw Error.make("delete confliction",self);
                                let ctrl = current.control in
                                    (when ctrl.state = state_update_commit then
                                        self.abort();
                                        throw Error.make("delete confliction",self);
                                     when ctrl.state = state_stable then
                                        ctrl.acquire_delete();
                                        ctrl.release_refer();
                                        entry.set_committed();
                                     when ctrl.state = state_deleted then
                                        ctrl.release_refer();
                                        entry.set_committed();
                                    );
                    end
                },
            stable_delete : Function -spec- {
                -parameter- (entry:ShadowEntry)
                -process- begin
                    let shadow = entry.image,
                        current = self.space.refer(entry.image.id)
                     in
                        if shadow.version = 0 then
                            self.space.delete_object(shadow.id);
                            self.space.free_image(shadow);
                        else
                            self.space.free_image(shadow);
                            if current != null and current.control.state != state_deleted then
                                current.control.transit(state_deleted);
                                self.space.delete_object(current.id);
                                if not current.control.in_refer() then
                                    self.space.free_image(current);
                    end
                },
            commit_update : Function -spec- {
                -parameter- (entry:ShadowEntry)
                -process- begin
                    let shadow = entry.image in
                        if shadow.version > 0 then
                            let current = self.space.refer(shadow.id)
                                if current.version != shadow.version then
                                    self.abort();
                                    throw Error.make("update confliction",self);
                                let ctrl = current.control in
                                    (when ctrl.state = state_update_commit
                                        or ctrl.state = state_deleted
                                        or ctrl.in_delete()
                                        then
                                        self.abort();
                                        throw Error.make("update confliction",self);
                                     else ctrl.state = state_stable then
                                        ctrl.transit(state_update_commit);
                                        ctrl.release_refer();
                                    );
                        entry.storage << self.write_update(shadow);
                        entry.set_committed();
                    end
                },
            stable_update : Function -spec- {
                -parameter- (entry:ShadowEntry)
                -process- begin
                    let shadow = entry.image,
                        current = self.space.refer(shadow.id)
                     in
                        self.space.update_object(shadow,entry.storage);
                        if shadow.version > 1 then
                            if current != null and not current.control.in_refer() then
                                self.space.free_shadowed_image(current,shadow);
                    end
                },
            write_update : Function -spec- {
                -parameter- (shadow:RelationalObject):SpaceAddress
                -process- begin
                    shadow.upgrade();
                    shadow.control.reset();
                    if shadow.is_sliced() then
                        shadow.slice_table().for_each(
                            λ(se,sln):(SliceEntry,int32).-process- begin
                                if se.shadowed() then
                                    se.storage << self.space.write(se.image,se.size);
                                    se.unset_shadowed();
                                end );
                    self.space.write(shadow,shadow.size);
                    end
                },
            abort : Function -spec- {
                -parameter- ()
                -process- begin
                    self.state << state_abort;
                    self.index.for_each(
                        λx:OID,entry:ShadowEntry.-process- begin
                            (when entry.operation = op_refer then
                                self.abort_refer(entry);
                             when entry.operation = op_new then
                                self.abort_new(entry);
                             when entry.operation = op_delete then
                                self.abort_delete(entry);
                             when entry.operation = op_update then
                                self.abort_update(entry);
                            );
                            end );
                    end
                },
            abort_refer : Function -spec- {
                -parameter- (entry:ShadowEntry)
                -process- begin
                    if not entry.committed() then
                        entry.refering << 0;
                        let shadow = entry.image in
                            if shadow != null then
                                shadow.control.release_refer();
                                let current = self.space.refer(shadow.id) in
                                    if not shadow.in_refer()
                                        and (shadow.is_deleted() or shadow.version != current.version)
                                    then
                                        self.space.free_shadowed_image(shadow,current);
                    end
                },
            abort_new : Function -spec- {
                -parameter- (entry:ShadowEntry)
                -process- begin
                    self.space.free_image(entry.image);
                    if entry.committed() then
                        self.space.delete_object(entry.image.id);
                    end
                },
            abort_delete : Function -spec- {
                -parameter- (entry:ShadowEntry)
                -process- begin
                    self.space.free_image(entry.image);
                    let current = self.space.refer(entry.image.id) in
                        if current != null then
                            if entry.committed() then
                                if current.control.in_delete() then
                                    current.control.transit(state_stable);
                                    current.control.release_delete();
                            else
                                current.control.release_refer();
                    end
                },
            abort_update : Function -spec- {
                -parameter- (entry:ShadowEntry)
                -process- begin
                    self.space.free_shadowed_image(entry.image);
                    let current = self.space.refer(entry.image.id) in
                        if current != null then
                            if entry.committed() then
                                if current.control.state = state_update_commit then
                                    current.control.transit(state_stable);
                            else
                                current.control.release_refer();
                    end
                },
            delete : Function -spec- {
                -parameter- (x:OID)
                -process- begin
                    let shadow = self.make_shadow(x,1,0) then
                        if x.is_type(Relement) or x.is_type(RelementVector) then
                            Relement.delete(self,shadow);
                        else
                            RelationalObject.delete(self,shadow);
                    end
                },
            new : Function -spec- {
                -parameter- (Ty)
                            (args):Ty
                -process- begin
                    let instance = Ty.new(self,args) >>
                        self.push(instance,op_new);
                    end
                },
            derefer : Function -spec- {
                -parameter- (x:OID)
                -process- begin
                    let entry = self.index.lookup(x) in
                        if entry != null
                            and entry.image != null
                            and entry.operation = op_refer
                        then
                            entry.release_refer();
                            if entry.refering != 0 then
                                self.commit_refer(entry);
                                self.stable_refer(entry);
                                entry.image << null;
                    end
                },
            refer : Function -spec- {
                -parameter- (x:OID):RelationalObject
                -process- begin
                    let image = self.refer_included(x) in
                        if image != null then
                            this.result << image;
                        else
                            let x_ = self.space.refer(x,acquire_counting) in
                                if x_ != null and not x_.control.in_delete() then
                                    self.push(x_,op_refer);
                                    this.result << x_;
                                else
                                    this.result << null;
                    end
                },
            refer_included : Function -spec- {
                -parameter- (x:OID):RelationalObject
                -process- begin
                    let entry = self.index.lookup(x) in
                        if entry = null then
                            this.result << null;
                        else
                            if entry.operation = op_delete then
                                this.result << null;
                            else
                                if entry.image = null then
                                    entry.image << self.space.refer(x,acquire_counting);
                                else
                                    entry.image << self.space.refer(x);
                                entry.operation << op_refer;
                                entry.acquire_refer();
                                this.result << entry.image;
                    end
                },
            shadow : Function -spec- {
                -parameter- (x:OID,
                             ext:int32):RelationalObject
                -process- begin
                    self.make_shadow(x,0,ext);
                    end
                },
            shadow : Function -spec- {
                -parameter- (base:MemoryAddress,
                             size:int32):MemoryAddress
                -process- begin
                    let block = self.shadow_ext(base,size,0) in
                        this.result << block.base;
                    end
                },
            make_shadow : Function -spec- {
                -parameter- (x:OID,
                             to_delete:bit,
                             ext:int32):RelationalObject
                -process- begin
                    let entry = self.index.lookup(x),
                        op = (if to_delete then op_delete else op_update)
                     in
                        if ext != 0 then
                            let shadow_block = self.shadow_ext(entry.image,entry.image.size,ext) in
                                let xshadow = shadow_block.base.asType(RelationalObject) in
                                    xshadow.resize(shadow_block.size);
                                    entry.image << xshadow;
                        else
                            if entry.operation = op_refer then
                                entry.image << self.shadow(entry.image,entry.image.size);
                        entry.operation << op;
                        this.result << entry.image;
                    end
                },
            shadow_ext : Function -spec- {
                -parameter- (base:MemoryAddress,
                             size:int32,
                             ext:int32):SBlock
                -process- begin
                    let block = self.space.allocate(size+ext) >>
                        copy_memory(base,block.base,min(size,size+ext));
                    end
                },
            push : Function -spec- {
                -parameter- (x:RelationalObject,
                             op:{op_refer,op_new,op_delete,op_update})
                -process- begin
                    let refering = (if op = op_refer then 1 else 0) in
                        let entry = ShadowEntry.make(x,0,(not_committed,op,refering));
                            self.index.insert(x.id,entry);
                    end
                },
            delete_conflicted : Function -spec- {
                -parameter- (current:RelationalObject,
                             shadow:RelationalObject):Bool
                -process- begin
                    (current.control.state = state_update_commit or current.version != shadow.version);
                    end
                },
            update_conflicted : Function -spec- {
                -parameter- (current:RelationalObject,
                             shadow:RelationalObject):Bool
                -process-
                    (current.control.in_delete() or
                     current.control.state = state_deleted or
                     current.control.state = state_update_commit or
                     current.version != shadow.version);
                    end
                },
            },

        new : Function -spec- {
            -parameter- (space:RSpace,
                         alloc:Allocator):Transaction
            -process- begin
                let instance = alloc.allocate(sizeof(Transaction)).asType(Transaction) >>
                    instance.initialize(space,alloc);
                end
            },
        new : Function -spec- {
            -parameter- (space:RSpace,
                         buffer:Buffer):Transaction
            -process- begin
                let instance = buffer.allocate(sizeof(Transaction)).asType(Transaction) >>
                    instance.initialize(space,buffer);
                end
            },
        delete : Function -spec- {
            -parameter- (instance:Transaction,
                         alloc:Allocator)
            -process- begin
                instance.destroy();
                alloc.deallocate(instance,sizeof(Transaction));
                end
            },
        }

    Transaction.ShadowIndex <: BTree(OID,Ref(ShadowEntry),Allocator) {
        model -owns- {
            alloc : Ref(Allocator),
            },
        trait -owns- {
            construct : Function -spec- {
                -parameter- (alloc:Ref(Allocator))
                -process- begin
                    self.alloc << alloc;
                    self..construct(alloc);
                    end
                },
            lookup : Function -spec- {
                -parameter- (id:OID):ShadowEntry
                -process- begin
                    self..lookup(id);
                    end
                },
            insert : Function -spec- {
                -parameter- (id:OID,
                             entry:ShadowEntry)
                -process- begin
                    let insert_entry = self.alloc.allocate(sizeof(entry)).asType(ShadowEntry) in
                        copy entry to insert_entry;
                        self..insert(id,insert_entry);
                    end
                },
            }
        }
