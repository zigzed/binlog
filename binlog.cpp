#include "binlog.h"
#include "cxxlib/sys/filesystem.h"

namespace mom {
    namespace binlog {

        BinLog::BinLog(const char *name, int size)
            : hdrptr_(NULL), file_(NULL), lock_(NULL)
        {
            lock_ = new cxx::sys::namedmutex((std::string(name) + ".lck").c_str());

            cxx::sys::namedmutex::scopelock guard(*lock_);

            cxx::sys::make_dir(cxx::sys::Path(name).base().c_str(), 0755);

            uint32_t filesize = sizeof(FileHeader) + size;
            file_ = new cxx::ipc::file_mapping(name, cxx::ipc::memory_mappable::ReadWrite);
            file_->size(filesize);

            header_.attach(*file_, cxx::ipc::mapped_region::ReadWrite);
            header_.move(0, sizeof(FileHeader));
            hdrptr_ = (FileHeader* )header_.data();

            writer_.attach(*file_, cxx::ipc::mapped_region::ReadWrite);
            reader_.attach(*file_, cxx::ipc::mapped_region::ReadOnly);

            if(hdrptr_->avail == 0) {
                hdrptr_->avail = size;
                hdrptr_->crc32 = 0;
                hdrptr_->idx_a = 0;
                hdrptr_->len_a = 0;
                hdrptr_->idx_b = 0;
                hdrptr_->len_b = 0;
                hdrptr_->idx_r = 0;
                hdrptr_->len_r = 0;

                header_.commit();
            }
        }

        BinLog::~BinLog()
        {
            close();
            delete file_;
            delete lock_;
        }

        void BinLog::close()
        {
            cxx::sys::namedmutex::scopelock guard(*lock_);
            header_.commit();
            writer_.commit();
            reader_.commit();
        }

        void BinLog::reset()
        {
            cxx::sys::namedmutex::scopelock guard(*lock_);

            hdrptr_->crc32 = 0;
            hdrptr_->idx_a = 0;
            hdrptr_->len_a = 0;
            hdrptr_->idx_b = 0;
            hdrptr_->len_b = 0;
            hdrptr_->idx_r = 0;
            hdrptr_->len_r = 0;

            header_.commit();
        }

        char* BinLog::reserve(int size)
        {
            cxx::sys::namedmutex::scopelock guard(*lock_);

            if(hdrptr_->len_b) {
                int free = get_free_b();
                if(size < free)
                    free = size;
                if(free == 0 || free < size)
                    return NULL;

                hdrptr_->len_r = size;
                hdrptr_->idx_r = hdrptr_->idx_b + hdrptr_->len_b;
                writer_.move(sizeof(FileHeader) + hdrptr_->idx_r, size);
                return (char* )writer_.data();
            }
            else {
                int free = get_free_a();
                if(free >= hdrptr_->idx_a) {
                    if(free == 0 || free < size)
                        return NULL;

                    hdrptr_->len_r = size;
                    hdrptr_->idx_r = hdrptr_->idx_a + hdrptr_->len_a;
                    writer_.move(sizeof(FileHeader) + hdrptr_->idx_r, size);
                    return (char* )writer_.data();
                }
                else {
                    if(hdrptr_->idx_a == 0)
                        return NULL;
                    if(hdrptr_->idx_a < size)
                        return NULL;
                    hdrptr_->len_r = size;
                    hdrptr_->idx_r = 0;
                    writer_.move(sizeof(FileHeader), size);
                    return (char* )writer_.data();
                }
            }
        }

        void BinLog::commits(int size)
        {
            cxx::sys::namedmutex::scopelock guard(*lock_);

            if(size == 0) {
                hdrptr_->len_r = 0;
                hdrptr_->idx_r = 0;
                header_.commit();
                return;
            }

            if(size > hdrptr_->len_r)
                size = hdrptr_->len_r;

            if(hdrptr_->len_a == 0 && hdrptr_->len_b == 0) {
                hdrptr_->idx_a = hdrptr_->idx_r;
                hdrptr_->len_a = size;
                hdrptr_->idx_r = 0;
                hdrptr_->len_r = 0;
                writer_.commit();
                header_.commit();
                return;
            }

            if(hdrptr_->idx_r == hdrptr_->len_a + hdrptr_->idx_a)
                hdrptr_->len_a += size;
            else
                hdrptr_->len_b += size;

            hdrptr_->idx_r = 0;
            hdrptr_->len_r = 0;

            writer_.commit();
            header_.commit();
        }

        char* BinLog::acquire(int *size)
        {
            cxx::sys::namedmutex::scopelock guard(*lock_);

            if(hdrptr_->len_a == 0) {
                *size = 0;
                return NULL;
            }

            *size = hdrptr_->len_a;
            reader_.move(sizeof(FileHeader) + hdrptr_->idx_a, *size);
            return (char* )reader_.data();
        }

        void BinLog::release(int size)
        {
            cxx::sys::namedmutex::scopelock guard(*lock_);

            if(size >= hdrptr_->len_a) {
                hdrptr_->idx_a = hdrptr_->idx_b;
                hdrptr_->len_a = hdrptr_->len_b;
                hdrptr_->idx_b = 0;
                hdrptr_->len_b = 0;
            }
            else {
                hdrptr_->len_a -= size;
                hdrptr_->idx_a += size;
            }
            header_.commit();
        }

        int BinLog::commited_size() const
        {
            cxx::sys::namedmutex::scopelock guard(*lock_);

            return hdrptr_->len_a + hdrptr_->len_b;
        }

        int BinLog::reserved_size() const
        {
            return hdrptr_->len_r;
        }

        int BinLog::capacity_size() const
        {
            return hdrptr_->avail;
        }

    }
}
