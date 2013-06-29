#ifndef MOM_BINLOG_H
#define MOM_BINLOG_H
#include "cxxlib/ipc/mmap.h"
#include "cxxlib/sys/mutex.h"

namespace mom {
    namespace binlog {


        struct FileHeader {
            int     crc32;
            int     avail;
            int     idx_a;  // region A
            int     len_a;
            int     idx_b;  // region B
            int     len_b;
            int     idx_r;  // region Reserved
            int     len_r;
        };

        struct DataHeader {
            int     size;           // 在这个数据块中的数据大小
        };

        enum Result {
            success         = 0,
            error_internal  = -1,
            error_not_open  = -2,
            error_too_big   = -3,
            error_no_free   = -4,
            error_no_data   = -5,
            error_more_data = -6,
            error_timeout   = -7,
            error_open_failed   = -8
        };

        class BinLog {
        public:
            static void remove(const char* name);

            BinLog(const char* name, int size);
            ~BinLog();

            void    reset();
            void    close();
            char*   reserve(int  size);
            void    commits(int  size);
            char*   acquire(int* size);
            void    release(int  size);

            int     commited_size() const;
            int     reserved_size() const;
            int     capacity_size() const;
        private:
            FileHeader*             hdrptr_;
            cxx::ipc::mapped_region header_;
            cxx::ipc::mapped_region writer_;
            cxx::ipc::mapped_region reader_;
            cxx::ipc::file_mapping* file_;
            cxx::sys::namedmutex*   lock_;

            inline int get_free_a() const {
                return hdrptr_->avail - hdrptr_->idx_a - hdrptr_->len_a;
            }

            inline int get_free_b() const {
                return hdrptr_->idx_a - hdrptr_->idx_b - hdrptr_->len_b;
            }
        };

    }
}

#endif
