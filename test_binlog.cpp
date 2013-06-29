#include "binlog.h"
#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "cxxlib/sys/threads.h"

int count       = 10000;
int capacity    = 100 * 1024 * 1024;
int size        = 512;
int writer      = 0;

void do_send(int capacity, int count, int size)
{
    char* buffer = new char[size];
    mom::binlog::BinLog blog("test.log", capacity);
    for(int i = 0; i < count; ++i) {
        char* p = NULL;
        while(p == NULL) {
            p = blog.reserve(size);
            if(p == NULL) {
                cxx::sys::threadcontrol::sleep(1);
            }
        }
        memcpy(p, buffer, size);
        blog.commits(size);
    }
    delete[] buffer;
}

void do_recv(int capacity, int count, int size)
{
    int received = 0;
    mom::binlog::BinLog blog("test.log", capacity);
    for(int i = 0; i < count; ++i) {
        int len = 0;
        char* p = NULL;
        while(p == NULL) {
            p = blog.acquire(&len);
            if(p == NULL) {
                cxx::sys::threadcontrol::sleep(1);
            }
//            else
//                printf("received %d, size: %d\n", i, len);
        }
        received += len;
        blog.release(len);
        if(received == count * size)
            break;
    }
    printf("received %d count %d size %d capacity %d\n",
           received, count, size, capacity);
}

int main(int argc, char* argv[])
{
    int opt;
    while((opt = getopt(argc, argv, "wc:s:f:")) != -1) {
        switch(opt) {
        case 'w': writer = 1; break;
        case 'c': count = atoi(optarg); break;
        case 's': size = atoi(optarg); break;
        case 'f': capacity = atoi(optarg); break;
        default:
            return -1;
        }
    }

    if(writer)
        do_send(capacity, count, size);
    else
        do_recv(capacity, count, size);

    return 0;
}
