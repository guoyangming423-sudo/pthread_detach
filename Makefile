CC      = gcc
CFLAGS  = -Wall -Wextra -pthread -O2
TARGET  = demo
SRCS    = thread_pool.c demo.c
OBJS    = thread_pool.o demo.o

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

thread_pool.o: thread_pool.c thread_pool.h
	$(CC) $(CFLAGS) -c thread_pool.c

demo.o: demo.c thread_pool.h
	$(CC) $(CFLAGS) -c demo.c

clean:
	rm -f $(OBJS) $(TARGET)
