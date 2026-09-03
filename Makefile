CC = mpicc

OVLATENCY_OBJS = ovlatency.o
OVLATENCY_LIBADD = -lflux-core -ljansson

HELLO_OBJS = hello.o

PROGS = ovlatency hello

all: $(PROGS)

ovlatency: $(OVLATENCY_OBJS)
	$(CC) -o $@ $^ $(OVLATENCY_LIBADD)
hello: $(HELLO_OBJS)
	$(CC) -o $@ $^

clean:
	rm -f *.o a.out core $(PROGS)
