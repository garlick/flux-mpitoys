CC = mpicc

OVLATENCY_OBJS = ovlatency.o
LIBADD = -lflux-core -ljansson

all: ovlatency

ovlatency: $(OVLATENCY_OBJS)
	$(CC) -o $@ $^ $(LIBADD)

clean:
	rm -f *.o a.out core
