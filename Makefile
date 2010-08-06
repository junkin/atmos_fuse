LIBS=-latmos -L/usr/local/lib/ -I../../atmos-c -lssl -lcurl

atmos_fuse : atmos_fuse.o log.o
	gcc -g `pkg-config fuse --libs` -o atmos_fuse atmos_fuse.o log.o ${INC} ${LIBS}

atmos_fuse.o : atmos_fuse.c log.h params.h
	gcc -g -Wall `pkg-config fuse --cflags` ${LIBS} ${INC} -c atmos_fuse.c

log.o : log.c log.h params.h
	gcc -g -Wall `pkg-config fuse --cflags` ${LIBS} ${INC} -c log.c

clean:
	rm -f atmos_fuse *.o
