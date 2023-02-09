CC=gcc
CFLAGS=-g -pedantic -std=gnu17 -Werror -Wextra

.PHONY: all
all: nyuenc

nyuenc: nyuenc.o 
	${CC} -o nyuenc nyuenc.o -lpthread

nyuenc.o: nyuenc.c 

.PHONY: clean
clean:
	rm -f *.o nyuenc
