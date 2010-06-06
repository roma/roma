#

RM = rm -f
CC = gcc
OBJS = api.o scanner.o loader.o writer.o reader.o dumper.o parser.o emitter.o

default: $(OBJS)

clean:
	$(RM) $(OBJS)

.c.o:
	$(CC) -c $<