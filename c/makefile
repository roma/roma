#

RM = rm -f
CC = gcc
OBJS = roma_common.o roma_connection.o roma_sender.o roma_routing_table.o roma_client.o

SHA1_OBJS = sha1/sha1.o
YAML_OBJS = yaml/api.o yaml/scanner.o yaml/loader.o yaml/writer.o \
	yaml/reader.o yaml/dumper.o yaml/parser.o yaml/emitter.o
LIBS = libromaclient.a

default: clean libromaclient

.c.o:
	$(CC) -c $<

main: main.c libromaclient.a
	$(CC) -o main main.c -L. -lromaclient

clean:
	$(RM) $(OBJS)
	$(RM) $(LIBS)
	make -C sha1 clean
	make -C yaml clean

dir_sha1:
	make -C sha1

dir_yaml:
	make -C yaml

libromaclient: $(OBJS) dir_sha1 dir_yaml
	ar rcs $@.a $(OBJS) $(SHA1_OBJS) $(YAML_OBJS)
