LFLAGS=-Llib
LIB=-lrequest -lpthread -lrt
FLAGS=-g -Werror -Wall -Wpedantic -std=c99 -O0 -da
CXX=gcc
SRC=src
BUILD=build
INC=-Iinclude

all: db

$(BUILD)/%.o: $(SRC)/%.c
	$(CXX) $(FLAGS) $(INC) -c $< -o $@

db: $(BUILD)/main.o $(BUILD)/server.o $(BUILD)/db_functions.o $(BUILD)/queue.o $(BUILD)/thread_pool.o

	@echo "*** Building db ***"
	$(CXX) $(FLAGS) $(LFLAGS) -o db $(BUILD)/main.o $(BUILD)/server.o $(BUILD)/db_functions.o $(BUILD)/queue.o $(BUILD)/thread_pool.o $(LIB)

	@echo "*** Success! ***"

client: $(BUILD)/client.o

	@echo "*** Building client ***"
	$(CXX) $(FLAGS) $(LFLAGS) -o client $(BUILD)/client.o $(LIB)

	@echo "*** Success! ***"

run: db
	bash test.sh

re:
	make clean && make db

rerun:
	make clean && make run

clean:
	@echo "*** Removing object files and executable ***"
	rm -f db client $(BUILD)/*

clean_client:
	@echo "*** Removing object files and executable ***"
	rm -f client $(BUILD)/client*

.PHONY: clean
