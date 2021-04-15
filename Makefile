# Makefile, Adama Sanoh, Bryn Lasher, Seattle University, CPSC4300, Spring 2021               

CCFLAGS         = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c
COURSE          = /usr/local/db6
INCLUDE_DIR     = $(COURSE)/include
LIB_DIR         = $(COURSE)/lib

# following is a list of all the compiled object files needed to build the main executable    
OBJS	= sql5300.o

# General rule for compilation                                                                
%.o: %.cpp
	g++ -I$(INCLUDE_DIR) $(CCFLAGS) -o "$@" "$<"

# Rule for linking to create the executable                                                   
# Note that this is the default target                                                        
sql5300: $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $< -ldb_cxx -lsqlparser

# Rule for removing all non-source files                                                      
clean:
	rm -f sql5300 *.o
