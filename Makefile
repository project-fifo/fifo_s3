REBAR = $(shell pwd)/rebar3

.PHONY: deps rel package

all: compile
	$(REBAR) escriptize

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

test: all
	$(REBAR) eunit

###
### Docs
###
docs:
	$(REBAR) doc

##
## Developer targets
##

xref:
	$(REBAR) xref 

console: all
	$(REBAR) shell
