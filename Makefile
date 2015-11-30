##=======================================================================##
## Makefile
## Created: Wed Aug 05 14:35:14 PDT 2015 @941 /Internet Time/
# :mode=makefile:tabSize=3:indentSize=3:
## Purpose: 
##======================================================================##

SHELL=/bin/bash
GPATH = $(shell pwd)

.PHONY: fmt test get

fmt:
	@GOPATH=${GPATH} gofmt -s -w src/${PROJECT_NAME}

get:
	@GOPATH=${GPATH} go get ${PKG} #PKG is passed to the task on the command line, for example `make get PKG=github.com/pelletier/go-toml`
	@find src -type d -name '.hg' -or -type d -name '.git' | xargs rm -rf
	@rm -f bin/cmd
	@rm -f bin/example

test: fmt
	@GOPATH=${GPATH} go test

scrape:
	@find src -type d -name '.hg' -or -type d -name '.git' | xargs rm -rf

clean:
	@GOPATH=${GPATH} go clean

list:
	@GOPATH=${GPATH} go list ${PROJECT_NAME}/main/${PROJECT_NAME}
