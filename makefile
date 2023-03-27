.POHNY: generate build-win build-linux build

generate:
	go generate level.go

build-win: generate
	CGO_ENABLE=0 GOOS=windows go build -ldflags="-s -w -X 'main.Version=`git log --pretty=format:'%H' -1`' -X 'main.BuildTime=`date '+%Y-%m-%d %H:%M:%S'`' -X 'main.BuildBy=`go version`'" -o bin/sqler.exe .

build-linux: generate
	CGO_ENABLE=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w -X 'main.Version=`git log --pretty=format:'%H' -1`' -X 'main.BuildTime=`date '+%Y-%m-%d %H:%M:%S'`' -X 'main.BuildBy=`go version`'" -o bin/sqler .

build: build-win build-linux