build: build-win build-linux build-ss-win build-ss-linux

# generate code
generate:
	go generate level.go

# sqler
build-win: generate
	CGO_ENABLE=0 GOOS=windows go build -ldflags="-s -w -extldflags "-static" -X 'main.Version=`git log --pretty=format:'%H' -1`' -X 'main.BuildTime=`date '+%Y-%m-%d %H:%M:%S'`' -X 'main.BuildBy=`go version`'" -o bin/sqler.exe .
build-linux: generate
	CGO_ENABLE=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w -extldflags "-static" -X 'main.Version=`git log --pretty=format:'%H' -1`' -X 'main.BuildTime=`date '+%Y-%m-%d %H:%M:%S'`' -X 'main.BuildBy=`go version`'" -o bin/sqler-amd64 .
	CGO_ENABLE=0 GOOS=linux GOARCH=arm64 go build -ldflags="-s -w -extldflags "-static" -X 'main.Version=`git log --pretty=format:'%H' -1`' -X 'main.BuildTime=`date '+%Y-%m-%d %H:%M:%S'`' -X 'main.BuildBy=`go version`'" -o bin/sqler-arm64 .

# sqler-ss
build-ss-win: generate
	cd cmd/ss; CGO_ENABLE=0 GOOS=windows go build -ldflags="-s -w -X 'main.Version=`git log --pretty=format:'%H' -1`' -X 'main.BuildTime=`date '+%Y-%m-%d %H:%M:%S'`' -X 'main.BuildBy=`go version`'" -o ../../bin/sqler-ss.exe .
build-ss-linux: generate
	cd cmd/ss; CGO_ENABLE=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w -X 'main.Version=`git log --pretty=format:'%H' -1`' -X 'main.BuildTime=`date '+%Y-%m-%d %H:%M:%S'`' -X 'main.BuildBy=`go version`'" -o ../../bin/sqler-ss .
