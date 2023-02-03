.POHNY: build

build:
	go build -ldflags="-s -w -X 'main.Version=`git log --pretty=format:'%H' -1`' -X 'main.BuildTime=`date '+%Y-%m-%d %H:%M:%S' `'" -o sqler.exe .