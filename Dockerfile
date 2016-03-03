
FROM alpine:3.3

ADD *.go /genres-rw-neo4j/
ADD genres/*.go /genres-rw-neo4j/genres/

RUN apk add --update bash \
  && apk --update add git bzr \
  && apk --update add go \
  && export GOPATH=/gopath \
  && REPO_PATH="github.com/Financial-Times/genres-rw-neo4j" \
  && mkdir -p $GOPATH/src/${REPO_PATH} \
  && mv genres-rw-neo4j/* $GOPATH/src/${REPO_PATH} \
  && cd $GOPATH/src/${REPO_PATH} \
  && go get -t ./... \
  && go build \
  && mv genres-rw-neo4j /app \
  && apk del go git bzr \
  && rm -rf $GOPATH /var/cache/apk/*

CMD [ "/app" ] 