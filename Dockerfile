# Build the image with go-1.11
FROM golang:1.11.1 AS build

# Copy all the files needed for the build to the container
COPY . /go/src/github.com/su225/godemlia

# Set the working directory to the root of the
# godemlia project
WORKDIR /go/src/github.com/su225/godemlia/src/server

# Build the server binary with CGO disabled. This should
# produce a single binary with all the related libraries linked
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o server

# Once the build is successful, build the container which 
# runs a Godemlia bootstrap node.
FROM alpine:3.6

# Create a new user called knodeuser and switch to it
RUN mkdir node && adduser -S -D -H -h /node knodeuser
USER knodeuser

# Copy the binary from the build container
COPY --from=build /go/src/github.com/su225/godemlia/src/server/server /node
COPY --from=build /go/src/github.com/su225/godemlia/script/node-start.sh /node

# Set the environment variables denoting various node settings
# This container represents a bootstrap node
ENV BOOTSTRAP=true        \
    LISTEN_ADDR=127.0.0.1 \
    RPC_PORT=6666         \
    REST_PORT=6667        \
    CONCURRENCY=5         \
    REPLICATION=8         \
    NODE_BIN_PATH=/node   \
    NODE_ID=100

WORKDIR /node

# Start the node with the settings specified
# in the container environment
CMD sh ./node-start.sh
EXPOSE 6666 6667
