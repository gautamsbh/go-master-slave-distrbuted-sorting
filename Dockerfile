FROM golang:1.19-alpine as build
# Set the Current Working Directory inside the container
WORKDIR /app
# copy project files from current directory to docker filesystem
COPY . .
# install dependencies
RUN go mod download
# Build the Go app
RUN go build -o main main.go

# Final stage build
FROM alpine:latest
# Read environment variables
ENV FileName=names.txt
# Workdir
WORKDIR /usr/bin
# copy main executable file from build stage
COPY --from=build /app/main .
COPY --from=build /app/names.txt .
# Expose port
EXPOSE 3000
# run program command
CMD ["main", "--numberOfNodes", "3", "clusterIp", "127.0.0.1", "port", "3000", "fileNme", "${FileName}"]
