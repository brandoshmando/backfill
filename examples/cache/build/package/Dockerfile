FROM golang:1.15.7-alpine as builder
COPY . src/
WORKDIR src/examples/cache/cmd/consumer
RUN go mod download
RUN go build -o /go/bin/consumer .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /go/bin/consumer /bin/

ENTRYPOINT ["/bin/consumer"]
