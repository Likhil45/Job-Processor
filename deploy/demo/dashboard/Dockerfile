# Build from repo root: docker build -f deploy/demo/dashboard/Dockerfile .
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /demo-server ./cmd/demo-server

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
COPY --from=builder /demo-server /demo-server
EXPOSE 8080
ENTRYPOINT ["/demo-server"]
