FROM golang:1.22-alpine AS build

WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/chunkgate ./cmd/chunkgate

FROM alpine:3.20

RUN addgroup -S chunkgate && adduser -S -G chunkgate chunkgate
RUN apk add --no-cache ca-certificates

USER chunkgate
WORKDIR /app
COPY --from=build /out/chunkgate /usr/local/bin/chunkgate

ENV CHUNKGATE_LISTEN=:8080
ENV CHUNKGATE_DATA_DIR=/data

VOLUME ["/data"]
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --retries=3 CMD wget -qO- http://127.0.0.1:8080/healthz || exit 1

ENTRYPOINT ["chunkgate"]
