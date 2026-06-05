FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/chunkgate ./cmd/chunkgate && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/chunkgate-delta ./cmd/chunkgate-delta

FROM alpine:3.22

RUN addgroup -S -g 10001 chunkgate && adduser -S -D -H -u 10001 -G chunkgate chunkgate
RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=build /out/chunkgate /usr/local/bin/chunkgate
COPY --from=build /out/chunkgate-delta /usr/local/bin/chunkgate-delta

ENV CHUNKGATE_LISTEN=:8080
ENV CHUNKGATE_DATA_DIR=/data

RUN mkdir -p /data && chown -R chunkgate:chunkgate /data
USER 10001:10001

VOLUME ["/data"]
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --retries=3 CMD wget -qO- http://127.0.0.1:8080/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/chunkgate"]
