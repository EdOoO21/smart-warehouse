FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN go build -o /out/smart-warehouse ./cmd/smart-warehouse

FROM alpine:3.20
RUN adduser -D -H app
USER app
WORKDIR /app
COPY --from=build /out/smart-warehouse /app/smart-warehouse
ENTRYPOINT ["/app/smart-warehouse"]
