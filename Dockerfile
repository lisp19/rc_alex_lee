FROM golang:1.27.1 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/notifier ./cmd/notifier
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/notifier /notifier
USER nonroot:nonroot
ENTRYPOINT ["/notifier"]
