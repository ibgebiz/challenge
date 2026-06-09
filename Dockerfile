# syntax=docker/dockerfile:1

FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/api ./cmd/api \
 && CGO_ENABLED=0 go build -o /out/worker ./cmd/worker \
 && CGO_ENABLED=0 go build -o /out/scheduler ./cmd/scheduler

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /app/
# Default entrypoint is the API; compose overrides it for worker/scheduler.
ENTRYPOINT ["/app/api"]
