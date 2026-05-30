# Multi-stage build producing both the agent and manager binaries.
# Build a specific binary with: docker build --target agent|manager .
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/agent ./cmd/agent && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/manager ./cmd/manager

FROM gcr.io/distroless/static-debian12 AS agent
COPY --from=build /out/agent /agent
ENTRYPOINT ["/agent"]

FROM gcr.io/distroless/static-debian12 AS manager
COPY --from=build /out/manager /manager
ENTRYPOINT ["/manager"]
