# The linearizability checker: a static Go binary around Porcupine. Building the image also vets and tests it, so a
# broken checker never checks a history.
#
#   docker build -t keyforge-checker keyforge-chaos/checker
#   docker run --rm -v "$PWD/results/run:/run" keyforge-checker -history /run/history.json
FROM golang:1.25-alpine AS build
ENV CGO_ENABLED=0
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go vet ./... && go test ./... && go build -trimpath -o /out/checker .

FROM scratch
COPY --from=build /out/checker /checker
ENTRYPOINT ["/checker"]
