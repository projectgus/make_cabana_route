FROM rust:alpine AS builder

WORKDIR /app

RUN apk add \
    capnproto \
    capnproto-dev \
    ffmpeg-dev \
    clang-dev \
    git

COPY . .

RUN git submodule update --init
RUN RUSTFLAGS='-C target-feature=-crt-static' cargo build --release

FROM alpine

RUN apk add \
capnproto \
ffmpeg

COPY --from=builder /app/target/release/make_cabana_route /usr/local/bin/make_cabana_route
RUN chmod +x /usr/local/bin/make_cabana_route

WORKDIR /work

USER 1000:1000

ENTRYPOINT ["make_cabana_route"]