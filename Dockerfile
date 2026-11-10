FROM rust:1.99

WORKDIR /usr/src/load-balancer
COPY . .

RUN cargo install --path .

CMD ["load-balancer"]
