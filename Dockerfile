FROM gcc:latest AS build
WORKDIR /src
COPY *.c ./
RUN gcc -O2 -pthread kafka.c -o broker && gcc -O2 publisher.c -o publisher && gcc -O2 subscribe.c -o consumer

FROM debian:bookworm-slim
COPY --from=build /src/broker /src/publisher /src/consumer /app/
WORKDIR /app