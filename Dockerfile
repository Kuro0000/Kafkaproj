FROM gcc:bookworm AS build
WORKDIR /src
COPY *.c ./
RUN gcc -O2 -o broker kafka.c -lpthread && gcc -O2 publisher.c -o publisher && gcc -O2 subscribe.c -o consumer

FROM debian:bookworm-slim
COPY --from=build /src/broker /src/publisher /src/consumer /app/
WORKDIR /app
EXPOSE 9092 9100
CMD ["./broker"]