FROM gcc:latest

WORKDIR /app

COPY kafka.c publisher.c subscribe.c .

RUN gcc -pthread kafka.c -o broker
RUN gcc publisher.c -o publisher
RUN gcc subscribe.c -o consumer
EXPOSE 9092
CMD ["./broker"]