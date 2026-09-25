package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"hftbacktest/engine"
	"hftbacktest/ipc"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:9000", "local TCP listen address")
	minPrice := flag.Int64("min-price-ticks", 0, "inclusive lower bound for integer price ticks")
	maxPrice := flag.Int64("max-price-ticks", 1_000_000, "inclusive upper bound for integer price ticks")
	queue := flag.Int("queue-capacity", 65_536, "bounded command inbox capacity")
	flag.Parse()

	book, err := engine.NewOrderBook(*minPrice, *maxPrice)
	if err != nil {
		log.Fatal(err)
	}
	matching := engine.NewEngine(book, *queue)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go matching.Run(ctx)

	server := ipc.NewServer(matching, *listen)
	log.Printf("hftd listening on %s, tick range [%d,%d]", *listen, *minPrice, *maxPrice)
	if err := server.ListenAndServe(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
