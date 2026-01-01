package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	pubsub "cloud.google.com/go/pubsub/apiv1"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
)

const (
	scannerMaxBytes = 1 * 1024 * 1024  // 1 MB per line
	publishTimeout  = 30 * time.Second // per message
	listenHost      = "127.0.0.1"      // localhost only
	scanBufCap      = 64 * 1024        // initial scanner buffer capacity
)

func main() {
	if err := run(); err != nil {
		log.New(os.Stderr, "", 0).Fatal(err)
	}
}

func run() error {
	// Flags
	port := flag.Int("port", 5514, "Listen port (localhost only). Example: 5514")
	topic := flag.String("topic", "", "Required: full Pub/Sub topic name (e.g. projects/<PROJECT_ID>/topics/<TOPIC_ID>)")
	httpsProxy := flag.String("https_proxy", "", "Optional: proxy URL (sets HTTPS_PROXY and HTTP_PROXY)")
	gac := flag.String("google_application_credentials", "", "Optional: path to credentials JSON (sets GOOGLE_APPLICATION_CREDENTIALS)")
	flag.Parse()

	errLogger := log.New(os.Stderr, "", 0)
	outLogger := log.New(os.Stdout, "", 0)

	// Validate input
	if *port < 1 || *port > 65535 {
		return fmt.Errorf("invalid port: %d", *port)
	}
	if *topic == "" {
		return errors.New("-topic is required (projects/<PROJECT_ID>/topics/<TOPIC_ID>)")
	}
	if !isFullTopicName(*topic) {
		return fmt.Errorf("-topic must be projects/<PROJECT_ID>/topics/<TOPIC_ID>, got: %s", *topic)
	}

	// Apply optional environment variables before creating the Pub/Sub client.
	// net/http honors HTTP_PROXY/HTTPS_PROXY/NO_PROXY for outgoing requests.
	if *httpsProxy != "" {
		_ = os.Setenv("HTTPS_PROXY", *httpsProxy)
		_ = os.Setenv("HTTP_PROXY", *httpsProxy)
	}
	// ADC prefers GOOGLE_APPLICATION_CREDENTIALS when present.
	if *gac != "" {
		_ = os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", *gac)
	}

	// Root context canceled on SIGINT/SIGTERM
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Create Pub/Sub REST publisher client
	pubClient, err := pubsub.NewPublisherRESTClient(ctx)
	if err != nil {
		return fmt.Errorf("NewPublisherRESTClient: %w", err)
	}
	defer pubClient.Close()

	addr := fmt.Sprintf("%s:%d", listenHost, *port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen error: %w", err)
	}
	defer ln.Close()

	errLogger.Printf("listening on tcp %s (localhost only)", addr)
	errLogger.Printf("publishing to %s", *topic)

	// Close listener on shutdown to unblock Accept()
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	var wg sync.WaitGroup

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break // shutting down
			}
			errLogger.Printf("accept error: %v", err)
			continue
		}

		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			handleConn(ctx, c, pubClient, *topic, outLogger, errLogger)
		}(conn)
	}

	wg.Wait()
	errLogger.Println("shutdown complete")
	return nil
}

func isFullTopicName(topic string) bool {
	return strings.HasPrefix(topic, "projects/") && strings.Contains(topic, "/topics/")
}

func handleConn(
	ctx context.Context,
	conn net.Conn,
	pubClient *pubsub.PublisherClient,
	topic string,
	outLogger *log.Logger,
	errLogger *log.Logger,
) {
	// ctx.Done() でコネクションを閉じて Scan を解除する
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	defer conn.Close()

	sc := bufio.NewScanner(conn)
	sc.Split(bufio.ScanLines)
	sc.Buffer(make([]byte, 0, scanBufCap), scannerMaxBytes)

	for sc.Scan() {
		// Scanner's buffer is reused; copy before publishing.
		line := append([]byte(nil), sc.Bytes()...)

		// Keep behavior similar to the original syslog server: print to stdout.
		outLogger.Println(string(line))

		if err := publishLine(ctx, pubClient, topic, line); err != nil {
			// Do not terminate the connection on publish errors; keep reading.
			errLogger.Printf("publish error: %v", err)
		}
	}

	if err := sc.Err(); err != nil && !errors.Is(err, net.ErrClosed) {
		if errors.Is(err, bufio.ErrTooLong) {
			errLogger.Printf("read error: line too long (max %d bytes): %v", scannerMaxBytes, err)
		} else {
			errLogger.Printf("read error: %v", err)
		}
	}
}

func publishLine(ctx context.Context, c *pubsub.PublisherClient, topic string, data []byte) error {
	pubCtx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()

	req := &pubsubpb.PublishRequest{
		Topic: topic,
		Messages: []*pubsubpb.PubsubMessage{
			{Data: data},
		},
	}

	_, err := c.Publish(pubCtx, req)
	return err
}
