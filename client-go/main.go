// Client Go pentru agentul de mesagerie.
//
//	go run . -mode receive -name go-recv -format xml -topics "news.*,alerts"
//	go run . -mode send -name go-sender -format json -topic news.tech -message "salut" -count 5
package main

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const maxFrame = 1 << 20

type Client struct {
	conn    net.Conn
	adapter MessageAdapter
	wmu     sync.Mutex
}

func (c *Client) Send(m *Message) error {
	body, err := c.adapter.Serialize(m)
	if err != nil {
		return err
	}
	frame := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(frame, uint32(len(body))) // prefix de lungime
	copy(frame[4:], body)
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err = c.conn.Write(frame)
	return err
}

func (c *Client) Receive() (*Message, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(c.conn, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > maxFrame {
		return nil, fmt.Errorf("cadru prea mare: %d", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(c.conn, body); err != nil {
		return nil, err
	}
	a, err := DetectAdapter(body)
	if err != nil {
		return nil, err
	}
	return a.Deserialize(body)
}

func sum(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }

func uuid() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6], b[8] = (b[6]&0x0f)|0x40, (b[8]&0x3f)|0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func main() {
	mode := flag.String("mode", "receive", "send | receive")
	host := flag.String("host", "127.0.0.1:5000", "adresa brokerului")
	name := flag.String("name", "", "numele clientului")
	format := flag.String("format", "json", "json | xml")
	topic := flag.String("topic", "news.general", "subiect pentru publicare")
	topics := flag.String("topics", "#", "abonări, separate prin virgulă")
	message := flag.String("message", "", "text de trimis (gol = interactiv)")
	count := flag.Int("count", 1, "de câte ori se trimite mesajul")
	interval := flag.Duration("interval", 300*time.Millisecond, "pauză între mesaje")
	noAck := flag.Bool("no-ack", false, "nu confirma mesajele (demonstrează retransmiterea)")
	flag.Parse()

	if *name == "" {
		*name = "go-" + *mode + "er"
	}
	adapter, err := AdapterFor(*format)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	conn, err := net.Dial("tcp", *host)
	if err != nil {
		fmt.Fprintln(os.Stderr, "nu mă pot conecta la broker:", err)
		os.Exit(1)
	}
	c := &Client{conn: conn, adapter: adapter}
	role := "receiver"
	if *mode == "send" {
		role = "sender"
	}
	c.Send(&Message{Type: "connect", Sender: *name, Format: adapter.Name(), Role: role})
	if m, err := c.Receive(); err != nil || m.Type != "connack" {
		fmt.Fprintln(os.Stderr, "conectare refuzată:", err, m)
		os.Exit(2)
	}
	fmt.Printf("[go] conectat ca %s, format %s\n", *name, strings.ToUpper(adapter.Name()))

	done := make(chan struct{})
	go func() { // goroutine de citire: nu blochează trimiterea
		defer close(done)
		for {
			m, err := c.Receive()
			if err != nil {
				fmt.Println("[go] conexiune închisă:", err)
				return
			}
			switch m.Type {
			case "deliver":
				ok := "✓ integru"
				if sum(m.Payload) != m.Checksum {
					ok = "✗ ALTERAT"
				}
				fmt.Printf("[go] ← %s de la %s (trimis ca %s, încercarea %s) %s\n       %s\n",
					m.Topic, m.Sender, strings.ToUpper(m.Format), m.Attempt, ok, m.Payload)
				if !*noAck {
					c.Send(&Message{Type: "ack", Ref: m.ID})
				}
			case "puback":
				fmt.Printf("[go] ✓ publicat %.8s pe %s: %s\n", m.Ref, m.Topic, m.Payload)
			case "suback":
				fmt.Printf("[go] abonat la %s\n", m.Topic)
			case "error":
				fmt.Printf("[go] eroare broker: %s\n", m.Error)
			}
		}
	}()

	publish := func(t, p string) {
		c.Send(&Message{Type: "publish", ID: uuid(), Topic: t, Payload: p, Checksum: sum(p),
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano)})
	}

	if *mode == "send" {
		if *message != "" {
			for i := 1; i <= *count; i++ {
				p := *message
				if *count > 1 {
					p = fmt.Sprintf("%s #%d", p, i)
				}
				publish(*topic, p)
				time.Sleep(*interval)
			}
			time.Sleep(500 * time.Millisecond)
			return
		}
		fmt.Println("[go] scrie mesaje (Enter = trimite). Prefix „@topic ” pentru alt subiect.")
		sc := bufio.NewScanner(os.Stdin)
		t := *topic
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			if strings.HasPrefix(line, "@") && strings.Contains(line, " ") {
				i := strings.Index(line, " ")
				t, line = line[1:i], line[i+1:]
			}
			publish(t, line)
		}
		time.Sleep(300 * time.Millisecond)
		return
	}
	for _, t := range strings.Split(*topics, ",") {
		c.Send(&Message{Type: "subscribe", ID: uuid(), Topic: strings.TrimSpace(t)})
	}
	<-done
}
