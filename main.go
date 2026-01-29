package main

import (
	"bytes"
	"log"
	"net/http"
	"time"
	
	"github.com/go-resty/resty/v2"
	"github.com/gorilla/websocket"
)

const (
	wsURL  = "wss://app.send.tg/internal/v1/p2c-socket/?EIO=4&transport=websocket"
	apiURL = "https://app.send.tg/internal/v1/p2c/payments/take/"
	cookie = "access_token=TOKEN"
)



func takePayment(id string) {
  client := resty.New().
    EnableTrace()

  start := time.Now()
  resp, err := client.R().
    SetHeader("Cookie", cookie).
    SetHeader("Content-Type", "application/json").
    SetBody([]byte("{}")).
    Post("https://app.send.tg/internal/v1/p2c/payments/take/" + id)
  if err != nil {
    log.Printf("ERR: Post %s: %v", id, err)

    return
  }

  ti := resp.Request.TraceInfo()

  log.Printf(
    "PAYMENT: %s | STATUS: %d | TIME: %v | DNS: %v | TCP: %v | TLS: %v | SERVER: %v | TOTAL: %v",
    id,
    resp.StatusCode(),
    time.Since(start),
    ti.DNSLookup,
    ti.TCPConnTime,
    ti.TLSHandshake,
    ti.ServerTime,
    ti.TotalTime,
  )
}


func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	wsHeader := http.Header{
		"Cookie": []string{cookie},
		"Origin": []string{"https://app.send.tg"},
	}

	ws, _, err := websocket.DefaultDialer.Dial(wsURL, wsHeader)
	if err != nil {
		log.Fatal("Dial error:", err)
	}
	defer ws.Close()

	log.Println("SNIPER READY | NEXTREADER MODE")

	for {
		messageType, reader, err := ws.NextReader()
		if err != nil {
			log.Println("WS Closed:", err)
			return
		}
		if messageType != websocket.TextMessage {
			continue
		}

		buf := make([]byte, 512)
		n, _ := reader.Read(buf)
		if n == 0 {
			continue
		}

		
		switch buf[0] {
		case '2': 
			ws.WriteMessage(websocket.TextMessage, []byte("3"))
			continue
		case '0':
			ws.WriteMessage(websocket.TextMessage, []byte("40"))
			time.AfterFunc(100*time.Millisecond, func() {
				ws.WriteMessage(websocket.TextMessage, []byte(`42["list:initialize"]`))
			})
			continue
		case '4':
			if bytes.Contains(buf[:n], []byte(`"add"`)) {
				if idIdx := bytes.Index(buf[:n], []byte(`"id":"`)); idIdx != -1 {
					
					endIdx := bytes.Index(buf[idIdx+6:n], []byte(`"`))
					if endIdx != -1 {
						id := string(buf[idIdx+6 : idIdx+6+endIdx])
						go takePayment(id)
					}
				}
			}
		}
	}
}