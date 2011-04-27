package streamplay

import (
	"log"
	"streamplay/status"
)

func Main() {
	log.Println("Initiating streamplay core")
	status.Start(":10124")
	Load("test pipe", map[string]string{"path": "/tmp/golist-1", "address": ":8081"})
}
