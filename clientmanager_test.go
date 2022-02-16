package zinxclient

import (
	"log"
	"strconv"
	"testing"
	"time"
)

func TestClientManager_SendObject(t *testing.T) {
	config := Config{
		Host:    "127.0.0.1:6667",
		Network: "tcp",
		Timeout: time.Second * 5,
	}
	manager := NewClientManager(config)
	//manager := NewClientManager("127.0.0.1:6667", "tcp", time.Second*1, 2, 10, 10, 1)
	/*res, err := manager.Send(1, []byte("ping"))
	if err != nil {
		return
	}
	log.Println("============================")
	log.Println(string(res))*/

	for {
		time.Sleep(time.Second * 2)
		send, err := manager.Send(1, []byte("ping"))
		log.Println("err = ", err)
		log.Println(string(send))
	}
}

func TestClientManagerForHashRing_SendAuto(t *testing.T) {
	config := Config{
		Timeout: time.Second * 5,
	}
	hashRing := NewClientManagerForHashRing("1#127.0.0.1:6667#10,2#127.0.0.1:6668#8", config)
	for i := 0; i < 100; i++ {
		if i == 5 {
			hashRing.CloseNode("1")
		}
		if i == 30 {
			hashRing.RecoverNode("1")
		}
		node := hashRing.GetNode(strconv.Itoa(i))
		log.Println(i, ": node = ", node)
	}

}
