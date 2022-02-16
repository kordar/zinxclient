package zinxclient

import (
	"encoding/json"
	"errors"
	"github.com/g4zhuj/hashring"
	"strconv"
	"strings"
	"sync"
)

type ClientManagerForHashRing struct {
	Manager  map[string]*ClientManager
	HashRing *hashring.HashRing
	// 如果结点离线，则更新权重值为0，等待再次连通后恢复该结点权重值
	FixedWeight  map[string]int
	ClosedWeight map[string]bool
	mu           sync.RWMutex
}

// NewClientManagerForHashRing 基于一致性hash的客户端管理器
// config : nodeId#ip:port
func NewClientManagerForHashRing(cfgStr string, config Config) *ClientManagerForHashRing {
	// 解析结点配置
	split := strings.Split(cfgStr, ",")
	newHashRing := hashring.NewHashRing(0)
	weights := make(map[string]int, len(split))
	manager := make(map[string]*ClientManager, len(split))
	for _, s := range split {
		n := strings.Split(s, "#")
		if len(n) >= 2 {
			config := Config{
				Host:                n[1],
				Network:             config.Network,
				Timeout:             config.Timeout,
				MinIdle:             config.MinIdle,
				MaxIdle:             config.MaxIdle,
				MaxSize:             config.MaxSize,
				MaxValidateAttempts: config.MaxValidateAttempts,
			}
			manager[n[0]] = NewClientManager(config)

			if weight, err := strconv.Atoi(n[2]); err != nil {
				newHashRing.AddNode(n[0], 0)
				weights[n[0]] = 0
			} else {
				newHashRing.AddNode(n[0], weight) // 自动权重值初始均为1，后续根据结点进行调整
				weights[n[0]] = weight
			}
		}
	}

	return &ClientManagerForHashRing{
		Manager:      manager,
		HashRing:     newHashRing,
		FixedWeight:  weights,
		ClosedWeight: make(map[string]bool, len(split)),
		mu:           sync.RWMutex{},
	}
}

func (hashRingMgr *ClientManagerForHashRing) GetClientManager(id string) (*ClientManager, error) {
	clientManager := hashRingMgr.Manager[id]
	if clientManager == nil {
		return nil, errors.New("not found client hashRingMgr")
	}
	return clientManager, nil
}

func (hashRingMgr *ClientManagerForHashRing) SendObject(id string, msgId uint32, object interface{}) ([]byte, error) {
	if marshal, err := json.Marshal(object); err == nil {
		return hashRingMgr.Send(id, msgId, marshal)
	} else {
		return nil, err
	}
}

func (hashRingMgr *ClientManagerForHashRing) Size(id string) int {
	clientManager, err := hashRingMgr.GetClientManager(id)
	if err != nil {
		return 0
	}
	return clientManager.Size()
}

func (hashRingMgr *ClientManagerForHashRing) GetNode(key string) string {
	return hashRingMgr.HashRing.GetNode(key)
}

func (hashRingMgr *ClientManagerForHashRing) CloseNode(id string) {
	hashRingMgr.mu.Lock()
	defer hashRingMgr.mu.Unlock()
	hashRingMgr.HashRing.UpdateNode(id, 0)
	hashRingMgr.ClosedWeight[id] = true
}

func (hashRingMgr *ClientManagerForHashRing) RecoverNode(id string) {
	hashRingMgr.mu.Lock()
	defer hashRingMgr.mu.Unlock()
	hashRingMgr.HashRing.UpdateNode(id, hashRingMgr.FixedWeight[id])
	hashRingMgr.ClosedWeight[id] = false
}

func (hashRingMgr *ClientManagerForHashRing) Send(id string, msgId uint32, data []byte) ([]byte, error) {
	clientManager, err := hashRingMgr.GetClientManager(id)
	if err != nil {
		return nil, err
	}
	// 发送消息失败，需要对hash值进行更新
	send, err := clientManager.Send(msgId, data)
	if err != nil {
		hashRingMgr.CloseNode(id)
	}
	return send, nil
}
