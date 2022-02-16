package zinxclient

import (
	"context"
	"encoding/json"
	"github.com/aceld/zinx/zlog"
	"github.com/joway/pond"
	"time"
)

type ClientManager struct {
	Host    string
	Network string
	Timeout time.Duration
	Pool    *pond.Pool // 连接池
}

type Config struct {
	Host                string
	Network             string
	Timeout             time.Duration
	MinIdle             int
	MaxIdle             int
	MaxSize             int
	MaxValidateAttempts int
}

func NewClientManager(config Config) *ClientManager {
	pool, err := GenPool(config)
	if err != nil {
		panic(err)
	}
	return &ClientManager{Host: config.Host, Pool: pool, Network: config.Network, Timeout: config.Timeout}
}

func (manager ClientManager) SendObject(msgId uint32, object interface{}) ([]byte, error) {
	if marshal, err := json.Marshal(object); err == nil {
		return manager.Send(msgId, marshal)
	} else {
		return nil, err
	}
}

func (manager ClientManager) Size() int {
	return manager.Pool.Size()
}

func (manager ClientManager) Send(msgId uint32, data []byte) ([]byte, error) {
	ctx := context.Background()
	object, err := manager.Pool.BorrowObject(ctx)
	defer func(Pool *pond.Pool, ctx context.Context, object interface{}) {
		_ = Pool.ReturnObject(ctx, object)
	}(manager.Pool, ctx, object)
	if err != nil {
		return nil, err
	}

	//
	client := object.(*Client)
	mess := make(chan byte)
	res, err := client.SendByEvent(msgId, data, func() {
		go heartBeating(manager.Pool, ctx, object, mess, manager.Timeout)
	}, func() {
		mess <- byte(0)
	})
	if err != nil {
		_ = manager.Pool.InvalidateObject(ctx, object)
		return nil, err
	}

	return res, nil
}

// HeartBeating 心跳计时，根据GravelChannel判断Client是否在设定时间内发来信息
func heartBeating(pool *pond.Pool, ctx context.Context, object interface{}, readerChannel chan byte, timeout time.Duration) {
	select {
	case _ = <-readerChannel:
		break
	case <-time.After(timeout):
		zlog.Info("It's really weird to get Nothing!!!")
		_ = pool.InvalidateObject(ctx, object)
	}
}
