package zinxclient

import (
	"context"
	"github.com/aceld/zinx/ziface"
	"github.com/aceld/zinx/zlog"
	"github.com/aceld/zinx/znet"
	"github.com/joway/pond"
	"log"
	"net"
)

// WritePacket 向socket写入数据
func WritePacket(msg ziface.IMessage, conn net.Conn) error {
	dp := znet.NewDataPack()
	packet, err := dp.Pack(msg)
	if err != nil {
		return err
	}
	_, err = conn.Write(packet)
	if err != nil {
		zlog.Warn("write error err ", err)
		return err
	}
	return nil
}

func GenPool(config Config) (*pond.Pool, error) {
	cfg := pond.NewDefaultConfig()
	if config.MinIdle == 0 {
		config.MinIdle = 1
	}
	if config.MaxIdle == 0 {
		config.MaxIdle = 10
	}
	if config.MaxSize == 0 {
		config.MaxSize = 10
	}
	if config.MaxValidateAttempts == 0 {
		config.MaxValidateAttempts = 1
	}
	cfg.MinIdle = config.MinIdle
	cfg.MaxIdle = config.MaxIdle
	cfg.MaxValidateAttempts = config.MaxValidateAttempts
	cfg.MaxSize = config.MaxSize
	cfg.ObjectCreateFactory = func(ctx context.Context) (interface{}, error) {
		log.Println("create client....")
		return NewClient(config.Host, config.Network)
	}

	cfg.ObjectDestroyFactory = func(ctx context.Context, object interface{}) error {
		c := object.(*Client)
		c.Close()
		return nil
	}

	return pond.New(cfg)
}
