package zinxclient

import (
	"errors"
	"github.com/aceld/zinx/zlog"
	"github.com/aceld/zinx/znet"
	"io"
	"log"
	"net"
)

type Client struct {
	conn net.Conn
}

func NewClient(host string, network string) (*Client, error) {
	conn, err := net.Dial(network, host)
	if err != nil {
		zlog.Error("client start err, exit!", err)
		return nil, errors.New("client start err, exit")
	}
	// 客户端
	client := Client{conn: conn}
	// 创建是否自动校验连接心跳
	return &client, nil
}

func (client Client) Close() {
	if client.conn != nil {
		err := client.conn.Close()
		if err != nil {
			log.Println("client close err = ", err)
			return
		}
	}
}

func (client Client) Send(msgId uint32, data []byte) ([]byte, error) {
	return client.SendByEvent(msgId, data, func() {}, func() {})
}

func (client Client) SendByEvent(msgId uint32, data []byte, writeFinish func(), readFinish func()) ([]byte, error) {
	// 发封包message消息
	dp := znet.NewDataPack()
	err := WritePacket(znet.NewMsgPackage(msgId, data), client.conn)
	if err != nil {
		return nil, err
	}

	// TODO 发送完成
	writeFinish()

	// 先读出流中的head部分
	headData := make([]byte, dp.GetHeadLen())
	_, err = io.ReadFull(client.conn, headData) // ReadFull 会把msg填充满为止
	if err != nil {
		zlog.Warn("read head error")
		return nil, err
	}

	// TODO 接收完成
	readFinish()

	// 将headData字节流 拆包到msg中
	msgHead, err := dp.Unpack(headData)
	if err != nil {
		zlog.Warn("server unpack error", err)
		return nil, err
	}

	if msgHead.GetDataLen() > 0 {
		// msg 是有data数据的，需要再次读取data数据
		msg := msgHead.(*znet.Message)
		msg.Data = make([]byte, msg.GetDataLen())

		// 根据dataLen从io中读取字节流
		_, err := io.ReadFull(client.conn, msg.Data)
		if err != nil {
			zlog.Warn("server unpack data err:", err)
			return nil, err
		}

		return msg.Data, nil
	}

	zlog.Warn("message error")
	return nil, errors.New("message error")
}
