package main

import (
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	lmt "github.com/fy403/local-msg-table"
	"github.com/fy403/local-msg-table/constant"
	"github.com/fy403/local-msg-table/domain"
	"github.com/fy403/local-msg-table/schedule"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
	"strconv"
	"sync"
)

var gDB *gorm.DB
var gScheduler *schedule.SendTxcMessageScheduler
var onceDB sync.Once
var onceScheduler sync.Once

func getDB() *gorm.DB {
	onceDB.Do(func() {
		var err error
		gDB, err = gorm.Open(mysql.Open("root:123456@tcp(127.0.0.1:3306)/test_db?charset=utf8mb4&parseTime=True&loc=Local"), &gorm.Config{})
		if err != nil {
			log.Fatalf("Failed to connect to database: %v", err)
		}
	})
	return gDB
}

func getScheduler(db *gorm.DB) *schedule.SendTxcMessageScheduler {
	onceScheduler.Do(func() {
		/// 配置Properties
		props := lmt.NewProperties().
			SetRetryTimesWhenSendFailed(3).
			SetTranMessageSendPeriod(3).
			SetEventStoreType(constant.MYSQL).
			SetMessageQueueType(constant.ROCKETMQ).
			SetRocketMQConfig(lmt.RocketMQConfig{
				NameSrvAddr: "127.0.0.1:9876",
				TopicSource: "AAA",
			})
		var err error
		gScheduler, err = props.Init(db)
		if err != nil {
			log.Fatalf("Failed to initialize gScheduler: %v", err)
		}
		gScheduler.RegisterRollbackListenerWithRocketMQ(props.RocketMQConfig.TopicSource, props.RocketMQConfig.NameSrvAddr, func(msg *domain.ShieldTxcMessage) consumer.ConsumeResult {
			fmt.Printf("Received rollback message: %s", msg.EventID)
			fmt.Println("++++++++++++++++++++++++++++++++++++++")
			return consumer.ConsumeSuccess
		})

	})
	return gScheduler
}

func main() {
	db := getDB()
	scheduler := getScheduler(db)
	scheduler.Schedule()
	for i := 0; i < 100; i++ {
		// 初始化数据库连接
		tx := db.Begin()
		if tx.Error != nil {
			// 处理事务开启失败的情况
			return
		}
		defer func() {
			if r := recover(); r != nil {
				tx.Rollback()
			}
		}()

		// 业务代码
		// 使用 repo 进行数据库操作
		msg := domain.NewShieldTxcMessage()
		msg.Content = strconv.Itoa(i)
		if err := scheduler.PutMessage(msg, msg.EventID, constant.INSERT, constant.COMMIT, "appId"); err != nil {
			tx.Rollback()
			return
		}
		// 提交事务
		tx.Commit()
	}
	select {}
}
