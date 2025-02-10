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
	"gorm.io/gorm/logger"
	"log"
	"math/rand"
	"sync"
	"time"
)

var gDB *gorm.DB
var gScheduler *schedule.SendTxcMessageScheduler
var onceDB sync.Once
var onceScheduler sync.Once

func getDB() *gorm.DB {
	onceDB.Do(func() {
		var err error
		newLogger := logger.New(
			log.New(log.Writer(), "\r\n", log.LstdFlags), // io writer
			logger.Config{
				SlowThreshold:             time.Second,  // 慢 SQL 阈值
				LogLevel:                  logger.Error, // 日志级别
				IgnoreRecordNotFoundError: true,         // 忽略ErrRecordNotFound（记录未找到）错误
				Colorful:                  true,         // 启用彩色打印
			},
		)
		gDB, err = gorm.Open(mysql.Open("root:123456@tcp(127.0.0.1:3306)/test_db2?charset=utf8mb4&parseTime=True&loc=Local"), &gorm.Config{
			Logger: newLogger,
		})
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
		gScheduler.RegisterCommitListenerWithRocketMQ(props.RocketMQConfig.TopicSource, props.RocketMQConfig.NameSrvAddr, func(msg *domain.ShieldTxcMessage) consumer.ConsumeResult {
			//return consumer.ConsumeRetryLater
			if rand.Int()%2 == 0 {
				return consumer.ConsumeRetryLater
			}
			fmt.Printf("Received commit message: %s\n", msg.EventID)
			return consumer.ConsumeSuccess
		})
	})
	return gScheduler
}

func main() {
	// 初始化数据库连接
	db := getDB()
	scheduler := getScheduler(db)
	scheduler.Schedule()
	select {}
}
