package main

import (
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	lmt "github.com/fy403/local-msg-table"
	"github.com/fy403/local-msg-table/constant"
	"github.com/fy403/local-msg-table/domain"
	"github.com/fy403/local-msg-table/schedule"
	"github.com/google/uuid"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"strconv"
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
		gDB, err = gorm.Open(mysql.Open("root:123456@tcp(127.0.0.1:3306)/test_db?charset=utf8mb4&parseTime=True&loc=Local"), &gorm.Config{
			Logger: newLogger,
		})
		if err != nil {
			log.Fatalf("Failed to connect to database: %v", err)
		}
		// 获取底层数据库连接池
		sqlDB, err := gDB.DB()
		if err != nil {
			log.Fatalf("Failed to get database connection pool: %v", err)
		}
		// 设置连接池参数
		sqlDB.SetMaxIdleConns(3)            // 最大空闲连接数
		sqlDB.SetMaxOpenConns(5)            // 最大打开连接数
		sqlDB.SetConnMaxLifetime(time.Hour) // 连接的最大存活时间
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
	for i := 0; i < 12000; i++ {
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
		if err := scheduler.PutMessage(tx, "appid", constant.COMMIT, uuid.New().String(), strconv.Itoa(i)); err != nil {
			tx.Rollback()
			return
		}
		// 提交事务
		tx.Commit()
		time.Sleep(time.Second)
	}
	select {}
}
