package main

import (
	"fmt"
	"github.com/fy403/local-msg-table/domain"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"time"
)

func getDB(dsn string) *gorm.DB {
	var err error
	newLogger := logger.New(
		log.New(log.Writer(), "\r \n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             time.Second,  // 慢 SQL 阈值
			LogLevel:                  logger.Error, // 日志级别
			IgnoreRecordNotFoundError: true,         // 忽略ErrRecordNotFound（记录未找到）错误
			Colorful:                  true,         // 启用彩色打印
		},
	)
	gDB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: newLogger,
	})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
		return nil
	}
	// 获取底层数据库连接池
	sqlDB, err := gDB.DB()
	if err != nil {
		log.Fatalf("Failed to get database connection pool: %v", err)
	}
	// 设置连接池参数
	sqlDB.SetMaxIdleConns(3)                   // 最大空闲连接数
	sqlDB.SetMaxOpenConns(5)                   // 最大打开连接数
	sqlDB.SetConnMaxLifetime(time.Second * 90) // 连接的最大存活时间
	return gDB
}

func reconcileEvents(db1, db2 *gorm.DB, timeThreshold time.Time) {
	// Step 1: 查询 db1 中已 Commit 的消息状态是否为 PRODUCE_PROCESSED，并且创建时间在指定时间之前
	var committedEvents []domain.ShieldEvent
	db1.Where("tx_type = ? AND event_status = ? AND record_status = 0 AND gmt_create < ?", "COMMIT", "PRODUCE_PROCESSED", timeThreshold).Find(&committedEvents)

	for _, event := range committedEvents {
		// Step 2: 检查这些消息是否在 db2 中存在
		var db2Event domain.ShieldEvent
		result := db2.Where("event_id = ? AND record_status = 0", event.EventID).First(&db2Event)
		if result.Error != nil {
			if result.Error == gorm.ErrRecordNotFound {
				fmt.Printf("Event ID %s not found in db2 \n", event.EventID)
			} else {
				log.Printf("Error querying db2 for event ID %s: %v \n", event.EventID, result.Error)
			}
			continue
		}

		// Step 3: 检查 db2 中状态为 CONSUME_MAX_RECONSUMETIMES 的消息是否在 db2 中存在一条对应的 ROLLBACK 记录
		if db2Event.EventStatus == "CONSUME_MAX_RECONSUMETIMES" {
			var rollbackEvent domain.ShieldEvent
			result := db2.Where(" tx_type = ? AND event_id = ? AND record_status = 0", "ROLLBACK", event.EventID).First(&rollbackEvent)
			if result.Error != nil {
				if result.Error == gorm.ErrRecordNotFound {
					fmt.Printf("ROLLBACK event for Event ID %s not found in db2 \n", event.EventID)
				} else {
					log.Printf("Error querying db2 for ROLLBACK event ID %s: %v \n", event.EventID, result.Error)
				}
			}

			// Step 4: 检查 db1 中是否存在一条对应的 ROLLBACK 记录
			var rollbackEvent2 domain.ShieldEvent
			result = db1.Where(" tx_type = ? AND event_id = ? AND record_status = 0", "ROLLBACK", event.EventID).First(&rollbackEvent2)
			if result.Error != nil {
				if result.Error == gorm.ErrRecordNotFound {
					fmt.Printf("ROLLBACK event for Event ID %s not found in db1 \n", event.EventID)
				} else {
					log.Printf("Error querying db1 for ROLLBACK event ID %s: %v \n", event.EventID, result.Error)
				}
			}
		}
	}
	log.Println("End a scan check")
}

func checkAndReconnect(db *gorm.DB, dsn string) *gorm.DB {
	sqlDB, err := db.DB()
	if err != nil {
		//log.Printf("Failed to get database connection: %v", err)
		return getDB(dsn)
	}

	// 检查连接是否有效
	if err := sqlDB.Ping(); err != nil {
		//log.Printf("Connection is invalid, reconnecting: %v", err)
		return getDB(dsn)
	}

	return db
}

func main() {
	source := "root:123456@tcp(127.0.0.1:3306)/test_db?charset=utf8mb4&parseTime=True&loc=Local"
	destination := "root:123456@tcp(127.0.0.1:3306)/test_db2?charset=utf8mb4&parseTime=True&loc=Local"
	db1 := getDB(source)
	db2 := getDB(destination)

	ticker := time.NewTicker(3 * time.Minute)
	defer ticker.Stop()
	timeThreshold := time.Now().Add(-1 * time.Minute)
	reconcileEvents(db1, db2, timeThreshold)
	for {
		select {
		case <-ticker.C:
			// 检查并重新连接
			db1 = checkAndReconnect(db1, source)
			db2 = checkAndReconnect(db2, destination)

			timeThreshold := time.Now().Add(-1 * time.Minute)
			reconcileEvents(db1, db2, timeThreshold)
		}
	}
}
