package mgo

//
//import (
//	"context"
//	"github.com/fy403/local-msg-table/domain"
//	"github.com/sirupsen/logrus"
//	"go.mongodb.org/mongo-driver/bson"
//	"go.mongodb.org/mongo-driver/mongo"
//	"go.mongodb.org/mongo-driver/mongo/options"
//	"time"
//)
//
//var (
//	logger = logrus.WithField("module", "MongoDBBaseEventRepository")
//)
//
//// MongoDBBaseEventRepository MongoDB 实现
//type MongoDBBaseEventRepository struct {
//	collection *mongo.Collection
//	session    mongo.Session
//}
//
//// NewMongoDBBaseEventRepository 创建一个新的 MongoDBBaseEventRepository 实例
//func NewMongoDBBaseEventRepository(collection *mongo.Collection) *MongoDBBaseEventRepository {
//	return &MongoDBBaseEventRepository{
//		collection: collection,
//	}
//}
//
//// WithSession 使用外部会话
//func (r *MongoDBBaseEventRepository) WithSession(session mongo.Session) *MongoDBBaseEventRepository {
//	return &MongoDBBaseEventRepository{
//		collection: r.collection,
//		session:    session,
//	}
//}
//
//// InsertEvent 插入事件
//func (r *MongoDBBaseEventRepository) InsertEvent(event *domain.ShieldEvent) (bool, error) {
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//
//	var err error
//	if r.session != nil {
//		err = r.session.StartTransaction(options.Transaction().SetReadPreference(options.ReadPreferencePrimary()))
//		if err != nil {
//			logger.Errorf("Failed to start transaction: %v", err)
//			return false, err
//		}
//		defer r.session.AbortTransaction(ctx)
//	}
//
//	_, err = r.collection.InsertOne(ctx, event)
//	if err != nil {
//		logger.Errorf("Failed to insert event: %v", err)
//		return false, err
//	}
//
//	if r.session != nil {
//		err = r.session.CommitTransaction(ctx)
//		if err != nil {
//			logger.Errorf("Failed to commit transaction: %v", err)
//			return false, err
//		}
//	}
//	return true, nil
//}
//
//// InsertEventWithId 插入事件带主键id
//func (r *MongoDBBaseEventRepository) InsertEventWithId(event *domain.ShieldEvent) (bool, error) {
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//
//	var err error
//	if r.session != nil {
//		err = r.session.StartTransaction(options.Transaction().SetReadPreference(options.ReadPreferencePrimary()))
//		if err != nil {
//			logger.Errorf("Failed to start transaction: %v", err)
//			return false, err
//		}
//		defer r.session.AbortTransaction(ctx)
//	}
//
//	_, err = r.collection.InsertOne(ctx, event)
//	if err != nil {
//		logger.Errorf("Failed to insert event with ID: %v", err)
//		return false, err
//	}
//
//	if r.session != nil {
//		err = r.session.CommitTransaction(ctx)
//		if err != nil {
//			logger.Errorf("Failed to commit transaction: %v", err)
//			return false, err
//		}
//	}
//	return true, nil
//}
//
//// UpdateEventStatusById 更新事件状态
//func (r *MongoDBBaseEventRepository) UpdateEventStatusById(event *domain.ShieldEvent) (bool, error) {
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//
//	filter := bson.M{"id": event.GetID(), "event_status": event.GetBeforeUpdateEventStatus()}
//	update := bson.M{"$set": bson.M{"event_status": event.GetEventStatus()}}
//
//	var err error
//	if r.session != nil {
//		err = r.session.StartTransaction(options.Transaction().SetReadPreference(options.ReadPreferencePrimary()))
//		if err != nil {
//			logger.Errorf("Failed to start transaction: %v", err)
//			return false, err
//		}
//		defer r.session.AbortTransaction(ctx)
//	}
//
//	result, err := r.collection.UpdateOne(ctx, filter, update)
//	if err != nil {
//		logger.Errorf("Failed to update event status: %v", err)
//		return false, err
//	}
//
//	if r.session != nil {
//		err = r.session.CommitTransaction(ctx)
//		if err != nil {
//			logger.Errorf("Failed to commit transaction: %v", err)
//			return false, err
//		}
//	}
//	return result.MatchedCount == 1, nil
//}
//
//// DeleteEventLogicallyById 逻辑删除事件
//func (r *MongoDBBaseEventRepository) DeleteEventLogicallyById(id string) (bool, error) {
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//
//	filter := bson.M{"id": id}
//	update := bson.M{"$set": bson.M{"record_status": 1}}
//
//	var err error
//	if r.session != nil {
//		err = r.session.StartTransaction(options.Transaction().SetReadPreference(options.ReadPreferencePrimary()))
//		if err != nil {
//			logger.Errorf("Failed to start transaction: %v", err)
//			return false, err
//		}
//		defer r.session.AbortTransaction(ctx)
//	}
//
//	result, err := r.collection.UpdateOne(ctx, filter, update)
//	if err != nil {
//		logger.Errorf("Failed to logically delete event: %v", err)
//		return false, err
//	}
//
//	if r.session != nil {
//		err = r.session.CommitTransaction(ctx)
//		if err != nil {
//			logger.Errorf("Failed to commit transaction: %v", err)
//			return false, err
//		}
//	}
//	return result.MatchedCount == 1, nil
//}
//
//// QueryEventListByStatus 根据事件状态获取事件列表
//func (r *MongoDBBaseEventRepository) QueryEventListByStatus(eventStatus string) ([]*domain.ShieldEvent, error) {
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//
//	filter := bson.M{"event_status": eventStatus, "record_status": 0}
//	cursor, err := r.collection.Find(ctx, filter, options.Find().SetLimit(50))
//	if err != nil {
//		logger.Errorf("Failed to query events by status: %v", err)
//		return nil, err
//	}
//	defer cursor.Close(ctx)
//
//	var resultList []*domain.ShieldEvent
//	for cursor.Next(ctx) {
//		var shieldEvent domain.ShieldEvent
//		err := cursor.Decode(&shieldEvent)
//		if err != nil {
//			logger.Errorf("Failed to decode row: %v", err)
//			return nil, err
//		}
//		resultList = append(resultList, &shieldEvent)
//	}
//	if err = cursor.Err(); err != nil {
//		logger.Errorf("Error during iteration: %v", err)
//		return nil, err
//	}
//	return resultList, nil
//}
//
//// QueryEventById 查询事件详情
//func (r *MongoDBBaseEventRepository) QueryEventById(id string) (*domain.ShieldEvent, error) {
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//
//	filter := bson.M{"id": id}
//	var shieldEvent domain.ShieldEvent
//	err := r.collection.FindOne(ctx, filter).Decode(&shieldEvent)
//	if err != nil {
//		if err == mongo.ErrNoDocuments {
//			return nil, nil
//		}
//		logger.Errorf("Failed to query event by ID: %v", err)
//		return nil, err
//	}
//	shieldEvent.SetSuccess(true)
//	return &shieldEvent, nil
//}
//
//// QueryEventByBizkeyCond 查询事件详情
//func (r *MongoDBBaseEventRepository) QueryEventByBizkeyCond(bizKey, txType, appId string) ([]*domain.ShieldEvent, error) {
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//
//	filter := bson.M{
//		"biz_key":      bizKey,
//		"tx_type":      txType,
//		"app_id":       appId,
//		"event_status": bson.M{"$in": []string{"CONSUME_INIT", "CONSUME_PROCESSING", "CONSUME_PROCESSED"}},
//	}
//	cursor, err := r.collection.Find(ctx, filter)
//	if err != nil {
//		logger.Errorf("Failed to query events by bizkey cond: %v", err)
//		return nil, err
//	}
//	defer cursor.Close(ctx)
//
//	var resultList []*domain.ShieldEvent
//	for cursor.Next(ctx) {
//		var shieldEvent domain.ShieldEvent
//		err := cursor.Decode(&shieldEvent)
//		if err != nil {
//			logger.Errorf("Failed to decode row: %v", err)
//			return nil, err
//		}
//		shieldEvent.SetSuccess(true)
//		resultList = append(resultList, &shieldEvent)
//	}
//	if err = cursor.Err(); err != nil {
//		logger.Errorf("Error during iteration: %v", err)
//		return nil, err
//	}
//	return resultList, nil
//}
