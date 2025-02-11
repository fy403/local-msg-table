package rocketmq

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/fy403/local-msg-table/constant"
	"github.com/fy403/local-msg-table/domain"
	"github.com/fy403/local-msg-table/event"
)

// ShieldTxcCommitListener defines the Shield domain commit listener.
type ShieldTxcCommitListener struct {
	txCommitFunc     func(*domain.ShieldTxcMessage) consumer.ConsumeResult
	baseEventService event.BaseEventService
}

// NewShieldTxcCommitListener creates a new ShieldTxcCommitListener instance.
func NewShieldTxcCommitListener(txCommitFunc func(*domain.ShieldTxcMessage) consumer.ConsumeResult, baseEventService event.BaseEventService) *ShieldTxcCommitListener {
	return &ShieldTxcCommitListener{
		txCommitFunc:     txCommitFunc,
		baseEventService: baseEventService,
	}
}

// ConsumeMessage processes the consumed messages.
func (l *ShieldTxcCommitListener) ConsumeMessage() func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	return func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			msgBody := string(msg.Body)
			msgID := msg.MsgId
			logger.Debugf("[ShieldTxcCommitListener] Consuming [COMMIT] Message start... msgId=%s, msgBody=%s", msgID, msgBody)

			shieldTxcMessage := &domain.ShieldTxcMessage{}
			err := shieldTxcMessage.Decode(msgBody)
			if err != nil {
				logger.Errorf("Failed to decode message: %v", err)
				return consumer.ConsumeRetryLater, err
			}
			baseEventService := l.baseEventService

			event := &domain.ShieldEvent{}
			event.Convert(shieldTxcMessage)
			// Handle reconsume threshold.
			currReconsumeTimes := msg.ReconsumeTimes
			if currReconsumeTimes >= constant.MAX_COMMIT_RECONSUME_TIMES {
				logger.Debugf("[ShieldTxcCommitListener] START transaction rollback sequence! msgId=%s, currReconsumeTimes=%d", msgID, currReconsumeTimes)
				if l.doPutRollbackMsgAfterMaxConsumeTimes(baseEventService, event, msgID) {
					logger.Debugf("[ShieldTxcCommitListener] transaction rollback sequence executed! msgId=%s", msgID)
					return consumer.ConsumeSuccess, nil
				} else {
					return consumer.ConsumeRetryLater, nil
				}
			}
			event.SetEventStatus(constant.CONSUME_INIT)
			queryEvent, err := baseEventService.QueryEventById(event)
			if err != nil {
				logger.Warnf("[ShieldTxcCommitListener] Query CommitShieldEvent Message failed, msgId=%s", msgID)
				return consumer.ConsumeRetryLater, err
			}
			if queryEvent == nil {
				_, err = baseEventService.InsertEventWithId(event)
				if err != nil {
					logger.Warnf("[ShieldTxcCommitListener] InsertEventWithId CommitShieldEvent Message failed, msgId=%s", msgID)
					return consumer.ConsumeRetryLater, err
				}
			} else {
				if queryEvent.EventStatus != constant.CONSUME_FAILED {
					return consumer.ConsumeSuccess, nil
				}
			}
			// Update the status to "processing".
			l.doUpdateMessageStatusProcessing(event)

			// Real consumption using the provided function.
			consumeResult := l.txCommitFunc(shieldTxcMessage)
			//l.doUpdateAfterConsumed(baseEventService, consumeResult, event)
			return l.doUpdateAfterConsumed(consumeResult, event), nil
		}
		return consumer.ConsumeSuccess, nil
	}
}

// doPutRollbackMsgAfterMaxConsumeTimes handles rollback for messages exceeding the retry limit.
func (l *ShieldTxcCommitListener) doPutRollbackMsgAfterMaxConsumeTimes(baseEventService event.BaseEventService, shieldEvent *domain.ShieldEvent, msgID string) bool {
	shieldEvent.SetBeforeUpdateEventStatus(constant.CONSUME_PROCESSING)
	shieldEvent.SetEventStatus(constant.CONSUME_MAX_RECONSUMETIMES)
	// Insert rollback message.
	rollbackEvent := &domain.ShieldEvent{}
	rollbackEvent.SetEventID(shieldEvent.GetEventID()).
		SetTxType(constant.GetRollback()).
		SetEventStatus(constant.PRODUCE_INIT).
		SetContent(shieldEvent.GetContent()).
		SetAppID(shieldEvent.GetAppID())
	// 是否已经回滚
	queryEvent, err := baseEventService.QueryEventById(rollbackEvent)
	if err != nil {
		logger.Errorf("Failed to query rollback event: %v", err)
		return false
	}
	if queryEvent != nil {
		return true
	}
	// 更新状态
	updateBefore, err := baseEventService.UpdateEventStatusById(shieldEvent)
	logger.Debugf("[ShieldTxcCommitListener::UPDATE TO CONSUME_MAX_RECONSUMETIMES] {}, msgId={}, updateResult:[%v]", updateBefore, msgID, updateBefore)
	if !updateBefore || err != nil {
		return false
	}

	insertResult, err := baseEventService.InsertEventWithId(rollbackEvent)
	logger.Debugf("[ShieldTxcCommitListener::INSERT ROLLBACK MESSAGE] {}, msgId={}", insertResult, msgID)
	if !insertResult || err != nil {
		return false
	}
	return true
}

// doUpdateAfterConsumed updates the message status after consumption based on the result.
func (l *ShieldTxcCommitListener) doUpdateAfterConsumed(consumeResult consumer.ConsumeResult, shieldEvent *domain.ShieldEvent) consumer.ConsumeResult {
	baseEventService := l.baseEventService
	logger.Debugf("[ShieldTxcCommitListener::doUpdateAfterConsumed] The Real ConsumeConcurrentlyStatus is : [%s]", consumeResult)
	if consumeResult >= consumer.ConsumeRetryLater {
		shieldEvent.SetBeforeUpdateEventStatus(shieldEvent.GetEventStatus())
		shieldEvent.SetEventStatus(constant.CONSUME_FAILED)
		updateBefore, err := baseEventService.UpdateEventStatusById(shieldEvent)
		if !updateBefore || err != nil {
			return consumer.ConsumeRetryLater
		}
		return consumeResult
	}
	if consumeResult == consumer.ConsumeSuccess {
		shieldEvent.SetBeforeUpdateEventStatus(shieldEvent.GetEventStatus())
		shieldEvent.SetEventStatus(constant.CONSUME_PROCESSED)
		updateBefore, err := baseEventService.UpdateEventStatusById(shieldEvent)
		if !updateBefore || err != nil {
			return consumer.ConsumeRetryLater
		}
	}
	return consumer.ConsumeSuccess
}

// doUpdateMessageStatusProcessing updates the message status to "processing".
func (l *ShieldTxcCommitListener) doUpdateMessageStatusProcessing(shieldEvent *domain.ShieldEvent) {
	baseEventService := l.baseEventService
	shieldEvent.SetBeforeUpdateEventStatus(shieldEvent.GetEventStatus())
	shieldEvent.SetEventStatus(constant.CONSUME_PROCESSING)
	updateBefore, err := baseEventService.UpdateEventStatusById(shieldEvent)
	if !updateBefore || err != nil {
		return
	}
}
