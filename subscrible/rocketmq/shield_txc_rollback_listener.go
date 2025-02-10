package rocketmq

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/fy403/local-msg-table/constant"
	"github.com/fy403/local-msg-table/domain"
	"github.com/fy403/local-msg-table/event"
	"github.com/sirupsen/logrus"
	"strings"
)

var (
	logger = logrus.WithField("module", "ShieldTxcRollbackListener")
)

// ShieldTxcRollbackListener defines the Shield TXC rollback listener.
type ShieldTxcRollbackListener struct {
	baseEventService event.BaseEventService
	txRollbackFunc   func(*domain.ShieldTxcMessage) consumer.ConsumeResult
}

// NewShieldTxcRollbackListener creates a new instance of ShieldTxcRollbackListener.
func NewShieldTxcRollbackListener(txRollbackFunc func(*domain.ShieldTxcMessage) consumer.ConsumeResult, baseEventService event.BaseEventService) *ShieldTxcRollbackListener {
	return &ShieldTxcRollbackListener{
		txRollbackFunc:   txRollbackFunc,
		baseEventService: baseEventService,
	}
}

// ConsumeMessage processes the consumed message.
func (l *ShieldTxcRollbackListener) ConsumeMessage() func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	return func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			msgBody := string(msg.Body)
			msgID := msg.MsgId
			logger.Debugf("[ShieldTxcRollbackListener] Consuming [ROLLBACK] Message start... msgId=%s, msgBody=%s", msgID, msgBody)

			shieldTxcMessage := &domain.ShieldTxcMessage{}
			err := shieldTxcMessage.Decode(msgBody)
			if err != nil {
				logger.Errorf("Failed to decode message: %v", err)
				return consumer.ConsumeRetryLater, err
			}

			rollbackEvent := &domain.ShieldEvent{}
			rollbackEvent.Convert(shieldTxcMessage)

			baseEventService := l.baseEventService

			// Persist rollback message.
			rollbackEvent.SetEventStatus(constant.CONSUME_INIT)

			// Handle database insert failure.
			_, err = baseEventService.InsertEventWithId(rollbackEvent)
			if err != nil {
				if !strings.Contains(err.Error(), "Duplicate entry") {
					return consumer.ConsumeRetryLater, err
				} else {
					queryEvent, err := baseEventService.QueryEventById(rollbackEvent)
					if err != nil {
						logger.Errorf("Failed to query event: %v", err)
						return consumer.ConsumeRetryLater, err
					}
					if queryEvent.EventStatus != constant.CONSUME_FAILED {
						return consumer.ConsumeSuccess, nil
					}
				}
			}
			//if !insertResult {
			//	logger.Warnf("[ShieldTxcRollbackListener] Insert RollbackShieldEvent Consume Message failed, msgId=%s", msgID)
			//	return consumer.ConsumeRetryLater, err
			//}

			// Update message status to "processing".
			l.doUpdateMessageStatusProcessing(rollbackEvent)

			// Real consumption via the provided callback.
			consumeResult := l.txRollbackFunc(shieldTxcMessage)
			l.doUpdateAfterRollbackConsumed(consumeResult, rollbackEvent)

			// Log the successful consumption.
			logger.Debugf("[ShieldTxcRollbackListener] Consuming [ROLLBACK] Message end... msgId=%s", msgID)
		}
		return consumer.ConsumeSuccess, nil
	}
}

// doUpdateAfterRollbackConsumed updates the status after rollback consumption based on the result.
func (l *ShieldTxcRollbackListener) doUpdateAfterRollbackConsumed(consumeResult consumer.ConsumeResult, rollbackEvent *domain.ShieldEvent) consumer.ConsumeResult {
	baseEventService := l.baseEventService
	if consumeResult >= consumer.ConsumeRetryLater {
		rollbackEvent.SetBeforeUpdateEventStatus(rollbackEvent.GetEventStatus())
		rollbackEvent.SetEventStatus(constant.CONSUME_FAILED)
		updateBefore, err := baseEventService.UpdateEventStatusById(rollbackEvent)
		if !updateBefore || err != nil {
			return consumer.ConsumeRetryLater
		}
		return consumeResult
	}
	if consumeResult == consumer.ConsumeSuccess {
		// Consumption successful, update status to "processed".
		rollbackEvent.SetBeforeUpdateEventStatus(rollbackEvent.GetEventStatus())
		rollbackEvent.SetEventStatus(constant.CONSUME_PROCESSED)
		updateBefore, err := baseEventService.UpdateEventStatusById(rollbackEvent)
		if !updateBefore || err != nil {
			// Update failed, retry later.
			return consumer.ConsumeRetryLater
		}
	}
	return consumer.ConsumeSuccess
}

// doUpdateMessageStatusProcessing updates the message status to "processing".
func (l *ShieldTxcRollbackListener) doUpdateMessageStatusProcessing(rollbackEvent *domain.ShieldEvent) {
	baseEventService := l.baseEventService
	rollbackEvent.SetBeforeUpdateEventStatus(rollbackEvent.GetEventStatus())
	rollbackEvent.SetEventStatus(constant.CONSUME_PROCESSING)
	updateBefore, err := baseEventService.UpdateEventStatusById(rollbackEvent)
	if !updateBefore || err != nil {
		// Log the failure and return.
		logger.Warnf("Failed to update message status to processing: %v", err)
		return
	}
}
