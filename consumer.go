package angora

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type ConsumerOptions struct {
	Queue      string     // Queue of the deliveries
	Consumer   string     // Name of the consumer
	BufferSize int        // Size of buffered channels
	AutoAck    bool       // Auto acknowledgement
	Exclusive  bool       // Exclusive consumer for queue
	NoLocal    bool       // No Local
	NoWait     bool       // No Wait
	Args       amqp.Table // Additional arguments
}

type ConsumerContext struct {
	id    string          // Unique id of the context
	opt   ConsumerOptions // Consumer options
	state State           // State of the consumer

	channel   *amqp.Channel   // AMQP channel
	channelWg *sync.WaitGroup // Waitgroup for the consumer to finish
	closeMtx  *sync.RWMutex   // Mutex for preventing concurrent calls to Close
	refreshFn RefreshFn       // Function used to refresh the AMQP channel

	refrCtx     context.Context // Context used for refreshing channels
	refrCtxCncl func()          // Function to cancel the refresh context

	consumerWg  *sync.WaitGroup    // Waitgroup to wait for consumer
	consumeChan chan amqp.Delivery // Buffered AMQP delivery channel

	logger *zap.Logger
}

// Creates a new consumer context from a channel and initializes it.
func newConsumerContext(
	id string,
	options ConsumerOptions,
	channel *amqp.Channel,
	refreshFn RefreshFn,
	lgr *zap.Logger,
) (*ConsumerContext, error) {
	refrCtx, cncl := context.WithCancel(
		context.TODO(),
	)

	cns := &ConsumerContext{
		id:    id,
		opt:   options,
		state: Running,

		channel:   channel,
		channelWg: &sync.WaitGroup{},
		closeMtx:  &sync.RWMutex{},
		refreshFn: refreshFn,

		refrCtx:     refrCtx,
		refrCtxCncl: cncl,

		consumerWg:  &sync.WaitGroup{},
		consumeChan: make(chan amqp.Delivery, options.BufferSize),

		logger: lgr,
	}

	cns.initialize()
	return cns, nil
}

// Initializes the consumer context by creating a consumer on the channel,
// settin up a close handler to listen to channel closed events, and starting
// a goroutine that reads deliveries.
func (cns *ConsumerContext) initialize() error {
	var cons <-chan amqp.Delivery
	var err error

	cns.logger.Info("initializing consumer context")
	cons, err = cns.channel.Consume(
		cns.opt.Queue,
		cns.opt.Consumer,
		cns.opt.AutoAck,
		cns.opt.Exclusive,
		cns.opt.NoLocal,
		cns.opt.NoWait,
		cns.opt.Args,
	)
	if err != nil {
		cns.logger.Error(
			"failed to create consumer",
			zap.Error(err),
		)
		return err
	}

	cns.consumerWg.Add(1)
	go func() {
		defer cns.consumerWg.Done()
		cns.logger.Info("started consuming messages")
		defer cns.logger.Info("stopped consuming messages")
		cns.deliveryReader(cons)
	}()

	closeChan := make(chan *amqp.Error, 1)
	cns.channel.NotifyClose(closeChan)
	cns.channelWg.Add(1)
	go func() {
		defer cns.channelWg.Done()
		cns.logger.Info("close handler started")
		defer cns.logger.Info("close handler finished")
		cns.closeHandler(closeChan)
	}()

	cns.logger.Info("consumer context initialized")
	return nil
}

// Reads deliveries from the amqp channel into a buffered channel.
func (cns *ConsumerContext) deliveryReader(
	cons <-chan amqp.Delivery,
) {
	var del amqp.Delivery
	for active := true; active; {
		if del, active = <-cons; active {
			select {
			case cns.consumeChan <- del:
				/* nothing to do here */
			default:
				cns.logger.Warn("buffered delivery channel is full")
				cns.consumeChan <- del
			}
		}
	}
}

// Handles close messages from the amqp channel. If an error happened, it stops
// the channel, refreshes it and reinitializes the consumer.
func (cns *ConsumerContext) closeHandler(
	close <-chan *amqp.Error,
) {
	err, active := <-close
	if err != nil {
		cns.logger.Warn(
			"channel closed unexpectedly",
			zap.String("state", cns.state.String()),
			zap.Error(err),
		)

		if cns.state != Closed {
			cns.channel.Close()
			cns.consumerWg.Wait()

			if err := cns.refresh(); err != nil {
				if err.Error() == "context canceled" {
					cns.logger.Info("refreshing consumer canceled")
				} else {
					cns.logger.Error("failed to refresh", zap.Error(err))
					panic(fmt.Errorf("failed to refresh: %w", err))
				}
			}
		}
	} else if !active {
		cns.logger.Info("channel closed without error")
	} else {
		cns.logger.Warn("no error received")
	}
}

// Refreshes the consumer context by calling the refresh function.
// Once the amqp channel is refreshed, the consumer is reinitialized.
func (cns *ConsumerContext) refresh() error {
	cns.logger.Info("refreshing consumer context")

	chnl, err := cns.refreshFn(cns.refrCtx)
	if err != nil {
		if err.Error() == "context canceled" {
			cns.logger.Info("refreshing channel canceled")
		} else {
			cns.logger.Error("failed to refresh channel", zap.Error(err))
		}
		return err
	}

	cns.channel = chnl
	err = cns.initialize()
	if err != nil {
		cns.logger.Error("failed to initialize consumer context", zap.Error(err))
		return err
	}

	cns.logger.Info("consumer context refreshed")
	return nil
}

// Closes the consumer context. During closure, the channel is put into
// Closing state. The function will wait for the operations to finish before
// closing the channel and cancelling refresh. The state is set to closed before
// exiting the function.
func (cns *ConsumerContext) close() {
	cns.logger.Info("attempting to close consumer context")
	cns.closeMtx.Lock()
	defer cns.closeMtx.Unlock()

	if cns.state == Running {
		cns.logger.Info("closing consumer context")
		cns.state = Closing

		cns.channel.Cancel(cns.opt.Consumer, false)

		cns.channel.Close()
		cns.refrCtxCncl()
		cns.consumerWg.Wait()
		close(cns.consumeChan)
		cns.channelWg.Wait()

		cns.state = Closed
		cns.logger.Info("consumer context closed")
	} else {
		cns.logger.Warn("consumer context already closed")
	}
}

// Returns the buffered delivery channel of the consumer context.
func (cns *ConsumerContext) Channel() (<-chan amqp.Delivery, error) {
	cns.closeMtx.Lock()
	defer cns.closeMtx.Unlock()

	if cns.state != Running {
		err := NewContextClosedError()
		cns.logger.Error("failed to get consumer", zap.Error(err))
		return nil, NewContextClosedError()
	}

	return cns.consumeChan, nil
}
