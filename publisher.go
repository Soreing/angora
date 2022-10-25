package angora

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type Dispatch struct {
	retries    int
	exchange   string
	routingKey string
	mandatory  bool
	immediate  bool
	publishing amqp.Publishing
}

type PublisherOptions struct {
	Publisher    string // Name of the publisher
	BufferSize   int    // Size of buffered channels
	MaxRetries   int    // Max publishing attempts
	WithConfirms bool   // Enable handling confirmations
	WithReturns  bool   // Enable handling returns
}

type PublisherContext struct {
	id    string           // Unique id of the context
	opt   PublisherOptions // Publisher options
	state State            // State of the channel

	channel    *amqp.Channel   // AMQP channel object
	channelWg  *sync.WaitGroup // Waitgroup for the channel to finish
	channelMtx *sync.RWMutex   // Mutex for idk exactly
	closeMtx   *sync.RWMutex   // This mutex prevents concurrent calls to Close
	refreshFn  RefreshFn       // Function used to refresh the channel by the manager

	refrCtx     context.Context // Context used for refreshing channels
	refrCtxCncl func()          // Function to cancel the refresh context

	publishTkn chan bool // Channel to acquire publishing rights for a thread

	dispatchBuff chan Dispatch // Buffered Dispatch channel
	dispatchMtx  *sync.RWMutex //
	dispatchWg   *sync.WaitGroup
	handlerWg    *sync.WaitGroup

	pending    map[uint64]Dispatch
	pendingMtx sync.Mutex
	maxRetries int

	logger *zap.Logger
}

// Creates a new channel context, starts processing dispatches, then
// initializes handlers and fills the publish token channel with 1 token
func newPublisherContext(
	id string,
	options PublisherOptions,
	channel *amqp.Channel,
	confirms chan amqp.Confirmation,
	returns chan amqp.Return,
	refreshFn RefreshFn,
	lgr *zap.Logger,
) (*PublisherContext, error) {
	refrCtx, cncl := context.WithCancel(
		context.TODO(),
	)

	pub := &PublisherContext{
		id:    id,
		opt:   options,
		state: Running,

		channel:    channel,
		channelWg:  &sync.WaitGroup{},
		channelMtx: &sync.RWMutex{},
		closeMtx:   &sync.RWMutex{},
		refreshFn:  refreshFn,

		refrCtx:     refrCtx,
		refrCtxCncl: cncl,

		publishTkn: make(chan bool, 1),

		dispatchBuff: make(chan Dispatch, options.BufferSize),
		dispatchMtx:  &sync.RWMutex{},
		dispatchWg:   &sync.WaitGroup{},
		handlerWg:    &sync.WaitGroup{},

		pending:    map[uint64]Dispatch{},
		pendingMtx: sync.Mutex{},
		maxRetries: options.MaxRetries,

		logger: lgr,
	}

	pub.handlerWg.Add(1)
	go func() {
		defer pub.handlerWg.Done()
		pub.logger.Info("started processing dispatches")
		defer pub.logger.Info("stopped processing dispatches")
		pub.dispatchHandler()
	}()

	pub.publishTkn <- true
	pub.initialize(options)
	return pub, nil
}

// Initializes the channel context by creating a Confirm, Return and Close
// handler. Confirm and Return handlers are optional.
func (pub *PublisherContext) initialize(
	opt PublisherOptions,
) {
	pub.logger.Info("initializing publisher context")

	if opt.WithConfirms {
		confirms := make(chan amqp.Confirmation)
		pub.channel.Confirm(false)
		pub.channel.NotifyPublish(confirms)
		pub.handlerWg.Add(1)
		go func() {
			defer pub.handlerWg.Done()
			pub.logger.Info("started handling confirmations")
			defer pub.logger.Info("stopped handling confirmations")
			pub.confirmsHandler(confirms)
		}()
	}

	if opt.WithReturns {
		returns := make(chan amqp.Return)
		pub.channel.NotifyReturn(returns)
		pub.handlerWg.Add(1)
		go func() {
			defer pub.handlerWg.Done()
			pub.logger.Info("started handling returns")
			defer pub.logger.Info("stopped handling returns")
			pub.returnsHandler(returns)
		}()
	}

	closeChan := make(chan *amqp.Error, 1)
	pub.channel.NotifyClose(closeChan)
	pub.channelWg.Add(1)
	go func() {
		defer pub.channelWg.Done()
		pub.closeHandler(closeChan)
	}()

	pub.logger.Info("publisher context initialized")
}

// Processes dispatches by publishing them. If a publishing succeeded, the
// dispatch is put into the pending map awaiting confirmation. If the publishin
// failed, then it is retried
func (pub *PublisherContext) dispatchHandler() {
	var dsp Dispatch
	for active := true; active; {
		if dsp, active = <-pub.dispatchBuff; active {
			seq, err := pub.publish(
				dsp.exchange,
				dsp.routingKey,
				dsp.mandatory,
				dsp.immediate,
				dsp.publishing,
			)
			if err != nil {
				pub.retryDispatch(dsp)
			} else {
				if dsp, ok := pub.pending[seq]; !ok {
					pub.pending[seq] = dsp
				} else {
					pub.setPending(seq, dsp)
				}
			}
		}
	}
}

// Retries the dispatch if it is below the maximum retries threshold
// If the dispatch can not be retried, it is logged as an error
// TODO: Return it to the client to handle it
func (pub *PublisherContext) retryDispatch(dsp Dispatch) {
	dsp.retries++
	if dsp.retries < pub.maxRetries {
		pub.dispatchBuff <- dsp
	} else {
		pub.logger.Error(
			"failed to retry dispatch",
			zap.Any("dispatch", dsp),
		)
	}
}

// Places the dispatch into the pending map. If there is already a dispatch
// waiting under the same sequence, it logs an error but overwrites it anyway
func (pub *PublisherContext) setPending(seq uint64, dsp Dispatch) {
	pub.pendingMtx.Lock()
	defer pub.pendingMtx.Unlock()
	if dsp, ok := pub.pending[seq]; ok {
		pub.logger.Error(
			"dispatch already pending",
			zap.Uint64("seq", seq),
			zap.Any("dispatch", dsp),
		)
	}
	pub.pending[seq] = dsp
}

// Gets the pending dispatch with a sequence number.
// TODO: I think it is possible for the confirm to run before the publisher has
// a chance to put the dispatch into the map, so that's like.. bad..
func (pub *PublisherContext) getPending(seq uint64) (Dispatch, error) {
	pub.pendingMtx.Lock()
	defer pub.pendingMtx.Unlock()
	if dsp, ok := pub.pending[seq]; ok {
		return dsp, nil
	} else {
		pub.logger.Error("dispatch not found", zap.Uint64("seq", seq))
		return Dispatch{}, fmt.Errorf("dispatch not found")
	}
}

// Clears the pending dispatch from the map
func (pub *PublisherContext) clearPending(seq uint64) {
	pub.pendingMtx.Lock()
	defer pub.pendingMtx.Unlock()
	if _, ok := pub.pending[seq]; ok {
		delete(pub.pending, seq)
	} else {
		pub.logger.Error("dispatch not found", zap.Uint64("seq", seq))
	}
}

// Processes confirm messages from the amqp server. If acknowledged, the
// dispatch is removed from the pending map and the waitgroup is done.
// If not acknowledged, the dispatch will be retried
func (pub *PublisherContext) confirmsHandler(
	cnfrms <-chan amqp.Confirmation,
) {
	var cnf amqp.Confirmation
	for active := true; active; {
		if cnf, active = <-cnfrms; active {
			if cnf.DeliveryTag > 0 {
				if cnf.Ack {
					pub.clearPending(cnf.DeliveryTag)
					pub.dispatchWg.Done()
				} else {
					dsp, err := pub.getPending(cnf.DeliveryTag)
					if err != nil {
						pub.retryDispatch(dsp)
					} else {
						pub.logger.Error(
							"can not retry dispatch",
							zap.Uint64("seq", cnf.DeliveryTag),
						)
						pub.dispatchWg.Done()
					}
				}
			}
		}
	}
}

// Processes returned publishings and logs them as errors
// TODO: Return the publishing to the client to handle it
func (pub *PublisherContext) returnsHandler(
	rtrns <-chan amqp.Return,
) {
	var rt amqp.Return
	for active := true; active; {
		if rt, active = <-rtrns; active {
			pub.logger.Error("publishing returned", zap.Any("dispatch", rt))
		}
	}
}

// Handles close messages from the amqp channel. If an error happened, it stops
// the channel, refreshes it and ... TODO
func (pub *PublisherContext) closeHandler(
	close chan *amqp.Error,
) {
	err, active := <-close
	if err != nil {
		pub.logger.Error(
			"channel closed unexpectedly",
			zap.String("state", pub.state.String()),
			zap.Error(err),
		)

		if pub.state != Closed {
			pub.channel.Close()
			pub.handlerWg.Wait()

			if err := pub.refresh(); err != nil {
				if err.Error() == "context canceled" {
					pub.logger.Info("refreshing cancelled")
				} else {
					pub.logger.Error("failed to refresh", zap.Error(err))
					panic(fmt.Errorf("failed to refresh: %w", err))
				}
			}
		}
	} else if !active {
		pub.logger.Info("channel closed without error")
	} else {
		pub.logger.Warn("no error received")
	}
}

// Refreshes the channel by calling the refresh function.
// If the channel context is not closing, it sets up consumers again.
// channel must be completely stopped before refresh is called
func (pub *PublisherContext) refresh() error {
	pub.logger.Info("attempting to refresh publisher context")
	pub.channelMtx.Lock()
	defer pub.channelMtx.Unlock()

	pub.logger.Info("refreshing channel")
	chnl, err := pub.refreshFn(pub.refrCtx)
	if err != nil {
		if err.Error() == "context canceled" {
			pub.logger.Info("refreshing cancelled")
		} else {
			pub.logger.Error("failed to refresh channel", zap.Error(err))
		}
		return err
	}

	pub.channel = chnl
	// TODO: Finish getting channels
	pub.initialize(pub.opt)

	pub.logger.Info("publisher context refreshed")
	return nil
}

// Closes the publisher context. During closure, the channel is put into
// Closing state. The function will wait for the operations to finish before
// closing the channel and cancelling refresh. The state is set to closed before
// exiting the function.
func (pub *PublisherContext) close() {
	pub.logger.Info("attempting to close publisher context")
	pub.closeMtx.Lock()
	defer pub.closeMtx.Unlock()

	if pub.state == Running {
		pub.logger.Info("closing publisher context")
		pub.state = Closing

		pub.logger.Info("waiting for pending dispatches")
		pub.dispatchWg.Wait()

		pub.channel.Close()
		pub.refrCtxCncl()
		close(pub.dispatchBuff)
		pub.handlerWg.Wait()
		pub.channelWg.Wait()

		pub.state = Closed
		pub.logger.Info("publisher context closed")
	} else {
		pub.logger.Warn("publisher context already closed")
	}
}

// Publishes a publishing.
func (pub *PublisherContext) publish(
	exchange string,
	key string,
	mandatory bool,
	immediate bool,
	msg amqp.Publishing,
) (uint64, error) {
	pub.channelMtx.RLock()
	defer pub.channelMtx.RUnlock()

	if pub.state != Running {
		return 0, NewContextClosedError()
	}

	// Publisher Token Scheme
	<-pub.publishTkn
	defer func() {
		pub.publishTkn <- true
	}()

	// Publish
	sqno := pub.channel.GetNextPublishSeqNo()
	err := pub.channel.PublishWithContext(
		context.TODO(),
		exchange,
		key,
		mandatory,
		immediate,
		msg,
	)
	if err != nil {
		pub.logger.Error("failed to publish", zap.Error(err))
		return 0, err
	}
	return sqno, nil
}

// Push an dispatch with specified exchange, routing key and json body into the
// dispatch channel that will be processed. If the publisher is closed, or the
// channel is full, the function returns an error
func (pub *PublisherContext) Dispatch(
	exchange string,
	routingKey string,
	mandatory bool,
	immediate bool,
	publishing amqp.Publishing,
) error {
	pub.dispatchMtx.RLock()
	defer pub.dispatchMtx.RUnlock()

	if pub.state != Running {
		err := NewContextClosedError()
		pub.logger.Error("failed to dispatch", zap.Error(err))
		return err
	}

	dsp := Dispatch{
		retries:    0,
		exchange:   exchange,
		routingKey: routingKey,
		mandatory:  mandatory,
		immediate:  immediate,
		publishing: publishing,
	}

	pub.dispatchWg.Add(1)
	select {
	case pub.dispatchBuff <- dsp:
		return nil
	default:
		pub.dispatchWg.Done()
		return NewDispatchBufferFullError()
	}
}
