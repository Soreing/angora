package angora

import (
	"context"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"
)

const PENDING_ID_TAG = "4bT4ExJH8x"

type PublisherOptions struct {
	Publisher   string // Name of the publisher
	BufferSize  int    // Size of buffered channels
	MaxRetries  int    // Max publishing attempts
	WithReturns bool   // Send back failed dispatches
}

type PublisherContext struct {
	id      string // Unique id of the context
	name    string // Name of the publisher
	maxRetr int    // Max retries for dispatches

	channel    *amqp.Channel // AMQP channel object
	channelMtx *sync.RWMutex // Prevents access to the channel when broken
	refreshFn  RefreshFn     // Function used to refresh a broken channel

	refrCtx     context.Context // Context used for refreshing channels
	refrCtxCncl func()          // Function to cancel the refresh context

	closeCh  chan error    // Used for checking the closed state of the publisher
	closeMtx *sync.RWMutex // Prevents concurrent calls to Close

	retryChan    chan *Dispatch  // Buffers retries to be processed with priority
	dispatchChan chan *Dispatch  // Buffers dispatches to be processed
	dispatches   *sync.WaitGroup // Total pending dispatches

	pendSeq map[uint64]*Dispatch // Map of pending dispatches by sequence num
	pendRId map[string]*Dispatch // Map of pending dispatches by reference id
	pendMtx sync.Mutex           // Prevents concurrent access to pending maps

	returnCh chan amqp.Publishing // Sends failed async publishings back to client

	channelWg *sync.WaitGroup // Waits for the channel to close
	handlerWg *sync.WaitGroup // Waits for the handler threads to finish
	processWg *sync.WaitGroup // Waits for the dispatch processor to finish

	onDispatch []func(context.Context, *Dispatch) // Hooks ran before queueing a dispatch
	onPublish  []func(*Dispatch)                  // Hooks ran before publishing a dispatch
	onConfirm  []func(*Dispatch, bool)            // Hooks ran after publishings are acked or nacked

	logger *zap.Logger
}

// Creates a new channel context, starts processing dispatches,
// then initializes handlers
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
		id:      id,
		name:    options.Publisher,
		maxRetr: options.MaxRetries,

		channel:    channel,
		channelMtx: &sync.RWMutex{},
		refreshFn:  refreshFn,

		refrCtx:     refrCtx,
		refrCtxCncl: cncl,

		closeCh:  make(chan error),
		closeMtx: &sync.RWMutex{},

		retryChan:    make(chan *Dispatch, options.BufferSize),
		dispatchChan: make(chan *Dispatch, options.BufferSize),
		dispatches:   &sync.WaitGroup{},

		pendSeq: map[uint64]*Dispatch{},
		pendRId: map[string]*Dispatch{},
		pendMtx: sync.Mutex{},

		channelWg: &sync.WaitGroup{},
		handlerWg: &sync.WaitGroup{},
		processWg: &sync.WaitGroup{},

		logger: lgr,
	}

	if options.WithReturns {
		pub.returnCh = make(chan amqp.Publishing, options.BufferSize)
	}

	pub.processWg.Add(1)
	go func() {
		defer pub.processWg.Done()
		pub.logger.Info("started processing dispatches")
		defer pub.logger.Info("stopped processing dispatches")
		pub.dispatchProcessor()
	}()

	pub.initialize()
	return pub, nil
}

// Sets up the Confirm, Return and Close handlers
func (pub *PublisherContext) initialize() {
	pub.logger.Info("initializing publisher context")

	confirms := make(chan amqp.Confirmation)
	returns := make(chan amqp.Return)
	closeChan := make(chan *amqp.Error, 1)

	pub.channel.NotifyPublish(confirms)
	pub.channel.NotifyReturn(returns)
	pub.channel.Confirm(false)
	pub.handlerWg.Add(1)
	go func() {
		defer pub.handlerWg.Done()
		pub.logger.Info("started handling confirms and returns")
		defer pub.logger.Info("stopped handling confirms and returns")
		pub.confirmsHandler(confirms, returns)
	}()

	pub.channel.NotifyClose(closeChan)
	pub.channelWg.Add(1)
	go func() {
		defer pub.channelWg.Done()
		pub.logger.Info("channel close handler started")
		defer pub.logger.Info("channel close handler finished")
		pub.closeHandler(closeChan)
	}()

	pub.logger.Info("publisher context initialized")
}

// Processes dispatches by publishing them. On failure, dispaches are retried
func (pub *PublisherContext) dispatchProcessor() {
	var dsp *Dispatch
	for active := true; active; {
		select {
		case dsp, active = <-pub.retryChan:
			/* successfully read from retry channel */
		default:
			select {
			case dsp, active = <-pub.retryChan:
				/* successfully read from retry channel */
			case dsp, active = <-pub.dispatchChan:
				/* successfully read from dispatch channel*/
			}
		}
		if active {
			if err := pub.publish(dsp); err != nil {
				pub.retryDispatch(dsp, pub.retryChan)
			}
		}
	}
}

// Retries the dispatch if it is below the maximum retries threshold. If the
// dispatch can not be retried, it is returned to the client
func (pub *PublisherContext) retryDispatch(dsp *Dispatch, dest chan *Dispatch) {
	dsp.retries++
	dsp.reset()

	var err error
	if dsp.retries <= pub.maxRetr {
		select {
		case dest <- dsp:
			/* successfully pushed to the destination */
		default:
			err = errors.New("destination channel is full")
		}
	} else {
		err = errors.New("failed after max retries")
	}

	if err != nil {
		pub.logger.Error(
			"failed to retry dispatch",
			zap.Any("dispatch", dsp),
			zap.Error(err),
		)
		if dsp.confirmCh != nil {
			dsp.confirmCh <- err
		} else if pub.returnCh != nil {
			pub.returnCh <- dsp.Publishing
		}
		pub.dispatches.Done()
	}
}

// Places the dispatch into the pending map.
func (pub *PublisherContext) setPending(dsp *Dispatch) error {
	pub.pendMtx.Lock()
	defer pub.pendMtx.Unlock()

	sdsp := pub.pendSeq[dsp.seqNo]
	rdsp := pub.pendRId[dsp.refId]
	if sdsp != nil || rdsp != nil {
		err := errors.New("dispatch already pending")
		pub.logger.Error(
			"failed to set as pending",
			zap.Uint64("seq", dsp.seqNo),
			zap.String("rid", dsp.refId),
			zap.Any("dispatch", dsp),
			zap.Error(err),
		)
		return err
	} else {
		pub.pendSeq[dsp.seqNo] = dsp
		pub.pendRId[dsp.refId] = dsp
		return nil
	}
}

// Gets the pending dispatch by sequence number.
func (pub *PublisherContext) getPending(seq uint64) (*Dispatch, error) {
	pub.pendMtx.Lock()
	defer pub.pendMtx.Unlock()
	if dsp := pub.pendSeq[seq]; dsp != nil {
		return dsp, nil
	} else {
		err := errors.New("dispatch not found")
		pub.logger.Error(
			"failed to get pending dispatch",
			zap.Uint64("seq", seq),
			zap.Error(err),
		)
		return nil, err
	}
}

// Sets the pending dispatch to be failed
func (pub *PublisherContext) failPending(rid string) error {
	pub.pendMtx.Lock()
	defer pub.pendMtx.Unlock()
	if dsp := pub.pendRId[rid]; dsp != nil {
		dsp.returned = true
		return nil
	} else {
		err := errors.New("dispatch not found")
		pub.logger.Error(
			"failed to fail pending dispatch",
			zap.String("rid", rid),
			zap.Error(err),
		)
		return err
	}
}

// Clears the pending dispatch by sequence number.
func (pub *PublisherContext) clearPending(seq uint64) error {
	pub.pendMtx.Lock()
	defer pub.pendMtx.Unlock()
	if dsp := pub.pendSeq[seq]; dsp != nil {
		delete(pub.pendSeq, dsp.seqNo)
		delete(pub.pendRId, dsp.refId)
		return nil
	} else {
		err := errors.New("dispatch not found")
		pub.logger.Error(
			"failed to clear pending dispatch",
			zap.Uint64("seq", seq),
			zap.Error(err),
		)
		return err
	}
}

// Processes confirm and return messages from the amqp server. If the publishing
// was returned, is marked as such in the pending map. Upon confirmation, the
// confirm hooks are ran, and  if the publishing was successful, it is removed
// from the pending map. Otherwise, the publishing is retried.
func (pub *PublisherContext) confirmsHandler(
	cnfrms <-chan amqp.Confirmation,
	rtrns <-chan amqp.Return,
) {
	var cnf amqp.Confirmation
	var rt amqp.Return

	for active := true; active; {
		select {
		case cnf, active = <-cnfrms:
			if active && cnf.DeliveryTag > 0 {
				if cnf.DeliveryTag%2 == 0 {
					continue
				}

				dsp, err := pub.getPending(cnf.DeliveryTag)
				if err != nil {
					pub.logger.Error(
						"dispatch is missing",
						zap.Uint64("seq", cnf.DeliveryTag),
						zap.Error(err),
					)
				} else {
					ok := cnf.Ack && !dsp.returned
					for _, hk := range pub.onConfirm {
						hk(dsp, ok)
					}
					if ok {
						pub.clearPending(cnf.DeliveryTag)
						pub.dispatches.Done()
						if dsp.confirmCh != nil {
							close(dsp.confirmCh)
						}
					} else {
						pub.retryDispatch(dsp, pub.dispatchChan)
					}
				}
			}
		case rt, active = <-rtrns:
			pub.logger.Warn(
				"publishing returned",
				zap.Any("return", rt),
			)
			if rt.Headers != nil {
				if data, ok := rt.Headers[PENDING_ID_TAG]; ok {
					if rid, ok := data.(string); ok {
						pub.failPending(rid)
					}
				}
			}
		}
	}
}

// Handles close messages from the amqp channel. If an error happened, it attempts
// to refresh the channel. If no error is received, it is closed gracefully.
func (pub *PublisherContext) closeHandler(
	close chan *amqp.Error,
) {
	err, active := <-close
	if err != nil {
		pub.logger.Error(
			"channel closed unexpectedly",
			zap.Error(err),
		)

		// panic on misconfiguration
		if err.Code == 404 {
			panic(err)
		}

		if err := pub.refresh(); err != nil {
			if err.Error() == "context canceled" {
				pub.logger.Info("refreshing canceled")
			} else {
				pub.logger.Error("failed to refresh", zap.Error(err))
				panic(fmt.Errorf("failed to refresh: %w", err))
			}
		}
	} else if !active {
		pub.logger.Info("channel closed without error")
	}
}

// Refreshes the channel by calling the refresh function. Puts a lock on the
// channel to stop other functions using it while it is stale. Once refreshed,
// the handlers are set up and pending publishings are retried.
func (pub *PublisherContext) refresh() error {
	pub.logger.Info("attempting to refresh publisher context")
	pub.channelMtx.Lock()
	defer pub.channelMtx.Unlock()

	pub.logger.Info("refreshing channel")
	chnl, err := pub.refreshFn(pub.refrCtx)
	if err != nil {
		if err.Error() == "context canceled" {
			pub.logger.Info("refreshing canceled")
		} else {
			pub.logger.Error("failed to refresh channel", zap.Error(err))
		}
		return err
	}

	pub.channel = chnl
	pub.initialize()
	pub.requeuePending()

	pub.logger.Info("publisher context refreshed")
	return nil
}

// Requeues the pending messages that are unable to receive a confirmation.
func (pub *PublisherContext) requeuePending() error {
	pub.logger.Info("requeuing pending publishings")

	for _, e := range pub.pendRId {
		for _, hk := range pub.onConfirm {
			hk(e, false)
		}
		pub.retryDispatch(e, pub.retryChan)
	}

	pub.pendRId = map[string]*Dispatch{}
	pub.pendSeq = map[uint64]*Dispatch{}
	return nil
}

// Publish a dispatch with a sequence and reference id. The publish hooks
// are ran and the dispatch is set as pending. On failure to publish,
// the dispatch is cleared from pending
func (pub *PublisherContext) publish(
	dsp *Dispatch,
) error {
	pub.channelMtx.RLock()
	defer pub.channelMtx.RUnlock()

	if dsp.Publishing.Headers == nil {
		dsp.Publishing.Headers = amqp.Table{}
	}

	dsp.refId = uuid.NewV4().String()
	dsp.seqNo = pub.channel.GetNextPublishSeqNo()
	dsp.Publishing.Headers[PENDING_ID_TAG] = dsp.refId

	pub.setPending(dsp)
	for _, hk := range pub.onPublish {
		hk(dsp)
	}

	err := pub.channel.PublishWithContext(
		context.TODO(),
		dsp.exchange,
		dsp.routingKey,
		dsp.mandatory,
		dsp.immediate,
		dsp.Publishing,
	)
	if err != nil {
		pub.clearPending(dsp.seqNo)
		pub.logger.Error("failed to publish", zap.Error(err))
		return err
	}
	return nil
}

// Closes the publisher context and waits for all pending dispatches before
// closing the underlying channel and processes gracefully.
func (pub *PublisherContext) close() {
	pub.logger.Info("attempting to close publisher context")
	pub.closeMtx.Lock()
	defer pub.closeMtx.Unlock()

	select {
	case <-pub.closeCh:
		pub.logger.Warn("publisher context already closed")
	default:
		pub.logger.Info("closing publisher context")
		close(pub.closeCh)

		pub.logger.Info("waiting for pending dispatches")
		pub.dispatches.Wait()
		close(pub.dispatchChan)
		close(pub.retryChan)

		pub.logger.Info("closing amqp channel")
		pub.channel.Close()
		pub.refrCtxCncl()

		pub.logger.Info("waiting for processes to stop")
		pub.handlerWg.Wait()
		pub.processWg.Wait()

		pub.logger.Info("publisher context closed")
	}
}

// Dispatch a publishing to an exchange with a routing key and options.
// The dispatch hooks are ran and the dispatch is pushed to a buffered channel.
// If the dispatch buffer is full, the function waits until there is space, or
// until the context is canceled. Processing the dispatch is asynchronous.
func (pub *PublisherContext) DispatchAsync(
	ctx context.Context,
	exchange, routingKey string,
	mandatory, immediate bool,
	publishing amqp.Publishing,
) (err error) {
	dsp := &Dispatch{
		retries:    0,
		returned:   false,
		exchange:   exchange,
		routingKey: routingKey,
		mandatory:  mandatory,
		immediate:  immediate,
		Publishing: publishing,
		Params:     map[string]any{},
	}

	select {
	case <-pub.closeCh:
		err = errors.New("publisher closed")
	default:
		pub.dispatches.Add(1)
		for _, hk := range pub.onDispatch {
			hk(ctx, dsp)
		}

		select {
		case <-ctx.Done():
			err = errors.New("context canceled")
		case pub.dispatchChan <- dsp:
			/* successfully dispatched */
		}
	}

	if err != nil {
		pub.logger.Error(
			"failed to dispatch",
			zap.Any("dispatch", *dsp),
			zap.Error(err),
		)
	}
	return
}

// Dispatch a publishing to an exchange with a routing key and options.
// The dispatch hooks are ran and the dispatch is pushed to a buffered channel.
// Waits untill the dispatch is confirmed or fails, or the context is canceled.
func (pub *PublisherContext) Dispatch(
	ctx context.Context,
	exchange, routingKey string,
	mandatory, immediate bool,
	publishing amqp.Publishing,
) (err error) {
	confirmCh := make(chan error)
	dsp := &Dispatch{
		retries:    0,
		returned:   false,
		confirmCh:  confirmCh,
		exchange:   exchange,
		routingKey: routingKey,
		mandatory:  mandatory,
		immediate:  immediate,
		Publishing: publishing,
		Params:     map[string]any{},
	}

	select {
	case <-pub.closeCh:
		err = errors.New("publisher closed")
	default:
		pub.dispatches.Add(1)
		for _, hk := range pub.onDispatch {
			hk(ctx, dsp)
		}

		select {
		case <-ctx.Done():
			err = errors.New("context canceled")
		case pub.dispatchChan <- dsp:
			select {
			case <-ctx.Done():
				err = errors.New("context canceled")
			case err = <-confirmCh:
				/*err = nil in case of success*/
			}

			if err != nil {
				pub.logger.Error(
					"dispatch failed",
					zap.Any("dispatch", *dsp),
					zap.Error(err),
				)
			}
			return
		}
	}

	if err != nil {
		pub.logger.Error(
			"failed to dispatch",
			zap.Any("dispatch", *dsp),
			zap.Error(err),
		)
	}
	return
}

// Attaches a dispatch hook on the publisher that is executed before
// the dispatch gets queued in the channel. The hook can not be removed.
func (pub *PublisherContext) UseDispatchHook(
	fn func(context.Context, *Dispatch),
) {
	pub.onDispatch = append(pub.onDispatch, fn)
}

// Attaches a publish hook on the publisher that is executed before
// the dispatch gets published. The hook can not be removed.
func (pub *PublisherContext) UsePublishHook(
	fn func(*Dispatch),
) {
	pub.onPublish = append(pub.onPublish, fn)
}

// Attaches a confrim hook on the publisher that is executed when the
// dispatch receives an ack or a nack from the server. The hook can
// not be removed.
func (pub *PublisherContext) UseConfirmHook(
	fn func(*Dispatch, bool),
) {
	pub.onConfirm = append(pub.onConfirm, fn)
}

// Returns a channel containing async publishings that failed to publish
func (pub *PublisherContext) Returns() chan amqp.Publishing {
	return pub.returnCh
}
