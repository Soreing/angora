package angora

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"
)

const PENDING_ID_TAG = "4bT4ExJH8x"

type Dispatch struct {
	refId string // Pending reference id
	seqNo uint64 // sequence number

	retries   int        // Number of retries
	failed    bool       // Dispatch was returned
	confirmCh chan error // Notify of confirmation

	startTime  time.Time       // Time of submission
	exchange   string          // Exchange name
	routingKey string          // Routing key
	mandatory  bool            // Mandatory flag
	immediate  bool            // Immediate flag
	Publishing amqp.Publishing // Publishing data

	Args map[string]any // Additional details for hooks
}

func (d *Dispatch) GetRetries() int {
	return d.retries
}

func (d *Dispatch) GetStartTime() time.Time {
	return d.startTime
}

func (d *Dispatch) GetExchange() string {
	return d.exchange
}

func (d *Dispatch) GetRoutingKey() string {
	return d.routingKey
}

func (d *Dispatch) IsMandatory() bool {
	return d.mandatory
}

func (d *Dispatch) IsImmediate() bool {
	return d.immediate
}

func (d *Dispatch) reset() {
	d.failed = false
	d.seqNo = 0
	d.refId = ""
}

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
	channelMtx *sync.RWMutex // Prevents concurrent access to the channel
	refreshFn  RefreshFn     // Function used to refresh the channel by the manager

	refrCtx     context.Context // Context used for refreshing channels
	refrCtxCncl func()          // Function to cancel the refresh context

	closeCh  chan error    // Signals the context to close
	closeMtx *sync.RWMutex // Prevents concurrent calls to Close

	publishTkn   chan bool            // Acquire publishing rights
	dispatchBuff chan *Dispatch       // Buffers dispatches to be processed
	dispatchMtx  *sync.RWMutex        // Synchronizes sending dispatches
	dispatchWg   *sync.WaitGroup      // Total pending dispatches
	pendSeq      map[uint64]*Dispatch // Map of pending dispatches by sequence num
	pendRId      map[string]*Dispatch // Map of pending dispatches by reference id
	pendMtx      sync.Mutex           // Prevents concurrent access to pending maps
	returnCh     chan amqp.Publishing // Sends failed async publishings back to client

	channelWg *sync.WaitGroup // Waits for the channel to close
	handlerWg *sync.WaitGroup // Waits for the handler threads to finish
	processWg *sync.WaitGroup // Waits for the dispatch processor to finish

	BeforePublish func(context.Context, *Dispatch) // Hook executing code before publishing a dispatch
	AfterConfirm  func(Dispatch, bool)             // Hook executing after confirming a dispatch

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

		publishTkn:   make(chan bool, 1),
		dispatchBuff: make(chan *Dispatch, options.BufferSize),
		dispatchMtx:  &sync.RWMutex{},
		dispatchWg:   &sync.WaitGroup{},
		pendSeq:      map[uint64]*Dispatch{},
		pendRId:      map[string]*Dispatch{},
		pendMtx:      sync.Mutex{},
		channelWg:    &sync.WaitGroup{},
		handlerWg:    &sync.WaitGroup{},
		processWg:    &sync.WaitGroup{},

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

	pub.publishTkn <- true
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
		if dsp, active = <-pub.dispatchBuff; active {
			err := pub.publishLocked(dsp)
			if err != nil {
				pub.retryDispatch(dsp)
			}
		}
	}
}

// Retries the dispatch if it is below the maximum retries threshold. If the
// dispatch can not be retried, it is returned to the client
func (pub *PublisherContext) retryDispatch(dsp *Dispatch) {
	dsp.retries++
	dsp.reset()

	if dsp.retries < pub.maxRetr {
		pub.dispatchBuff <- dsp
	} else {
		err := errors.New("failed after max retries")
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
		pub.dispatchWg.Done()
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
		dsp.failed = true
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
// was returned, is marked as such in the pending map. Upon confirmation, if
// the publishing is acknowledged and not returned, it is removed from the pending
// map. Otherwise, the publishing is retried.
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
					ok := cnf.Ack && !dsp.failed
					if pub.AfterConfirm != nil {
						pub.AfterConfirm(*dsp, ok)
					}
					if ok {
						pub.clearPending(cnf.DeliveryTag)
						pub.dispatchWg.Done()
						if dsp.confirmCh != nil {
							close(dsp.confirmCh)
						}
					} else {
						pub.retryDispatch(dsp)
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
// the handlers are set up and pending publishings are re-sent. If the pending
// publishings fail to be re-sent, the channel is closed to try again.
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

	if err = pub.republishPending(); err != nil {
		pub.logger.Warn("failed to retry pending publishings")
	}

	pub.logger.Info("publisher context refreshed")
	return nil
}

// Re-publishes all pending dispatches to receive confirmation.
func (pub *PublisherContext) republishPending() error {
	pub.logger.Info("retrying pending publishings")

	stale, idx := make([]*Dispatch, len(pub.pendRId)), 0
	for _, e := range pub.pendRId {
		stale[idx] = e
		idx++
		fmt.Println(e.seqNo)
	}
	pub.pendRId = map[string]*Dispatch{}
	pub.pendSeq = map[uint64]*Dispatch{}

	for _, e := range stale {
		if err := pub.publish(e); err != nil {
			return err
		}
	}
	return nil
}

// Publishes a dispatch with a read lock on the channel.
func (pub *PublisherContext) publishLocked(
	dsp *Dispatch,
) error {
	pub.channelMtx.RLock()
	defer pub.channelMtx.RUnlock()
	return pub.publish(dsp)
}

// Publishes a dispatch. A reference id is generated for setting the dispatch as
// pending. On failure to publish, the pending record is cleared.
func (pub *PublisherContext) publish(
	dsp *Dispatch,
) error {
	select {
	case <-pub.closeCh:
		err := NewContextClosedError()
		pub.logger.Error("failed to publish", zap.Error(err))
		return err
	default:
		if dsp.Publishing.Headers == nil {
			dsp.Publishing.Headers = amqp.Table{}
		}

		rid := uuid.NewV4().String()
		dsp.Publishing.Headers[PENDING_ID_TAG] = rid
		dsp.refId = rid

		// Publisher Token Scheme
		<-pub.publishTkn
		defer func() {
			pub.publishTkn <- true
		}()

		sqno := pub.channel.GetNextPublishSeqNo()
		dsp.seqNo = sqno

		pub.setPending(dsp)
		err := pub.channel.PublishWithContext(
			context.TODO(),
			dsp.exchange,
			dsp.routingKey,
			dsp.mandatory,
			dsp.immediate,
			dsp.Publishing,
		)
		if err != nil {
			pub.clearPending(sqno)
			pub.logger.Error("failed to publish", zap.Error(err))
			return err
		}
		return nil
	}
}

// Signals the publisher context to close. The function will wait for the dispatches
// to finish before closing the channel and cancelling refresh to stop gracefully.
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
		pub.dispatchWg.Wait()

		close(pub.dispatchBuff)
		pub.channel.Close()
		pub.refrCtxCncl()

		pub.handlerWg.Wait()
		pub.processWg.Wait()

		pub.logger.Info("publisher context closed")
	}
}

// Push a dispatch with specified exchange, routing key and json body into the
// dispatch channel that will be processed. If the publisher is closed, or the
// channel is full, the function returns an error. Publishing is done async. If
// The publishing fails, it is returned via the return channel.
func (pub *PublisherContext) DispatchAsync(
	ctx context.Context,
	exchange, routingKey string,
	mandatory, immediate bool,
	publishing amqp.Publishing,
) error {
	pub.dispatchMtx.RLock()
	defer pub.dispatchMtx.RUnlock()

	select {
	case <-pub.closeCh:
		err := NewContextClosedError()
		pub.logger.Error("failed to dispatch", zap.Error(err))
		return err
	default:
		dsp := &Dispatch{
			retries:    0,
			failed:     false,
			startTime:  time.Now(),
			exchange:   exchange,
			routingKey: routingKey,
			mandatory:  mandatory,
			immediate:  immediate,
			Publishing: publishing,
			Args:       map[string]any{},
		}

		if pub.BeforePublish != nil {
			pub.BeforePublish(ctx, dsp)
		}

		pub.dispatchWg.Add(1)
		select {
		case pub.dispatchBuff <- dsp:
			return nil
		default:
			pub.dispatchWg.Done()
			err := NewDispatchBufferFullError()
			pub.logger.Error("failed to dispatch", zap.Error(err))
			return err
		}
	}
}

// Push a dispatch with specified exchange, routing key and json body into the
// dispatch channel that will be processed. If the publisher is closed, or the
// channel is full, the function returns an error. Waits for the publishing
// to be confirmed or fail and return.
func (pub *PublisherContext) Dispatch(
	ctx context.Context,
	exchange, routingKey string,
	mandatory, immediate bool,
	publishing amqp.Publishing,
) error {
	pub.dispatchMtx.RLock()
	defer pub.dispatchMtx.RUnlock()

	select {
	case <-pub.closeCh:
		err := NewContextClosedError()
		pub.logger.Error("failed to dispatch", zap.Error(err))
		return err
	default:
		confirmCh := make(chan error)
		dsp := &Dispatch{
			retries:    0,
			failed:     false,
			startTime:  time.Now(),
			confirmCh:  confirmCh,
			exchange:   exchange,
			routingKey: routingKey,
			mandatory:  mandatory,
			immediate:  immediate,
			Publishing: publishing,
		}

		if pub.BeforePublish != nil {
			pub.BeforePublish(ctx, dsp)
		}

		pub.dispatchWg.Add(1)
		select {
		case <-ctx.Done():
			return errors.New("context canceled")
		case pub.dispatchBuff <- dsp:
			select {
			case <-ctx.Done():
				return errors.New("context canceled")
			case err := <-confirmCh:
				return err
			}
		}
	}
}

// Returns a channel that sends async publishings that failed to publish
func (pub *PublisherContext) Returns() chan amqp.Publishing {
	return pub.returnCh
}
