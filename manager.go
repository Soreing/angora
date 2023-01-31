package angora

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Soreing/retrier"

	amqp "github.com/rabbitmq/amqp091-go"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"
)

type RefreshFn func(context.Context) (*amqp.Channel, error)

type ConnectionManager struct {
	url      string           // The amqp url to connect to
	state    State            // State of the connection
	conn     *amqp.Connection // The amqp connection object
	connWg   *sync.WaitGroup  // This wait group should be waited on before Close can return
	connMtx  *sync.RWMutex    // This mutex prevents usage of conn while it's reconnecting
	closeMtx *sync.RWMutex    // This mutex prevents concurrent calls to Close

	retrCtx     context.Context // Context used for retrying connections
	retrCtxCncl func()          // Function to cancel the retrying context

	consumers  map[string]*ConsumerContext  // Managed consumer contexts
	publishers map[string]*PublisherContext // Managed publisher contexts

	logger *zap.Logger
}

// Constructs a new connection manager that reconnects amqp connections,
// and channels. The constructor attempts to make a connection and if it
// can't, it panics.
func NewConnectionManager(
	url string,
	lgr *zap.Logger,
) *ConnectionManager {
	retrCtx, cncl := context.WithCancel(
		context.TODO(),
	)

	mgr := &ConnectionManager{
		url:         url,
		state:       Created,
		conn:        nil,
		connWg:      &sync.WaitGroup{},
		connMtx:     &sync.RWMutex{},
		closeMtx:    &sync.RWMutex{},
		retrCtx:     retrCtx,
		retrCtxCncl: cncl,
		consumers:   map[string]*ConsumerContext{},
		publishers:  map[string]*PublisherContext{},
		logger:      lgr,
	}

	err := mgr.connect()
	if err != nil {
		lgr.Error("failed to connect to server")
		panic(err)
	}
	return mgr
}

// Attempts to connect to the amqp url provided. Upon successful connection,
// it sets up the close handler that listens to a close notifications.
func (mgr *ConnectionManager) connect() error {
	mgr.logger.Info("establishing connection")
	mgr.connMtx.Lock()
	defer mgr.connMtx.Unlock()

	mgr.logger.Info("dialing", zap.String("url", mgr.url))
	conn, err := amqp.Dial(mgr.url)
	if err != nil {
		mgr.logger.Error("dialing failed", zap.Error(err))
		return err
	}
	mgr.conn = conn
	mgr.state = Running

	closeChan := make(chan *amqp.Error)
	mgr.conn.NotifyClose(closeChan)
	mgr.connWg.Add(1)
	go func() {
		defer mgr.connWg.Done()
		mgr.logger.Info("connection close handler started")
		defer mgr.logger.Info("connection close handler finished")
		mgr.closeHandler(closeChan)
	}()

	mgr.logger.Info("connection established")
	return nil
}

// Attempts to reconnect to the amqp server. Unlike the connect function,
// reconnect retries connection untill it is canceled by the Close function.
// See Connect for more details.
func (mgr *ConnectionManager) reconnect() error {
	mgr.logger.Info("attempting to reconnect")
	retr := retrier.NewRetrier(-1, retrier.CappedExponentialDelay(25, 2, 10000))

	mgr.connMtx.Lock()
	defer mgr.connMtx.Unlock()

	err := retr.RunCtx(
		mgr.retrCtx,
		func(ctx context.Context) error {
			mgr.logger.Info("dialing", zap.String("url", mgr.url))
			conn, err := amqp.Dial(mgr.url)
			if err != nil {
				mgr.logger.Warn("dialing failed", zap.Error(err))
				return err
			}

			mgr.conn = conn
			return nil
		},
	)
	if err != nil {
		if err.Error() == "context canceled" {
			mgr.logger.Info("reconnecting canceled")
			return nil
		} else {
			mgr.logger.Error("failed to reconnect", zap.Error(err))
			return err
		}
	}

	closeChan := make(chan *amqp.Error)
	mgr.conn.NotifyClose(closeChan)
	mgr.connWg.Add(1)
	go func() {
		mgr.logger.Info("connection close handler started")
		defer mgr.logger.Info("connection close handler finished")
		defer mgr.connWg.Done()
		mgr.closeHandler(closeChan)
	}()

	mgr.logger.Info("reconnected successfully")
	return nil
}

// Close handler listens to a close notification from the amqp server.
// If the connection suffered an error and the manager is not closed, a
// reconnect will be attempted. If the connection was closed without error,
// the handler just terminates.
func (mgr *ConnectionManager) closeHandler(
	clCh chan *amqp.Error,
) {
	err, active := <-clCh
	if err != nil {
		if mgr.state != Closed {
			mgr.logger.Warn(
				"connection closed unexpectedly",
				zap.String("state", mgr.state.String()),
				zap.Error(err),
			)
			if err := mgr.reconnect(); err != nil {
				mgr.logger.Error("failed to reconnect", zap.Error(err))
				panic(fmt.Errorf("failed to reconnect: %w", err))
			}
		} else {
			mgr.logger.Warn("this can not happen", zap.Error(err))
		}
	} else if !active {
		mgr.logger.Info("connection closed without error")
	} else {
		mgr.logger.Warn("no error received")
	}
}

// Closes the amqp connection and the manager's resources.
// If the manager was already closed, the function simply quits.
// During closure, the manager is set into Closing state. The resources are
// requested to close and the manager waits for them to terminate. During
// this time, the connection is maintained to let resources close gracefullly.
// Once all resources closed, the connection is closed, and if the
// manager was currently reconnecting, the reconnection is canceled.
// The function waits for the connection to stop, then sets the manager into
// Closed state before exiting the function.
func (mgr *ConnectionManager) Close() {
	mgr.logger.Info("attempting to close manager")
	mgr.closeMtx.Lock()
	defer mgr.closeMtx.Unlock()

	if mgr.state != Closed {
		mgr.logger.Info("closing manager")
		mgr.state = Closing

		closeWg := &sync.WaitGroup{}
		closeWg.Add(len(mgr.consumers) + len(mgr.publishers))
		for _, ch := range mgr.consumers {
			func() {
				defer closeWg.Done()
				ch.close()
			}()
		}
		for _, ch := range mgr.publishers {
			func() {
				defer closeWg.Done()
				ch.close()
			}()
		}
		closeWg.Wait()

		mgr.conn.Close()
		mgr.retrCtxCncl()
		mgr.connWg.Wait()
		mgr.state = Closed
		mgr.logger.Info("manager closed")
	} else {
		mgr.logger.Warn("manager already closed")
	}
}

// Creates a refresh function that contexts can use to refresh their channel.
// The channel building keeps retrying untill it succeeds or it's canceled.
// While the connection is down, the reconnection pauses.
func (mgr *ConnectionManager) createRefresh(
	bldr ChannelBuilder,
	lgr *zap.Logger,
) RefreshFn {
	return func(retrCtx context.Context) (
		*amqp.Channel,
		error,
	) {
		lgr.Info("running refresh function")

		var err error
		var newChannel *amqp.Channel
		ret := retrier.NewRetrier(-1, retrier.ConstantDelay(1000))

		err = ret.RunCtx(
			retrCtx,
			func(ctx context.Context) error {
				lgr.Info("attempting to build channel")

				// Stops refresh from running while reconnecting
				asyncLock := make(chan bool)
				go func() {
					mgr.connMtx.RLock()
					close(asyncLock)
				}()

				select {
				case <-retrCtx.Done():
					return errors.New("canceled refresh function")
				case <-asyncLock:
					defer mgr.connMtx.RUnlock()

					lgr.Info("building channel")
					newChannel, err = bldr.Build(mgr.conn)
					if err != nil {
						mgr.logger.Warn("failed to build channel", zap.Error(err))
						return err
					}
					return nil
				}
			},
		)
		if err != nil {
			if err.Error() == "context canceled" {
				lgr.Info("canceled refresh function")
			} else {
				lgr.Error("failed to refresh channel", zap.Error(err))
			}

			return nil, err
		}
		return newChannel, err
	}
}

// Creates a new consumer context to read deliveries from the amqp server.
// The consumer is given a unique id and it's placed into a map for reference.
func (mgr *ConnectionManager) CreateConsumer(
	bldr ChannelBuilder,
	opt ConsumerOptions,
) (*ConsumerContext, error) {
	mgr.logger.Info("attempting to create consumer context")

	mgr.closeMtx.Lock()
	defer mgr.closeMtx.Unlock()
	mgr.connMtx.RLock()
	defer mgr.connMtx.RUnlock()

	if mgr.state != Running {
		err := NewConnectionClosedError()
		mgr.logger.Error("failed to create consumer", zap.Error(err))
		return nil, err
	}

	id := uuid.NewV4().String()
	lgrw := mgr.logger.With(
		zap.String("id", id),
		zap.String("queue", opt.Queue),
		zap.String("consumer", opt.Consumer),
	)

	lgrw.Info("creating consumer context")
	chnl, err := bldr.Build(mgr.conn)
	if err != nil {
		mgr.logger.Error("failed to build channel", zap.Error(err))
		return nil, err
	}

	refresh := mgr.createRefresh(bldr, lgrw)
	cnsmr, err := newConsumerContext(id, opt, chnl, refresh, lgrw)
	if err != nil {
		lgrw.Error("failed to create consumer context", zap.Error(err))
		return nil, err
	}

	mgr.consumers[opt.Consumer] = cnsmr
	lgrw.Info("consumer context created")
	return cnsmr, nil
}

// Creates a new publisher context to send publishings to the amqp server.
// The consumer is given a unique id and it's placed into a map for reference.
func (mgr *ConnectionManager) CreatePublisher(
	bldr ChannelBuilder,
	opt PublisherOptions,
) (*PublisherContext, error) {
	mgr.logger.Info("attempting to create publisher context")

	mgr.closeMtx.Lock()
	defer mgr.closeMtx.Unlock()
	mgr.connMtx.RLock()
	defer mgr.connMtx.RUnlock()

	if mgr.state != Running {
		err := NewConnectionClosedError()
		mgr.logger.Error("failed to create publisher", zap.Error(err))
		return nil, err
	}

	id := uuid.NewV4().String()
	lgrw := mgr.logger.With(
		zap.String("id", id),
		zap.String("publisher", opt.Publisher),
	)

	lgrw.Info("creating publisher context")
	chnl, err := bldr.Build(mgr.conn)
	if err != nil {
		mgr.logger.Error("failed to build channel", zap.Error(err))
		return nil, err
	}

	refresh := mgr.createRefresh(bldr, lgrw)
	cnsmr, err := newPublisherContext(id, opt, chnl, nil, nil, refresh, lgrw)
	if err != nil {
		lgrw.Error("failed to create publisher context", zap.Error(err))
		return nil, err
	}

	mgr.publishers[opt.Publisher] = cnsmr
	lgrw.Info("publisher context created")
	return cnsmr, nil
}

// Quick send that does no checks, just opens a connection, creates a
// channel and sends a publishing with the given parameters.
func Send(
	url string,
	exchange string,
	routingKey string,
	publishing amqp.Publishing,
) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(
		context.TODO(),
		exchange,
		routingKey,
		false,
		false,
		publishing,
	)
	if err != nil {
		return err
	}
	return nil
}
