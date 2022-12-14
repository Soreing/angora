package angora

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type exchangeOptions struct {
	name       string
	kind       string
	durable    bool
	autoDelete bool
	internal   bool
	noWait     bool
	args       map[string]interface{}
}

type queueOptions struct {
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	noWait     bool
	args       map[string]interface{}
}

type bindingOptions struct {
	dest   string
	key    string
	source string
	noWait bool
	args   map[string]interface{}
}

type ChannelBuilder struct {
	exchanges map[string]exchangeOptions
	queues    map[string]queueOptions
	qbindings []bindingOptions
	ebindings []bindingOptions
	confirms  bool
}

func NewChannelBuilder() *ChannelBuilder {
	return &ChannelBuilder{
		exchanges: map[string]exchangeOptions{},
		queues:    map[string]queueOptions{},
		qbindings: []bindingOptions{},
		ebindings: []bindingOptions{},
	}
}

func (bldr *ChannelBuilder) WithExchange(
	name string,
	kind string,
	durable bool,
	autoDelete bool,
	internal bool,
	noWait bool,
	args map[string]interface{},
) *ChannelBuilder {
	bldr.exchanges[name] = exchangeOptions{
		name:       name,
		kind:       kind,
		durable:    durable,
		autoDelete: autoDelete,
		internal:   internal,
		noWait:     noWait,
		args:       args,
	}
	return bldr
}

func (bldr *ChannelBuilder) WithQueue(
	name string,
	durable bool,
	autoDelete bool,
	exclusive bool,
	noWait bool,
	args map[string]interface{},
) *ChannelBuilder {
	bldr.queues[name] = queueOptions{
		name:       name,
		durable:    durable,
		autoDelete: autoDelete,
		exclusive:  exclusive,
		noWait:     noWait,
		args:       args,
	}
	return bldr
}

func (bldr *ChannelBuilder) WithQueueBinding(
	name string,
	key string,
	exchange string,
	noWait bool,
	args map[string]interface{},
) *ChannelBuilder {
	bldr.qbindings = append(bldr.qbindings, bindingOptions{
		dest:   name,
		key:    key,
		source: exchange,
		noWait: noWait,
		args:   args,
	})
	return bldr
}

func (bldr *ChannelBuilder) WithExchangeBinding(
	dest string,
	key string,
	source string,
	noWait bool,
	args map[string]interface{},
) *ChannelBuilder {
	bldr.qbindings = append(bldr.qbindings, bindingOptions{
		dest:   dest,
		key:    key,
		source: source,
		noWait: noWait,
		args:   args,
	})
	return bldr
}

func (bldr *ChannelBuilder) WithConfirms(
	confirms bool,
) *ChannelBuilder {
	bldr.confirms = confirms
	return bldr
}

func (bldr *ChannelBuilder) Build(
	conn *amqp.Connection,
) (*amqp.Channel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	for _, ex := range bldr.exchanges {
		err := ch.ExchangeDeclare(
			ex.name,
			ex.kind,
			ex.durable,
			ex.autoDelete,
			ex.internal,
			ex.noWait,
			ex.args,
		)
		if err != nil {
			return nil, err
		}
	}
	for _, q := range bldr.queues {
		_, err := ch.QueueDeclare(
			q.name,
			q.durable,
			q.autoDelete,
			q.exclusive,
			q.noWait,
			q.args,
		)
		if err != nil {
			return nil, err
		}
	}
	for _, qb := range bldr.qbindings {
		err := ch.QueueBind(
			qb.dest,
			qb.key,
			qb.source,
			qb.noWait,
			qb.args,
		)
		if err != nil {
			return nil, err
		}
	}
	for _, eb := range bldr.ebindings {
		err := ch.ExchangeBind(
			eb.dest,
			eb.key,
			eb.source,
			eb.noWait,
			eb.args,
		)
		if err != nil {
			return nil, err
		}
	}
	return ch, nil
}
