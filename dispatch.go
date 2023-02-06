package angora

import amqp "github.com/rabbitmq/amqp091-go"

type Dispatch struct {
	refId string // Pending reference id
	seqNo uint64 // sequence number

	retries   int        // Number of retries
	returned  bool       // Dispatch was returned
	confirmCh chan error // Notify of confirmation

	exchange   string          // Exchange name
	routingKey string          // Routing key
	mandatory  bool            // Mandatory flag
	immediate  bool            // Immediate flag
	Publishing amqp.Publishing // Publishing data

	Params map[string]any // Additional details for hooks
}

func (d *Dispatch) GetRetries() int {
	return d.retries
}

func (d *Dispatch) IsReturned() int {
	return d.retries
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
	d.returned = false
	d.seqNo = 0
	d.refId = ""
}
