package bittrex

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/OpinionatedGeek/go-cloudflare-scraper"
	"github.com/OpinionatedGeek/signalr"
)

type OrderUpdate struct {
	Orderb
	Type int
}

type Fill struct {
	Orderb
	OrderType string
	Timestamp jTime
}

// ExchangeState contains fills and order book updates for a market.
type ExchangeState struct {
	MarketName string
	Nounce     int
	Buys       []OrderUpdate
	Sells      []OrderUpdate
	Fills      []Fill
	Initial    bool
}

// doAsyncTimeout runs f in a different goroutine
//	if f returns before timeout elapses, doAsyncTimeout returns the result of f().
//	otherwise it returns "operation timeout" error, and calls tmFunc after f returns.
func doAsyncTimeout(f func() error, tmFunc func(error), timeout time.Duration) error {
	errs := make(chan error)
	go func() {
		err := f()
		select {
		case errs <- err:
		default:
			if tmFunc != nil {
				tmFunc(err)
			}
		}
	}()
	select {
	case err := <-errs:
		return err
	case <-time.After(timeout):
		return errors.New("operation timeout")
	}
}

func sendStateAsync(dataCh chan ExchangeState, st ExchangeState) {
	select {
	case dataCh <- st:
	default:
	}
}

func subForMarket(client *signalr.Client, market string) (json.RawMessage, error) {
	_, err := client.CallHub(WS_HUB, "SubscribeToExchangeDeltas", market)
	if err != nil {
		return json.RawMessage{}, err
	}
	return client.CallHub(WS_HUB, "QueryExchangeState", market)
}

func parseStates(messages []json.RawMessage, dataCh chan ExchangeState, market string) {
	for _, msg := range messages {
		var st ExchangeState
		if err := json.Unmarshal(msg, &st); err != nil {
			continue
		}
		if st.MarketName != market {
			continue
		}
		sendStateAsync(dataCh, st)
	}
}

// SubscribeExchangeUpdateExt subscribes for updates of the market.
// Updates will be sent to dataCh.
// To stop subscription, send to, or close 'stop'.
func (b *Bittrex) SubscribeExchangeUpdateExt(market string) (dataCh chan ExchangeState, done chan bool, errCh chan error) {
	dataCh = make(chan ExchangeState, 16)
	done = make(chan bool)
	errCh = make(chan error, 1)

	go func() {
		err := b.SubscribeExchangeUpdate(market, dataCh, done)
		if err != nil {
			errCh <- err
		}
	}()
	return
}

// SubscribeExchangeUpdate subscribes for updates of the market.
// Updates will be sent to dataCh.
// To stop subscription, send to, or close 'stop'.
func (b *Bittrex) SubscribeExchangeUpdate(market string, dataCh chan ExchangeState, stop chan bool) error {
	transport := http.DefaultTransport
	if b.client.httpClient != nil && b.client.httpClient.Transport != nil {
		transport = b.client.httpClient.Transport
	}

	scraper, transportErr := scraper.NewTransport(transport)
	if transportErr != nil {
		return transportErr
	}

	c := http.Client{Transport: scraper, Jar: scraper.CookieJar()}

	const timeout = 10 * time.Second
	client := signalr.NewWebsocketClient()
	client.OnClientMethod = func(hub string, method string, messages []json.RawMessage) {
		if hub != WS_HUB || method != "updateExchangeState" {
			return
		}
		parseStates(messages, dataCh, market)
	}
	err := doAsyncTimeout(func() error {
		return client.ConnectWithClient("https", WS_BASE, []string{WS_HUB}, &c)
	}, func(err error) {
		if err == nil {
			client.Close()
		}
	}, timeout)
	if err != nil {
		return errors.Wrap(err, "Error attempting ws async connect")
	}
	defer client.Close()
	var msg json.RawMessage
	err = doAsyncTimeout(func() error {
		var err error
		msg, err = subForMarket(client, market)
		return errors.Wrap(err, "Error subscribing to market events")
	}, nil, timeout)
	if err != nil {
		return errors.Wrap(err, "Performing market subscriptoin async task")
	}
	var st ExchangeState
	if err = json.Unmarshal(msg, &st); err != nil {
		return errors.Wrap(err, "Error unmarshalling response")
	}
	st.Initial = true
	st.MarketName = market
	sendStateAsync(dataCh, st)
	select {
	case <-stop:
	case <-client.DisconnectedChannel:
		stop <- false
	}
	return nil
}
