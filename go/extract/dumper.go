package extract

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog/log"
	"simple_etl_aws/common/filehandler"
	"time"
)

type Dumper struct {
	ctx         context.Context
	cancel      context.CancelFunc
	config      DumpConfig
	downloader  Downloader
	fileHandler filehandler.Handler
	items       []map[string]interface{}
	pending     chan map[string]interface{}
	bufferSize  int
	errors      chan error
}

type DumpConfig struct {
	TimeLimitInMilliseconds int
	SizeLimitInMB           int
	// TODO add general timeout
}

func NewDumper(ctx context.Context, config DumpConfig, downloader Downloader, handler filehandler.Handler) *Dumper {
	childCtx, cancel := context.WithCancel(ctx)
	return &Dumper{
		ctx:    childCtx,
		cancel: cancel,
		// TODO change it read from a path
		config:      config,
		downloader:  downloader,
		fileHandler: handler,
		items:       make([]map[string]interface{}, 0),
		bufferSize:  0,
		pending:     make(chan map[string]interface{}),
		errors:      make(chan error),
	}
}

func (d *Dumper) Run() error {
	d.startBuffering()
	d.startDownloading()

	for {
		select {
		case err, ok := <-d.errors:
			if !ok {
				return nil
			}
			return fmt.Errorf("dumper exited with an error:%w", err)
		case <-d.ctx.Done():
			return nil
		}
	}
}

func (d *Dumper) startDownloading() {
	go func() {
		defer close(d.pending)
		err := d.downloader.Download(d.pending)
		if err != nil {
			log.Error().Msgf("issue with downloading, error: %v", err)
			d.errors <- err
		} else {
			log.Info().Msg("finished downloading")
		}
	}()
}

func (d *Dumper) startBuffering() {
	interval := time.Duration(d.config.TimeLimitInMilliseconds) * time.Millisecond
	tick := time.NewTicker(interval)
	go func() {
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				log.Info().Msgf("hit the interval timeout of %d", d.config.TimeLimitInMilliseconds)
				d.dump()
			case <-d.ctx.Done():
				d.dump()
				return
			case item, ok := <-d.pending:
				if !ok {
					log.Info().Msg("downloading exited/finished: dumping remaining items")
					d.dump()
					close(d.errors)
					return
				}

				log.Debug().Msg("Got new item")

				d.items = append(d.items, item)

				if (d.bufferSize) >= d.config.SizeLimitInMB*int(MB) {
					log.Info().Msgf("items size hit the limit of %d MB", d.config.SizeLimitInMB)
					d.dump()
				}
			}
		}
	}()
}

func (d *Dumper) dump() {
	if len(d.items) == 0 {
		log.Info().Msg("empty buffer, discarding")
		return
	}

	data, err := json.Marshal(d.items)
	if err != nil {
		ferr := fmt.Errorf("could dump file, can't marshal data %w", err)
		log.Error().Err(ferr)
		d.errors <- ferr
		return
	}

	fileName := fmt.Sprintf("%d", time.Now().UnixNano()/int64(time.Millisecond))
	log.Info().Msgf("Dumping file: name: %s size - %d bytes", fileName, len(data))

	err = d.fileHandler.Write(data, fileName)
	if err != nil {
		ferr := fmt.Errorf("could dump file, can't write file %w", err)
		log.Error().Err(ferr)
		d.errors <- ferr
	}

	d.items = make([]map[string]interface{}, 0)
	d.bufferSize = 0
}

func (d *Dumper) addItem(item map[string]interface{}) {
	d.items = append(d.items, item)
	jsonBytes, err := json.Marshal(item)
	if err != nil {
		d.errors <- fmt.Errorf("can't add item: failed to marshal item: %w", err)
	}
	d.bufferSize += len(jsonBytes)
}
func (d *Dumper) Stop() {
	d.cancel()
}
