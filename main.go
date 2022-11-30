package main

import (
	"crypto/tls"
	"fmt"
	"github.com/gwuhaolin/livego/av"
	"github.com/gwuhaolin/livego/channel"
	"net"
	"path"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/gwuhaolin/livego/configure"
	"github.com/gwuhaolin/livego/protocol/api"
	"github.com/gwuhaolin/livego/protocol/hls"
	"github.com/gwuhaolin/livego/protocol/httpflv"
	"github.com/gwuhaolin/livego/protocol/rtmp"

	log "github.com/sirupsen/logrus"
)

var VERSION = "master"

func startHls() *hls.Server {
	hlsAddr := configure.Config.GetString("hls_addr")
	hlsListen, err := net.Listen("tcp", hlsAddr)
	if err != nil {
		log.Fatal(err)
	}

	hlsServer := hls.NewServer()
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error("HLS server panic: ", r)
			}
		}()
		log.Info("HLS listen On ", hlsAddr)
		hlsServer.Serve(hlsListen)
	}()
	return hlsServer
}

func startRtmp(stream *rtmp.RtmpStream, hlsServer *hls.Server, app configure.Application) {
	rtmpAddr := configure.Config.GetString("rtmp_addr")
	if app.RtmpAddr != "" {
		rtmpAddr = app.RtmpAddr
	}
	isRtmps := configure.Config.GetBool("enable_rtmps")

	var rtmpListen net.Listener
	if isRtmps {
		certPath := configure.Config.GetString("rtmps_cert")
		keyPath := configure.Config.GetString("rtmps_key")
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			log.Fatal(err)
		}

		rtmpListen, err = tls.Listen("tcp", rtmpAddr, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
		if err != nil {
			log.Fatal(err)
		}
	} else {
		var err error
		rtmpListen, err = net.Listen("tcp", rtmpAddr)
		if err != nil {
			log.Fatal(err)
		}
	}

	var rtmpServer *rtmp.Server

	if hlsServer == nil {
		rtmpServer = rtmp.NewRtmpServer(stream, nil)
		log.Info("HLS server disable....")
	} else {
		rtmpServer = rtmp.NewRtmpServer(stream, hlsServer)
		log.Info("HLS server enable....")
	}

	defer func() {
		if r := recover(); r != nil {
			log.Error("RTMP server panic: ", r)
		}
	}()
	if isRtmps {
		log.Info("RTMPS Listen On ", rtmpAddr)
	} else {
		log.Info("RTMP Listen On ", rtmpAddr)
	}
	rtmpServer.Serve(rtmpListen)
}

func startHTTPFlv(stream *rtmp.RtmpStream) *httpflv.Server {
	httpflvAddr := configure.Config.GetString("httpflv_addr")

	flvListen, err := net.Listen("tcp", httpflvAddr)
	if err != nil {
		log.Fatal(err)
	}

	hdlServer := httpflv.NewServer(stream)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error("HTTP-FLV server panic: ", r)
			}
		}()
		log.Info("HTTP-FLV listen On ", httpflvAddr)
		hdlServer.Serve(flvListen)
	}()
	return hdlServer
}

func startAPI(stream *rtmp.RtmpStream) *api.Server {
	apiAddr := configure.Config.GetString("api_addr")
	rtmpAddr := configure.Config.GetString("rtmp_addr")

	if apiAddr != "" {
		opListen, err := net.Listen("tcp", apiAddr)
		if err != nil {
			log.Fatal(err)
		}
		opServer := api.NewServer(stream, rtmpAddr)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error("HTTP-API server panic: ", r)
				}
			}()
			log.Info("HTTP-API listen On ", apiAddr)
			opServer.Serve(opListen)
		}()
		return opServer
	}
	return nil
}

func init() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			filename := path.Base(f.File)
			return fmt.Sprintf("%s()", f.Function), fmt.Sprintf(" %s:%d", filename, f.Line)
		},
	})
}

func main() {
	defer func() {
		if r := recover(); r != nil {

			log.Error("livego panic: ", r)
			log.Info(string(debug.Stack()))
			time.Sleep(1 * time.Second)
		}
	}()

	log.Infof(`
     _     _            ____       
    | |   (_)_   _____ / ___| ___  
    | |   | \ \ / / _ \ |  _ / _ \ 
    | |___| |\ V /  __/ |_| | (_) |
    |_____|_| \_/ \___|\____|\___/ 
        version: %s
	`, VERSION)

	channels := configure.Channels{}
	configure.Config.UnmarshalKey("channel", &channels)
	for _, chanCfg := range channels {
		log.Info("Start channel:", chanCfg.Name)
		_, err := channel.StartChannel(chanCfg.App, chanCfg.Name)
		if err != nil {
			log.Fatalf("unable to start channel '%s': %s", chanCfg.Name, err)
		}
	}

	apps := configure.Applications{}
	configure.Config.UnmarshalKey("server", &apps)
	var hlsServer *hls.Server
	var wg sync.WaitGroup
	for num, app := range apps {
		stream := rtmp.NewRtmpStream()
		if num == 0 {
			if app.Hls {
				hlsServer = startHls()
			}
			if app.Flv {
				startHTTPFlv(stream)
			}
			if app.Api {
				startAPI(stream)
			}

			// Register channel.BeginOutputCallback() function to be implemented in
			// rtmpServer with the channel so that when the channel is ready it can
			// start sending packets to the rtmpServer named input. The begin output
			// callback function will act similar to Server.handleConn and pass
			// the received ReadCloser to s.handler.HandleReader(reader)
			// It may be good to identify a rtmp.Stream as an output only stream so we
			// don't call s.ConnectChannels() again and risk a feedback loop

			callback := func(r av.ReadCloser) error {
				stream.HandleReader(r)
				return nil
			}
			channel.SetBeginOutputCallbacks(callback)

			wg.Add(1)

			go func() {
				defer wg.Done()
				startRtmp(stream, hlsServer, app)
			}()
		} else {
			log.Warningf("starting more than one app is not supported, skipping '%s'", app.Appname)
		}
	}
	channel.SetServerReady(true)
	wg.Wait()
	// TODO: pass a cancel context to startRtmp and anything
	// else that is started then catch SIGTERM and activate the
	// cancel context to signal a graceful shutdown.
}
