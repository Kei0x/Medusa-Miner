package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"math/bits"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// HashVersion is the current hash version.
var HashVersion = []byte{0, 0, 0, 1}

// Regex for ensuring valid comments. 
var BahlaouiRegex = regexp.MustCompile(`^[\x20-\x5B\x5D-\x7E]{1,64}$`)

// Data types
type wsPacket struct {
	Event          event  `json:"e"`
	SequenceNumber uint64 `json:"s"`
}
type event struct {
	Type string      `json:"t"`
	Data interface{} `json:"d"`
}

func (e *event) UnmarshalJSON(b []byte) error {
	var eMap map[string]*json.RawMessage
	err := json.Unmarshal(b, &eMap)
	if err != nil {
		return err
	}

	var dMap *json.RawMessage
	for key, val := range eMap {
		if key == "t" {
			err = json.Unmarshal(*val, &e.Type)
			if err != nil {
				return err
			}
		}
		if key == "d" {
			if val != nil {
				err = json.Unmarshal(*val, &dMap)
				if err != nil {
					return err
				}
			}
		}
	}
	switch e.Type {
	case "NB":
		b := block{}
		err = json.Unmarshal(*dMap, &b)
		if err != nil {
			return err
		}
		e.Data = b
	case "UD":
		var d uint32
		json.Unmarshal(*dMap, &d)
		if err != nil {
			return err
		}
		e.Data = d
	}
	return nil
}

type indexResponse struct {
	HighestBlock block           `json:"highest_block"`
	Parameters   indexParameters `json:"parameters"`
}

type indexParameters struct {
	MinimumDifficulty     int     `json:"minimum_difficulty"`
	CurrentDifficulty     uint32  `json:"current_difficulty"`
	BlockTimeout          float64 `json:"block_timeout"`
	RecalculateDifficulty int     `json:"recalculate_difficulty"`
}

type block struct {
	Height      uint      `json:"height"`
	Bits        uint32    `json:"bits"`
	Nonce       uint32    `json:"nonce"`
	Comment     string    `json:"comment"`
	Hash        string    `json:"hash"`
	SubmittedAt time.Time `json:"submitted_at,omitempty"`

	// Computed fields
	HashBytes []byte `json:"-"`
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flags := pflag.NewFlagSet("medusa", pflag.ExitOnError)
	configFile := flags.StringP("config-file", "c", "", "Path to configuration file.")
	flags.Bool("debug", false, "Enable debug mode.")
	viper.BindPFlag("debug", flags.Lookup("debug"))
	flags.Parse(os.Args)

	// Configuration defaults.
	viper.SetDefault("debug", true)
	cores := runtime.NumCPU() / 2
	if cores < 1 {
		cores = 1
	}
	viper.SetDefault("miner.threads", cores)

	// Enable debug mode
	if viper.GetBool("debug") {
		log.SetLevel(log.DebugLevel)
	}

	// Load configuration file
	viper.SetConfigType("toml")
	file, err := os.Open(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open configuration file (%s) for reading: %s", *configFile, err.Error())
		os.Exit(1)
		return
	}
	err = viper.ReadConfig(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse configuration file (%s): %s", *configFile, err.Error())
		os.Exit(1)
		return
	}
	file.Close()

	// Ensure required configuration variables are set
	if viper.GetString("chain.url") == "" {
		log.Fatal("Configuration: chain.url is required.")
	}
	if viper.GetString("chain.submissionURL") == "" {
		log.Fatal("Configuration: chain.submissionURL is required.")
	}
	if viper.GetString("chain.wsURL") == "" {
		log.Fatal("Configuration: chain.wsURL is required.")
	}
	if viper.GetInt("miner.threads") < 1 || viper.GetInt("miner.threads") > runtime.NumCPU() {
		log.Fatal("Configuration: miner.threads must be greater than 0 and less than NUMCPU.")
	}
	if !BahlaouiRegex.MatchString(viper.GetString("miner.comment")) {
		log.Fatal("Configuration: miner.comment is invalid.")
	}

	// Set default HTTP client
	http.DefaultClient = &http.Client{
		Timeout: time.Second * 30,
	}
}

func main() {
	// Current chain information
	var bits uint32
	var topBlock block

	// Connect to chain over websocket
	onConnect := make(chan int, 100)
	newBits := make(chan uint32, 100)
	newBlocks := make(chan block, 100)
	go websocketLoop(onConnect, newBits, newBlocks)
	<-onConnect
	close(onConnect)

	// Start mining
	var leadingData = generateLeadingData(topBlock.HashBytes, bits)
	var mineMutex = sync.Mutex{}
	var currentTopCovered uint32
	var resets = []chan int{}
	var comment = []byte(viper.GetString("miner.comment"))
	var gap = uint32(100000000 / viper.GetInt("miner.threads")) // 100 mil / threads
	for i := 0; i < (64 - len(comment)); i++ {
		comment = append(comment, 0)
	}

	// Wait for new updates and reset miners if necessary
	go func() {
		for {
			select {
			case b := <-newBlocks:
				log.WithField("len", len(newBlocks)).Info("yeet1")
				if len(newBlocks) == 0 {
					mineMutex.Lock()
					for {
						select {
						case b := <-newBits:
							log.WithField("len", len(newBits)).Info("yeet2")
							if len(newBits) == 0 {
								atomic.SwapUint32(&bits, b)
								log.WithField("bits", b).Info("Got new bits, not resetting miners.")
								break
							}
						default:
						}
						break
					}
					topBlock = b
					hashBytes, err := hex.DecodeString(topBlock.Hash)
					if err != nil {
						log.WithError(err).Warn("failed to hex.DecodeString(topBlock.Hash) in update loop.")
					}
					topBlock.HashBytes = hashBytes
					leadingData = generateLeadingData(topBlock.HashBytes, bits)
					currentTopCovered = 0
					submittingBlock = false
					log.WithField("block.Height", b.Height).Info("Got new block, resetting miners.")
					for _, r := range resets {
						r <- 1
					}
					mineMutex.Unlock()
				}
			}
		}
	}()

	// Launch miners.
	for i := 0; i < viper.GetInt("miner.threads"); i++ {
		j := i
		go func() {
			mineMutex.Lock()
			reset := make(chan int, 100)
			resets = append(resets, reset)
			mineMutex.Unlock()

			// Wait for bits and block
			<-reset

			// Mine
			for {
				mineMutex.Lock()
				from := currentTopCovered
				to := from + gap
				currentTopCovered = to + 1
				mineMutex.Unlock()
				log.WithFields(log.Fields{
					"i":    j,
					"from": from,
					"to":   to,
				}).Info("(re)launching miner")
				nonce, hash := mine(leadingData, bits, comment, from, to, reset)
				if hash != nil {
					// We found the block
					log.Info("Block found, submitting to server.")
					block := block{
						Height:    topBlock.Height + 1,
						Bits:      bits,
						Nonce:     nonce,
						Comment:   viper.GetString("miner.comment"),
						Hash:      hex.EncodeToString(hash),
						HashBytes: hash,
					}
					err := submitBlock(block)
					if err != nil {
						log.WithError(err).Error(
							"Block rejected by server or could not be submitted.")
					}
					<-reset
				}
			}
		}()
	}

	// Listen for interrupts and yeet gracefully.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	log.Info("Exiting..")
	os.Exit(1)
}

// websocket settings.
var wsDialer = websocket.Dialer{
	HandshakeTimeout: time.Second * 30,
}
var websocketLastReset = time.Now()

// websocketLoop restarts websocket on crash. If the websocket crashes twice
// within a minute, the program is yeeted.
func websocketLoop(onConnect chan int, newBits chan uint32, newBlocks chan block) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	conn, _, err := wsDialer.DialContext(ctx, viper.GetString("chain.wsURL"), map[string][]string{})
	if err != nil {
		log.WithError(err).Error("failed to connect to chain over WebSocket")
		failConnect(onConnect, newBits, newBlocks)
		return
	}
	closeHandler := conn.CloseHandler()
	closed := false
	conn.SetCloseHandler(func(code int, text string) error {
		closed = true
		log.WithError(err).Error("WebSocket connection was closed")
		closeHandler(code, text)
		failConnect(onConnect, newBits, newBlocks)
		return nil
	})
	var packet wsPacket
	var s uint64
	for {
		if closed {
			return
		}
		err := conn.ReadJSON(&packet)
		if err != nil {
			log.WithError(err).Error("Failed to ReadJSON from WebSocket connection.")
			conn.CloseHandler()
			return
		}
		if packet.SequenceNumber != s+1 && (s != 0 && packet.SequenceNumber != 0) {
			log.WithError(err).Error("Got incorrectly sequenced packet from WebSocket connection.")
			conn.CloseHandler()
			return
		}
		s = packet.SequenceNumber
		log.WithFields(log.Fields{
			"type": packet.Event.Type,
			"seq":  s,
		}).Debug("WebSocket packet")

		switch packet.Event.Type {
		case "HELLO":
			bits, hBlock, err := getChainParams()
			if err != nil {
				log.WithError(err).Error("Failed to get chain params over HTTP in websocketLoop.")
				conn.CloseHandler()
				return
			}
			newBits <- bits
			newBlocks <- hBlock
			if onConnect != nil {
				onConnect <- 1
				onConnect = nil
			}

		case "NB":
			newBlocks <- packet.Event.Data.(block)
		case "UD":
			newBits <- packet.Event.Data.(uint32)
		}
	}
}

// failConnect checks if the WebSocket should be restarted or if the program
// should yeet.
func failConnect(onConnect chan int, newBits chan uint32, newBlocks chan block) {
	if websocketLastReset.Before(time.Now().Add(-1 * time.Minute)) {
		log.Fatal("WebSocket connection dropped twice in last minute, exiting")
	}
	websocketLastReset = time.Now()
	go websocketLoop(onConnect, newBits, newBlocks)
}

// getChainParams queries chain information by making a HTTP request.
func getChainParams() (uint32, block, error) {
	resp, err := http.Get(viper.GetString("chain.url"))
	if err != nil {
		return 0, block{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, block{}, errors.Errorf(
			"Unexpected status code in params response in main: expected 200, got %v", resp.StatusCode)
	}
	var iResp indexResponse
	decoder := json.NewDecoder(resp.Body)
	decoder.DisallowUnknownFields()
	err = decoder.Decode(&iResp)
	if err != nil {
		return 0, block{}, err
	}
	return iResp.Parameters.CurrentDifficulty, iResp.HighestBlock, nil
}

// generateLeadingData generates leadingData from supplied arguments.
func generateLeadingData(previousHash []byte, bits uint32) []byte {
	output := make([]byte, 68)
	copy(output[0:4], HashVersion)
	copy(output[4:36], previousHash)
	binary.LittleEndian.PutUint32(output[36:68], bits)
	return output
}

// tries to find a nonce that creates a hash that satisifes the chain
// difficulty. From and to specifies the range of nonces to try.
func mine(leadingData []byte, difficulty uint32, comment []byte, from, to uint32, stop chan int) (uint32, []byte) {
	// Check all nonces in range
	nonce := make([]byte, 32)
	var hash1 hash.Hash
	var hash2 hash.Hash
	sum := make([]byte, 32)
	var verification uint64
	for current := from; current <= to; current++ {
		select {
		case <-stop:
			return 0, nil
		default:
		}

		// Write leadingData (version + previous_hash + bits).
		hash1 = sha256.New()
		_, err := hash1.Write(leadingData)
		if err != nil {
			log.WithError(err).Warn("Failed to write leadingData to hash1 in mine.")
			continue
		}

		// Write nonce
		binary.LittleEndian.PutUint32(nonce, current)
		_, err = hash1.Write(nonce)
		if err != nil {
			log.WithError(err).Warn("Failed to write nonce to hash1 in mine.")
			continue
		}

		// Write comment.
		_, err = hash1.Write(comment)
		if err != nil {
			log.WithError(err).Warn("Failed to write comment to hash1 in mine.")
			continue
		}

		// Sum hash twice.
		hash2 = sha256.New()
		hash1Sum := hash1.Sum(nil)
		_, err = hash2.Write(hash1Sum)
		if err != nil {
			log.WithError(err).Warn("failed to write sum of hash1 to hash2 in mine")
			continue
		}
		sum = hash2.Sum(nil)

		// Now verify it.
		verification = binary.BigEndian.Uint64(sum[:8])
		if uint32(bits.LeadingZeros64(verification)) >= difficulty {
			fmt.Println(hex.EncodeToString(sum), hex.EncodeToString(hash1Sum), current, verification, sum[0:8], bits.LeadingZeros64(verification))
			return current, sum
		}
	}
	return 0, nil
}

var submittingBlock = false

// submitBlock submits a block to the server.
func submitBlock(block block) error {
	if submittingBlock {
		return nil
	}
	submittingBlock = true
	buf := new(bytes.Buffer)
	encoder := json.NewEncoder(buf)
	err := encoder.Encode(block)
	if err != nil {
		return err
	}
	resp, err := http.Post(viper.GetString("chain.submissionURL"), "application/json", buf)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("Unexpected status code: expected 200, got %v", resp.StatusCode)
	}
	return nil
}
