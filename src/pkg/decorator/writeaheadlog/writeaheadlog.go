package decorator

import (
	"io"
	"strings"
	"time"
	"fmt"
	_log "log"
	"streamplay/pipe"
	"streamplay/event"
	"os"
	"path"
	"encoding/binary"
)

const name = "WriteAheadLog"
const (
	DATA        uint8 = 'D'
	CHECKPOINT  uint8 = 'C'
	TOPHEAD     uint8 = 'H'
	MAX_FILES   = 1024
	BUFFER_SIZE = 1024
	MAX_SPREAD  = 1000
	// 10 Seconds -> Nanoseconds
	TICKER = 1e9 * 10
)

// Define some logging constants
var log = _log.New(os.Stderr, "[WAL] ", _log.Ldate|_log.Ltime|_log.Lshortfile)
var NetworkOrder = binary.BigEndian

// This is GLOBALLY changed
var instantiated = false

type CommitLog struct {
	file      *os.File
	position  int64 // Checkpoint offset
	dataTotal int64 // Last bit of Data Offset
	dataTop   int64 // Top of the File Offset
}

type Header struct {
	Header uint8
	Length int64
}

type StateEnum int

const (
	CLOSED StateEnum = iota
	OPENED
	DISCONNECTED
	REPLAYING
)

var stateMap map[StateEnum]string = map[StateEnum]string{DISCONNECTED: "Disconnected",
	OPENED: "Opened", CLOSED: "Closed", REPLAYING: "Replaying"}


type WriteAheadLog struct {
	pipe         *pipe.Pipe // The reference Pipe 
	path         string
	outProcessor chan<- *pipe.Chunk
	checkpoint   chan *int64
	prefix       string       //WalPrefix
	commitLogs   []*CommitLog // Keep track of the set of interesting commitLogs
	curCommitLog CommitLog    // This refers to the current CommitLog
	isReady      chan bool
	discOffset   int64    // After disconnected
	toDelete     []string // More bookkeeping
	State        StateEnum
	stateMutex   sync.Mutex
	ticker       *time.Ticker
}

func NewDataHeader(length int64) *Header {
	return &Header{Header: DATA, Length: length}
}

func NewTopHeader(length int64) *Header {
	return &Header{Header: TOPHEAD, Length: length}
}

func NewCheckHeader(length int64) *Header {
	return &Header{Header: CHECKPOINT, Length: length}
}

func NewWriteAheadLog(pipe *pipe.Pipe, config map[string]string) *WriteAheadLog {
	log.Printf("Creating a new WriteAheadLog for pipe '%s'\n", pipe.Name)
	// TODO: can you assign nil to a string
	if config["path"] == "" {
		log.Fatal("Path necessary to configure WriteAheadLog")
	}

	if instantiated {
		log.Fatal("You can only use a single WriteAheadLog")
	}

	instantiated = true
	return &WriteAheadLog{pipe: pipe, path: config["path"], checkpoint: make(chan *int64),
		isReady: make(chan bool, 1), discOffset: -1, State: CLOSED}
}

func (cl *CommitLog) String() string {
	return fmt.Sprintf("dataTop=%d,dataTotal=%d,position=%d", cl.dataTop, cl.dataTotal, cl.position)
}

// Open the new commitlog file with a brand new handle
func (cl *CommitLog) Dup() *CommitLog {
	new_file, err := os.Open(cl.file.Name())
	if err != nil {
		log.Panicf("Unable to dup file handle '%s'\n", err)
	}
	return &CommitLog{file: new_file, position: cl.position, dataTotal: cl.dataTotal, dataTop: cl.dataTop}
}

func (cl *CommitLog) PositionInFile(position int64) bool {
	if position > fp.dataTop && position < fp.dataTotal {
		// It is in a previous commitlog, the current one
		// wouldn't be in this list
		return true
	}
	return false
}

func (wal *WriteAheadLog) IsOpen() bool {
	return wal.State != CLOSED
}

func (wal *WriteAheadLog) GetReady() chan bool {
	return wal.isReady
}

func (wal *WriteAheadLog) SetSourceStream(out chan<- *pipe.Chunk) {
	wal.outProcessor = out
}

func (wal *WriteAheadLog) replayOverFileList(fileMap []*CommitLog) {
	var n int64

	for _, fp := range fileMap {
		// Go back to the beginning
		fmt.Printf("Seeking back to the beginning of %v\n", fp.file.Name())
		fp.file.Seek(0, 0)
		n = 0
		// Special case 0, since it means we didn't find any checkpoints in the file
		if fp.position > 0 {
			var err os.Error
			n, err = wal.seekToData(fp.file, fp.position)
			if err != nil {
				log.Fatalf("Something went wrong parsing the file, failing %s\n", err)
			}
		}
		n = wal.processDataOnly(fp.file, n)
		if n == 0 {
			wal.toDelete = append(wal.toDelete, fp.file.Name())
		}
		if fp.position >= wal.curCommitLog.position {
			wal.curCommitLog = *fp
			fmt.Printf("Setting curCommitLog=%s\n", wal.curCommitLog)
		} else {
			fmt.Printf("This shouldn't be happening, everything is contiguous, %d > %d\n", wal.curCommitLog.position, fp.position)
		}
		wal.curCommitLog.dataTotal = fp.dataTotal
		fmt.Printf("Setting Current CommitLog dataTotal=%d\n",
			wal.curCommitLog.dataTotal)
	}
}

// Knowing the current commitlog and make sure that we only have relevant ones
// remake the array with only the new ones
func (wal *WriteAheadLog) filterRelevantLogs(files []*CommitLog, filePos int64) []*CommitLog {
	newFileMap := make([]*CommitLog, 0)

	for _, fp := range files {
		fmt.Printf("file %s has dataTotal %d and filePos is %d\n", fp.file.Name(), fp.dataTotal, filePos)
		if filePos > fp.dataTotal {
			// Checkpoint was after the last possible bit on this
			// file, therefore don't keep it
		} else {
			newFileMap = append(newFileMap, fp)
		}
	}
	return newFileMap
}

// Kick off the replay, this will only read off 
func (wal *WriteAheadLog) Replay() {
	fileMap := make([]*CommitLog, 0)

	// Open the file for read only
	fi, err := os.Open(wal.path)
	if err != nil {
		log.Fatalf("Directory '%s' does not exit, reconfigure the WAL to a directory that exists\n", wal.path)
	}

	files, err := fi.Readdirnames(MAX_FILES)
	if err != nil {
		log.Fatalf("Failed reading dirnames '%s'\n", wal.path)
	}

	for _, file := range files {
		if strings.HasSuffix(file, "log") && strings.HasPrefix(file, "wal-") {
			// starts with wal-*log
			pth := path.Join(wal.path, file)

			rdr, err := os.OpenFile(pth, os.O_RDWR, 0666)
			if err != nil {
				log.Fatalf("Unable to open file '%s' %s\n", pth, err)
			}

			_filePos, _totalData, _topData := wal.findLastCheckpoint(rdr)
			log.Printf("Iteration results pos=%d,totalData=%d,topData=%d\n", _filePos, _totalData, _topData)

			// Only do noits with room left past the checkpoint
			// And data in them
			if _filePos >= 0 && (_filePos < _totalData) {
				fileMap = wal.filterRelevantLogs(fileMap, _filePos)
				fileMap = append(fileMap, &CommitLog{file: rdr,
					position: _filePos, dataTotal: _totalData,
					dataTop: _topData})
			} else {
				err := rdr.Close()
				if err != nil {
					log.Fatalf("Unable to close file! %s\n", pth)
				}

				// Don't delete stuff that isn't completely full
				if _filePos == _totalData {
					wal.removeCommitLogs(fileMap)
					fileMap = make([]*CommitLog, 0)
				}
				wal.toDelete = append(wal.toDelete, pth)
				log.Printf("queueing file %s for deletion\n", pth)
			}
		}
	}

	fmt.Printf("Files to delete, before replayOverFileList, %d\n", len(wal.toDelete))
	wal.replayOverFileList(fileMap)
	wal.commitLogs = fileMap
	fmt.Printf("Files to delete, after %d\n", len(wal.toDelete))
	wal.removeStaleFiles()

	// Signal that the module is done loading
	wal.isReady <- true
	fmt.Printf("Potentially open commitlogs, %d\n", len(fileMap))
}

// Remove all CommitLog markers, this usually happens when the position
// is clearly in the current file.
func (wal *WriteAheadLog) removeCommitLogs(files []*CommitLog) {
	for _, fp := range files {
		err := fp.file.Close()
		if err != nil {
			log.Fatalf("Unable to close file! %s\n", fp.file.Name())
		}
		wal.toDelete = append(wal.toDelete, fp.file.Name())
		log.Printf("queueing file %s for deletion\n", fp.file.Name())
	}
}

// Remove a list of files
func (wal *WriteAheadLog) removeStaleFiles() {
	// Actually remove the files
	for _, file := range wal.toDelete {
		log.Printf("Removing file commit log '%s'\n", file)
		err := os.Remove(file)
		if err != nil {
			log.Printf("Unable to delete %s, %s\n", file, err)
		}
	}

	// Clear the list
	wal.toDelete = make([]string, 0)
}

// Read a whole data segment and send to the inbound bytes
func (wal *WriteAheadLog) readDataPart(file *os.File, size int64) int64 {
	var _buf [BUFFER_SIZE]uint8
	var chunk int64
	left := size
	var nRead int64

	// Iterate over chunks
	for {
		if left > BUFFER_SIZE {
			chunk = BUFFER_SIZE
		} else {
			chunk = left
		}
		n, err := file.Read(_buf[:chunk])
		fmt.Printf("file read, n=%v, err=%v\n", n, err)
		if err == os.EOF {
			break
		}
		left -= int64(n)
		if err != nil {
			log.Fatalf("Failed to read %s\n", err)
		}
		// Replay the Inbound likes its a new bits
		wal.outProcessor <- pipe.NewReplayChunk(_buf[:n])
		nRead += int64(n)
	}
	return nRead
}

// Seek to position in the data segment
// returns the amount of data left to read in the partial segment 
func (wal *WriteAheadLog) seekToData(file *os.File, position int64) (ret int64, err os.Error) {
	var hdr Header
	dataLeft := position

	for {
		err := binary.Read(file, NetworkOrder, &hdr)
		if err == os.EOF {
			return 0, err
		} else if err != nil {
			log.Fatalf("Error occured seeking to data portion, %s\n", err)
			return 0, err
		}

		// Skip over checkpoint segments
		switch hdr.Header {
		case DATA:
			if dataLeft <= hdr.Length {
				_, err := file.Seek(dataLeft, 1)
				if err != nil {
					log.Fatalf("Failed seeking over last data segment, %s\n", err)
					return 0, err
				}
				return hdr.Length - dataLeft, nil
			} else {
				_, err := file.Seek(hdr.Length, 1)
				if err != nil {
					log.Fatalf("Failed seeking over data segment, %s\n", err)
					return 0, err
				}
				dataLeft -= hdr.Length
			}
		case CHECKPOINT:
		case TOPHEAD:
			// Start from offset the 
			dataLeft -= hdr.Length
		default:
			log.Fatalf("Wrong header code found, '%c'\n", hdr.Header)
		}
	}
	return 0, nil
}

// Take all the data and process it
func (wal *WriteAheadLog) processDataOnly(file *os.File, inSegment int64) int64 {
	var hdr Header
	var n int64

	if inSegment > 0 {
		n += wal.readDataPart(file, inSegment)
	}

	for {
		err := binary.Read(file, NetworkOrder, &hdr)
		if err == os.EOF {
			break
		} else if err == io.ErrUnexpectedEOF {
			log.Fatalf("Failed reading binary segment processing data, %s\n", err)

		} else if err != nil {
			log.Fatalf("Unknown error occured processing data, %s\n", err)
		}

		// Skip over data segments
		switch hdr.Header {
		case DATA:
			n += wal.readDataPart(file, hdr.Length)
			// Seek relative to the current position
		case CHECKPOINT:
		case TOPHEAD:
		default:
			log.Fatalf("Wrong header code found processing data, '%c'\n", hdr.Header)
		}
	}
	return n
}

// read from the StopCommitLog if its present
// if its not present (-1) then find the position
func (wal *WriteAheadLog) findLastCheckpoint(file *os.File) (lastChk int64, totalData int64, topData int64) {
	var hdr Header

	lastChk = -1
	totalData = 0
	topData = 0

	for {
		err := binary.Read(file, NetworkOrder, &hdr)
		if err == os.EOF {
			break
		} else if err != nil {
			log.Fatalf("Unknown error occured finding last checkpoint, %s\n", err)
		}

		// Skip over data segments
		switch hdr.Header {
		case DATA:
			if lastChk == -1 {
				lastChk = 0
			}
			fmt.Printf("Data header length %d\n", hdr.Length)
			// Seek relative to the current position
			_, err := file.Seek(hdr.Length, 1)
			if err != nil {
				log.Fatalf("Error occured seeking over the data portion, %s\n", err)
			}
			totalData += hdr.Length
		case CHECKPOINT:
			lastChk = hdr.Length
			fmt.Printf("Checkpoint found %d\n", hdr.Length)
		case TOPHEAD:
			topData = hdr.Length
			totalData += hdr.Length
			fmt.Printf("Top header start %d\n", hdr.Length)
		default:
			log.Fatalf("Wrong header code found finding last checkpoint, '%c'\n", hdr.Header)
		}
	}
	return lastChk, totalData, topData
}

func (wal *WriteAheadLog) Open() {
	log.Printf("Opening '%s' log files\n", wal.GetName())
	if wal.IsOpen() {
		return
	}
	// Set to replaying
	wal.SetState(REPLAYING)

	wal.openFile()
	if wal.ticker == nil {
		wal.ticker = time.NewTicker(TICKER)
		go func() {
			for periodic := range wal.ticker.C {
				wal.Periodic(periodic)
			}
		}()

	}
	// Everything is done processing at this point
	wal.SetState(OPENED)
	log.Println("Successfully opened log files")
}

// Actually do the work to open the commitlog file
func (wal *WriteAheadLog) openFile() {

	wal.prefix = fmt.Sprintf("%d", time.Seconds())
	pth := path.Join(wal.path, fmt.Sprintf("wal-%s.log", wal.prefix))

	dataWriter, err := os.OpenFile(pth, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("Unable to open commitlog file '%s'\n", pth)
	}

	log.Printf("Writing %d data total to header",
		wal.curCommitLog.dataTotal)

	// Write the header to start
	err = binary.Write(dataWriter, NetworkOrder, NewTopHeader(wal.curCommitLog.dataTotal))
	if err != nil {
		log.Fatalf("Unable to write commitlog header '%s' '%s'\n", err, pth)
	}

	wal.curCommitLog.file = dataWriter
}

// Get the name of the Decorator
func (wal *WriteAheadLog) GetName() string {
	return name
}

// Inbound is part of the interface.  When the data is coming
// in we make sure that the file is open before trying to write.
// First thing to do is write to the commitlog with appropriate header.
// then if write fails blow up 
func (wal *WriteAheadLog) Inbound(chk *pipe.Chunk) {
	if !wal.IsOpen() {
		wal.Open()
	}
	amount := int64(chk.Len())
	err := binary.Write(wal.curCommitLog.file, NetworkOrder, NewDataHeader(amount))
	if err != nil {
		log.Fatalf("Failed to write data header! %v\n", err)
	}

	_, err = wal.curCommitLog.file.Write(chk.Bytes.Bytes())
	if err != nil {
		log.Fatalln("Failed to write data! %s\n", err)
	}
	wal.curCommitLog.dataTotal += amount

	// Only send data that passes the test
	chk.RunInDecos = wal.IsInQueueFull()
}

func (wal *WriteAheadLog) Outbound(e *event.Event) {
	if !wal.IsOpen() {
		wal.Open()
	}
	fmt.Printf("Wal position %d e.Position %d\n", wal.curCommitLog.position, e.Position)
	offset := e.Position + wal.curCommitLog.position

	err := binary.Write(wal.curCommitLog.file, NetworkOrder, NewCheckHeader(offset))
	if err != nil {
		log.Fatalf("Failed to write checkpoint header! %v\n", err)
	}
	// Set only after a success write
	wal.curCommitLog.position = offset
	// Check the other way
	wal.IsOutQueueFull()
}

func (wal *WriteAheadLog) SetState(state StateEnum) {
	if wal.State != state {
		log.Printf("State has changed %s -> %s\n", stateMap[wal.State],
			stateMap[state])
		if state == DISCONNECTED {
			// Transitioning to a DISCONNECTED state
			log.Println("Setting to disconnected mode")
			wal.State = state
			wal.setDisconnected()
		} else if state == OPENED && wal.State == DISCONNECTED {
			log.Println("Replaying from position", wal.discOffset)
			wal.State = REPLAYING
			wal.setReplayFromDisconnected()
		} else if state == REPLAYING && wal.State == CLOSED {
			// Check if we must replay files
			wal.Replay()
		} else if state == OPENED && wal.State == REPLAYING {

		}
	}
}

// This function only gets called if the state is changed into a 
// disconnected state.  If it is it's equivalent to * -> discon...
func (wal *WriteAheadLog) setDisconnected() {
	wal.discOffset = wal.curCommitLog.dataTotal
}

// Optimized method to handle just moving forward from the current file to the
// current place in line
func (wal *WriteAheadLog) setReplayFromDisconnected() {
	var processPos []*CommitLog

	for i, fp := range wal.commitLogs {
		if fp.PositionInFile(wal.discOffset) {
			processPos = wal.commitLogs[i:len(wal.commitLogs)]
		}
	}
	if processPos != nil {
		log.Printf("Found the last saved location in a previous commit log\n")
		processPos = make([]*CommitLog, 0, 1)
	}
	// Add them all to the list of go through
	processPos = append(processPos, wal.curCommitLog.Dup())

	go func() {
	}()
}

func (wal *WriteAheadLog) Periodic(period int64) {
	log.Println("Periodic, updating state in decorator")
}

// Track the size of the gap between the data and checkpoint
// if it gets too big go into disconnected mode and mark when
// you did that
func (wal *WriteAheadLog) IsInQueueFull() bool {
	if wal.State == DISCONNECTED {
		return false
	}

	// Calculate the offset
	spread := wal.curCommitLog.dataTotal - wal.curCommitLog.position
	if spread >= MAX_SPREAD {
		log.Printf("Queue size overrun, spread=%d\n", spread)
		wal.SetState(DISCONNECTED)
		return false
	}
	return true
}

// Track the size of the gap between the data and checkpoint
// if it gets smaller than run-replay, and 
// you did that
func (wal *WriteAheadLog) IsOutQueueFull() bool {
	if wal.State == OPENED {
		return true
	}

	// Calculate the offset
	spread := wal.curCommitLog.dataTotal - wal.curCommitLog.position
	if spread < MAX_SPREAD {
		wal.SetState(OPENED)
		return false
	}
	return true
}
