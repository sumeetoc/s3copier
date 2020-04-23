package main

import (
	"context"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"sync"

	"flag"

	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var baseDir = flag.String("baseDir", "", "Directory to copy s3 contents to. (required)")
var bucket = flag.String("bucket", "", "S3 Bucket to copy contents from. (required)")
var concurrency = flag.Int("concurrency", 10, "Number of concurrent connections to use.")
var queueSize = flag.Int("queueSize", 100, "Size of the queue")
var prefix = flag.Bool("prefix", false, "Set `true` if downloading a prefix (required)")

func main() {
	flag.Parse()
	if len(*baseDir) == 0 || len(*bucket) == 0 {
		flag.Usage()
		os.Exit(-1)
	}

	sess, err := session.NewSession()
	if err != nil {
		log.Fatalf("Failed to create a new session. %v", err)
	}

	//fmt.Println(s3Client)

	if prefix == true {
		s3Client := s3.New(sess, &aws.Config{
			DisableRestProtocolURICleaning: aws.Bool(true),
		})
		prefix_a := strings.Split(bucket, "/")
		DownloadPrefix(s3Client, *bucket, *prefix_a[1], *baseDir, *concurrency, *queueSize)
	} else {
		s3Client := s3.New(sess)
		DownloadBucket(s3Client, *bucket, *baseDir, *concurrency, *queueSize)
	}
}

func DownloadPrefix(client *s3.S3, bucket, prefix_a string, baseDir string, concurrency, queueSize int) {
	keysChan := make(chan string, queueSize)
	cpyr := &PrefixCopier{ //calling copier function, copier function returns every objects attributes(md5,etc), the returning attributes are stored in cpyr. Moreover Copyier returns slice of data using Copy()
		client:   client,
		bucket:   bucket,
		prefix_a: prefix_a,
		baseDir:  baseDir,
		bufPool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, 1024*16) //make allocates a type 'byte' of space and keeps it ready to be used, probably through a channel
			},
		}}
	wg := new(sync.WaitGroup)
	statsTracker := NewStatsTracker(1 * time.Second)
	defer statsTracker.Stop()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range keysChan { //Copy function copies a slice to another destination slice
				n, err := cpyr.Copy(key) //each value in the keysChan channel is picked from cpyr (cpyr has the attributes of each object), key is the object name. it Copy's the object from cpyr using key(object name)
				if err != nil {
					log.Printf("Failed to download key %v, due to %v", key, err)
				}
				statsTracker.Inc(n) //writes,calculates,display the slice of data given by cpyr.Copy(key)
			}
		}()
	}

	dc := &DirectoryCreator{baseDir: baseDir, dirsSeen: make(map[string]bool), newDirPermission: 0755}
	req := &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}
	//here it lists all of the bucket objects data! : example
	// 2020/04/23 11:38:19 this is op: %v {
	// 	AcceptRanges: "bytes",
	// 	Body: buffer(0xc000449740),
	// 	ContentLength: 3895650,
	// 	ContentType: "binary/octet-stream",
	// 	ETag: "\"6cb6c7271a498ff8bcd74d2de46d6f54\"",
	// 	LastModified: 2020-04-21 12:44:24 +0000 UTC,
	// 	Metadata: {
	// 	  Md5: "6cb6c7271a498ff8bcd74d2de46d6f54",
	// 	  Privilege: "755"
	// 	}
	//   }

	err := client.ListObjectsV2Pages(req, func(resp *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, content := range resp.Contents {
			key := *content.Key //key has object name
			//print key, example output
			// 2020/04/23 12:05:45 keys used in channel, abcdefgh
			// 2020/04/23 12:05:45 keys used in channel, afdedf

			if err := dc.MkDirIfNeeded(key); err != nil {
				log.Fatalf("Failed to create directory for key %v due to %v", key, err)
			}
			keysChan <- key //keysChan is a channel through which keys are passed
		}
		//print keysChan
		//020/04/23 11:48:40 keysChan: 0xc0000588a0

		return true
	})
	close(keysChan)
	if err != nil {
		log.Printf("Failed to list objects for bucket %v: %v", bucket, err)
	}
	wg.Wait()
}

type PrefixCopier struct {
	client   *s3.S3
	bucket   string
	prefix_a string
	baseDir  string
	bufPool  *sync.Pool
}

func (c *PrefixCopier) Copy(key string) (int64, error) {
	op, err := c.client.GetObjectWithContext(context.Background(), &s3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(c.prefix_a),
	}), func(r *request.Request) {
		r.HTTPRequest.Header.Add("Accept-Encoding", "gzip")
	})
	//print op, it has the attributes of objects
	if err != nil {
		return 0, err
	}
	defer op.Body.Close()

	f, err := os.Create(path.Join(c.baseDir, key))
	if err != nil {
		io.Copy(ioutil.Discard, op.Body)
		return 0, err
	}
	defer f.Close()

	buf := c.bufPool.Get().([]byte)
	n, err := io.CopyBuffer(f, op.Body, buf)
	c.bufPool.Put(buf)
	//print n
	return n, err
}
func DownloadBucket(client *s3.S3, bucket, baseDir string, concurrency, queueSize int) {
	keysChan := make(chan string, queueSize)
	cpyr := &Copier{ //calling copier function, copier function returns every objects attributes(md5,etc), the returning attributes are stored in cpyr. Moreover Copyier returns slice of data using Copy()
		client:  client,
		bucket:  bucket,
		baseDir: baseDir,
		bufPool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, 1024*16) //make allocates a type 'byte' of space and keeps it ready to be used, probably through a channel
			},
		}}
	wg := new(sync.WaitGroup)
	statsTracker := NewStatsTracker(1 * time.Second)
	defer statsTracker.Stop()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range keysChan { //Copy function copies a slice to another destination slice
				n, err := cpyr.Copy(key) //each value in the keysChan channel is picked from cpyr (cpyr has the attributes of each object), key is the object name. it Copy's the object from cpyr using key(object name)
				if err != nil {
					log.Printf("Failed to download key %v, due to %v", key, err)
				}
				statsTracker.Inc(n) //writes,calculates,display the slice of data given by cpyr.Copy(key)
			}
		}()
	}

	dc := &DirectoryCreator{baseDir: baseDir, dirsSeen: make(map[string]bool), newDirPermission: 0755}
	req := &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}
	//here it lists all of the bucket objects data! : example
	// 2020/04/23 11:38:19 this is op: %v {
	// 	AcceptRanges: "bytes",
	// 	Body: buffer(0xc000449740),
	// 	ContentLength: 3895650,
	// 	ContentType: "binary/octet-stream",
	// 	ETag: "\"6cb6c7271a498ff8bcd74d2de46d6f54\"",
	// 	LastModified: 2020-04-21 12:44:24 +0000 UTC,
	// 	Metadata: {
	// 	  Md5: "6cb6c7271a498ff8bcd74d2de46d6f54",
	// 	  Privilege: "755"
	// 	}
	//   }

	err := client.ListObjectsV2Pages(req, func(resp *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, content := range resp.Contents {
			key := *content.Key //key has object name
			//print key, example output
			// 2020/04/23 12:05:45 keys used in channel, abcdefgh
			// 2020/04/23 12:05:45 keys used in channel, afdedf

			if err := dc.MkDirIfNeeded(key); err != nil {
				log.Fatalf("Failed to create directory for key %v due to %v", key, err)
			}
			keysChan <- key //keysChan is a channel through which keys are passed
		}
		//print keysChan
		//020/04/23 11:48:40 keysChan: 0xc0000588a0

		return true
	})
	close(keysChan)
	if err != nil {
		log.Printf("Failed to list objects for bucket %v: %v", bucket, err)
	}
	wg.Wait()
}

type DirectoryCreator struct {
	dirsSeen         map[string]bool
	baseDir          string
	newDirPermission os.FileMode
}

func (dc *DirectoryCreator) MkDirIfNeeded(key string) error {
	if lastIdx := strings.LastIndex(key, "/"); lastIdx != -1 {
		prefix := key[:lastIdx]
		if _, ok := dc.dirsSeen[prefix]; !ok {
			dirPath := path.Join(dc.baseDir, prefix)
			if err := os.MkdirAll(dirPath, dc.newDirPermission); err != nil {
				return err
			}
			dc.dirsSeen[prefix] = true
		}
	}
	return nil
}

type Copier struct {
	client  *s3.S3
	bucket  string
	baseDir string
	bufPool *sync.Pool
}

func (c *Copier) Copy(key string) (int64, error) {
	op, err := c.client.GetObjectWithContext(context.Background(), &s3.GetObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(key)}, func(r *request.Request) {
		r.HTTPRequest.Header.Add("Accept-Encoding", "gzip")
	})
	//print op, it has the attributes of objects
	if err != nil {
		return 0, err
	}
	defer op.Body.Close()

	f, err := os.Create(path.Join(c.baseDir, key))
	if err != nil {
		io.Copy(ioutil.Discard, op.Body)
		return 0, err
	}
	defer f.Close()

	buf := c.bufPool.Get().([]byte)
	n, err := io.CopyBuffer(f, op.Body, buf)
	c.bufPool.Put(buf)
	//print n
	return n, err
}

type StatsTracker struct {
	startTime time.Time
	ticker    *time.Ticker

	count             uint64
	totalBytesWritten int64
}

func NewStatsTracker(logStatDuration time.Duration) *StatsTracker {
	s := &StatsTracker{startTime: time.Now(), ticker: time.NewTicker(logStatDuration), totalBytesWritten: 0}
	go s.start()
	return s
}

func (s *StatsTracker) Inc(writtenBytes int64) {
	atomic.AddInt64(&s.totalBytesWritten, writtenBytes)
	atomic.AddUint64(&s.count, 1)
}

func (s *StatsTracker) Stop() {
	duration := time.Now().Sub(s.startTime)
	log.Printf("Total number of files: %d, Total time taken: %v, Transfer rate %.4f MiB/s", s.count, duration, throughputInMiB(s.totalBytesWritten, duration))
	s.ticker.Stop()
}

func (s *StatsTracker) start() {
	previouslyPrintedTime := s.startTime
	var previouslyWrittenBytes int64
	for currentTime := range s.ticker.C {
		currentCount := atomic.LoadUint64(&s.count)
		if currentCount == 0 {
			continue
		}
		totalBytesWritten := atomic.LoadInt64(&s.totalBytesWritten)
		log.Printf("Copied %v files from s3 in %v (%.4f MiB/s)\n",
			currentCount,
			currentTime.Sub(s.startTime),
			throughputInMiB(totalBytesWritten-previouslyWrittenBytes, currentTime.Sub(previouslyPrintedTime)))
		previouslyPrintedTime = currentTime
		previouslyWrittenBytes = totalBytesWritten
	}
}

func throughputInMiB(bytesWritten int64, duration time.Duration) float64 {
	return float64(bytesWritten) / duration.Seconds() / (1024 * 1024)
}
