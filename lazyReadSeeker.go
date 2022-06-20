package s3fs

import (
	"bytes"
	"io"
	"io/ioutil"
)

// lazyReadSeeker supports on-demand converting an io.Reader into an io.ReadSeeker.
// When up-converting, the original io.Reader is read in full into a copy.
type lazyReadSeeker struct {
	rawReader  io.Reader
	readSeeker io.ReadSeeker
	size       int64
	fakeEOF    bool
}

// newLazyReadSeeker constructs a new lazyReadSeeker using the specified io.Reader.
// As a performance optimization, seeking to the end can be supported
// without up-converting.
func newLazyReadSeeker(reader io.Reader, size int64) *lazyReadSeeker {
	return &lazyReadSeeker{rawReader: reader, size: size}
}

func (r *lazyReadSeeker) Read(p []byte) (n int, err error) {
	// If we already have a real ReadSeeker, use it
	if r.readSeeker != nil {
		return r.readSeeker.Read(p)
	}

	// We're faking a seek, because we don't have a read ReadSeeker.
	// Thus, if the client has seeked to the end, handle that case.
	if r.fakeEOF {
		return 0, io.EOF
	}

	n, err = r.rawReader.Read(p)

	// We need to special case if we didnt read everything.
	// Hitting this case can only happen once since we'll
	// upconvert and then hit the first if statement above
	// from that point forward
	amountRead := int64(n)
	if amountRead > 0 && amountRead < r.size {
		r.upconvert(p[0:n])
		r.Seek(amountRead, io.SeekStart)
	}

	return
}

func (r *lazyReadSeeker) Seek(offset int64, whence int) (int64, error) {
	// If we already have a real ReadSeeker, use it
	if r.readSeeker != nil {
		return r.readSeeker.Seek(offset, whence)
	}

	// Without making a copy, we only support seeking to the beginning or end
	if offset == 0 {
		switch whence {
		case io.SeekStart:
			r.fakeEOF = false
			return 0, nil
		case io.SeekEnd:
			r.fakeEOF = true
			return r.size - 1, nil
		}
	}

	// Unfortunately, it's now time to take the hit and up-convert,
	// so we must read the entire buffer and create a real ReadSeeker.
	r.upconvert(nil)
	return r.readSeeker.Seek(offset, whence)
}

func (r *lazyReadSeeker) upconvert(seed []byte) {
	buffer := bytes.NewBuffer(seed)
	remaining, err := ioutil.ReadAll(r.rawReader)
	if err != nil {
		// Is there a better solution than to panic?
		panic(err)
	}
	buffer.Write(remaining)
	r.readSeeker = bytes.NewReader(buffer.Bytes())
}
