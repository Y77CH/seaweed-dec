package backend

/*
#cgo LDFLAGS: -lhdfs
#cgo CFLAGS: -I/usr/local/hadoop/include
#include <stdlib.h>
#include <hdfs.h>
*/
import "C"
import (
	"fmt"
	"os"
	"runtime"
	"time"
	"unsafe"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

var (
	_ BackendStorageFile = &DiskFile{}
)

const isMac = runtime.GOOS == "darwin"

type DiskFile struct {
	fs           C.hdfsFS
	readFile     C.hdfsFile
	writeFile    C.hdfsFile
	fullFilePath string
	fileSize     int64
	modTime      time.Time
}

func NewDiskFile(f *os.File) *DiskFile {
	// Use the file name from f as the HDFS file path.
	fullPath := f.Name()

	// Connect to HDFS.
	namenode := C.CString("hdfs://localhost:9000")
	defer C.free(unsafe.Pointer(namenode))
	fs := C.hdfsConnect(namenode, 0)
	if fs == nil {
		glog.Fatalf("Failed to connect to HDFS")
	}

	cPath := C.CString(fullPath)
	defer C.free(unsafe.Pointer(cPath))

	// Open file for reading.
	readFile := C.hdfsOpenFile(fs, cPath, C.O_RDONLY, 0, 0, 0)
	// If the file does not exist yet, readFile may be nil.
	// We do not treat this as fatal since the file may be created later.

	// Open file for writing.
	writeFile := C.hdfsOpenFile(fs, cPath, C.O_WRONLY|C.O_CREAT, 0, 0, 0)
	if writeFile == nil {
		glog.Fatalf("Failed to open file %s for writing in HDFS", fullPath)
	}

	// Get file information from HDFS.
	fileInfo := C.hdfsGetPathInfo(fs, cPath)
	var size int64
	var mTime time.Time
	if fileInfo != nil {
		size = int64(fileInfo.mSize)
		mTime = time.Unix(int64(fileInfo.mLastMod), 0)
		C.hdfsFreeFileInfo(fileInfo, 1)
	} else {
		size = 0
		mTime = time.Now()
	}

	// Adjust file size to align with NeedlePaddingSize.
	offset := size
	if offset%NeedlePaddingSize != 0 {
		offset = offset + (NeedlePaddingSize - offset%NeedlePaddingSize)
	}

	return &DiskFile{
		fs:           fs,
		readFile:     readFile,
		writeFile:    writeFile,
		fullFilePath: fullPath,
		fileSize:     offset,
		modTime:      mTime,
	}
}

func (df *DiskFile) ReadAt(p []byte, off int64) (n int, err error) {
	if df.readFile == nil {
		return 0, os.ErrClosed
	}
	ret := C.hdfsSeek(df.fs, df.readFile, C.tOffset(off))
	if ret != 0 {
		return 0, fmt.Errorf("hdfsSeek failed at offset %d", off)
	}
	nRead := C.hdfsRead(df.fs, df.readFile, unsafe.Pointer(&p[0]), C.tSize(len(p)))
	if nRead < 0 {
		return int(nRead), fmt.Errorf("hdfsRead failed")
	}
	return int(nRead), nil
}

func (df *DiskFile) WriteAt(p []byte, off int64) (n int, err error) {
	if df.writeFile == nil {
		return 0, os.ErrClosed
	}
	ret := C.hdfsSeek(df.fs, df.writeFile, C.tOffset(off))
	if ret != 0 {
		return 0, fmt.Errorf("hdfsSeek failed at offset %d", off)
	}
	nWritten := C.hdfsWrite(df.fs, df.writeFile, unsafe.Pointer(&p[0]), C.tSize(len(p)))
	if nWritten < 0 {
		return int(nWritten), fmt.Errorf("hdfsWrite failed")
	}
	waterMark := off + int64(nWritten)
	if waterMark > df.fileSize {
		df.fileSize = waterMark
		df.modTime = time.Now()
	}
	return int(nWritten), nil
}

func (df *DiskFile) Write(p []byte) (n int, err error) {
	return df.WriteAt(p, df.fileSize)
}

func (df *DiskFile) Truncate(off int64) error {
	if df.fs == nil {
		return os.ErrClosed
	}

	// If off is greater than or equal to the current file size, extend the file.
	if off >= df.fileSize {
		gap := off - df.fileSize
		if gap > 0 {
			// Extend by writing zeros.
			buf := make([]byte, gap)
			n, err := df.WriteAt(buf, df.fileSize)
			if err != nil {
				return fmt.Errorf("failed to extend file: %v", err)
			}
			if int64(n) != gap {
				return fmt.Errorf("failed to extend file, wrote %d bytes instead of %d", n, gap)
			}
		}
		df.fileSize = off
		df.modTime = time.Now()
		return nil
	}

	// For shrinking the file (off < df.fileSize), create a temporary file in HDFS.
	tempPath := df.fullFilePath + ".truncating"
	cTempPath := C.CString(tempPath)
	defer C.free(unsafe.Pointer(cTempPath))

	// Ensure the original file is open for reading.
	if df.readFile == nil {
		cOrigPath := C.CString(df.fullFilePath)
		defer C.free(unsafe.Pointer(cOrigPath))
		df.readFile = C.hdfsOpenFile(df.fs, cOrigPath, C.O_RDONLY, 0, 0, 0)
		if df.readFile == nil {
			return fmt.Errorf("failed to open original file for reading during truncate")
		}
	} else {
		// Seek to the beginning of the file.
		if C.hdfsSeek(df.fs, df.readFile, C.tOffset(0)) != 0 {
			return fmt.Errorf("failed to seek to beginning during truncate")
		}
	}

	// Open the temporary file for writing.
	tempFile := C.hdfsOpenFile(df.fs, cTempPath, C.O_WRONLY|C.O_CREAT, 0, 0, 0)
	if tempFile == nil {
		return fmt.Errorf("failed to open temporary file for writing during truncate")
	}

	// Copy the first off bytes from the original file to the temporary file.
	var bytesCopied int64 = 0
	bufSize := 4096
	buffer := make([]byte, bufSize)
	for bytesCopied < off {
		toRead := bufSize
		remaining := off - bytesCopied
		if remaining < int64(bufSize) {
			toRead = int(remaining)
		}
		nRead := C.hdfsRead(df.fs, df.readFile, unsafe.Pointer(&buffer[0]), C.tSize(toRead))
		if nRead < 0 {
			C.hdfsCloseFile(df.fs, tempFile)
			return fmt.Errorf("failed to read from original file during truncate")
		}
		if nRead == 0 {
			break // reached end-of-file unexpectedly
		}
		nWritten := C.hdfsWrite(df.fs, tempFile, unsafe.Pointer(&buffer[0]), C.tSize(nRead))
		if nWritten < 0 || nWritten != nRead {
			C.hdfsCloseFile(df.fs, tempFile)
			return fmt.Errorf("failed to write to temporary file during truncate")
		}
		bytesCopied += int64(nWritten)
	}

	// Flush and close the temporary file.
	if C.hdfsFlush(df.fs, tempFile) != 0 {
		return fmt.Errorf("failed to flush temporary file during truncate")
	}
	if C.hdfsHSync(df.fs, tempFile) != 0 {
		return fmt.Errorf("failed to hsync temporary file during truncate")
	}
	if C.hdfsCloseFile(df.fs, tempFile) != 0 {
		return fmt.Errorf("failed to close temporary file during truncate")
	}

	// Close the original file handles.
	if df.readFile != nil {
		C.hdfsCloseFile(df.fs, df.readFile)
		df.readFile = nil
	}
	if df.writeFile != nil {
		C.hdfsCloseFile(df.fs, df.writeFile)
		df.writeFile = nil
	}

	// Delete the original file.
	cOrigPath := C.CString(df.fullFilePath)
	defer C.free(unsafe.Pointer(cOrigPath))
	if C.hdfsDelete(df.fs, cOrigPath, 0) != 0 {
		return fmt.Errorf("failed to delete original file during truncate")
	}

	// Rename the temporary file to the original file name.
	if C.hdfsRename(df.fs, cTempPath, cOrigPath) != 0 {
		return fmt.Errorf("failed to rename temporary file to original file during truncate")
	}

	// Reopen the file for reading and writing.
	cOrigPath2 := C.CString(df.fullFilePath)
	defer C.free(unsafe.Pointer(cOrigPath2))
	df.readFile = C.hdfsOpenFile(df.fs, cOrigPath2, C.O_RDONLY, 0, 0, 0)
	if df.readFile == nil {
		return fmt.Errorf("failed to reopen file for reading after truncate")
	}
	df.writeFile = C.hdfsOpenFile(df.fs, cOrigPath2, C.O_WRONLY, 0, 0, 0)
	if df.writeFile == nil {
		return fmt.Errorf("failed to reopen file for writing after truncate")
	}

	df.fileSize = off
	df.modTime = time.Now()
	return nil
}

func (df *DiskFile) Close() error {
	if df.writeFile == nil && df.readFile == nil {
		return nil
	}
	err := df.Sync()
	var err1 error
	if df.readFile != nil {
		ret := C.hdfsCloseFile(df.fs, df.readFile)
		if ret != 0 {
			err1 = fmt.Errorf("failed to close read file")
		}
		df.readFile = nil
	}
	if df.writeFile != nil {
		ret := C.hdfsCloseFile(df.fs, df.writeFile)
		if ret != 0 {
			err1 = fmt.Errorf("failed to close write file")
		}
		df.writeFile = nil
	}
	ret := C.hdfsDisconnect(df.fs)
	if ret != 0 {
		if err1 == nil {
			err1 = fmt.Errorf("failed to disconnect HDFS")
		}
	}
	df.fs = nil
	if err != nil {
		return err
	}
	return err1
}

func (df *DiskFile) GetStat() (datSize int64, modTime time.Time, err error) {
	if df.fs == nil {
		return 0, time.Time{}, os.ErrClosed
	}
	return df.fileSize, df.modTime, nil
}

func (df *DiskFile) Name() string {
	return df.fullFilePath
}

func (df *DiskFile) Sync() error {
	if df.writeFile == nil {
		return os.ErrClosed
	}
	ret := C.hdfsFlush(df.fs, df.writeFile)
	if ret != 0 {
		return fmt.Errorf("hdfsFlush failed")
	}
	ret = C.hdfsHSync(df.fs, df.writeFile)
	if ret != 0 {
		return fmt.Errorf("hdfsHSync failed")
	}
	return nil
}
