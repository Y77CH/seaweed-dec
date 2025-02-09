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
	"io"
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
	readFile     C.hdfsFile // may be nil if the file is empty
	writeFile    C.hdfsFile
	fullFilePath string
	fileSize     int64
	modTime      time.Time
	empty        bool // true if the file is logically empty (size==0)
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

	// Get file information from HDFS.
	fileInfo := C.hdfsGetPathInfo(fs, cPath)
	var size int64
	var mTime time.Time
	var empty bool
	if fileInfo != nil {
		size = int64(fileInfo.mSize)
		mTime = time.Unix(int64(fileInfo.mLastMod), 0)
		C.hdfsFreeFileInfo(fileInfo, 1)
		if size == 0 {
			empty = true
		}
	} else {
		// File does not exist; create an empty file.
		empty = true
		size = 0
		mTime = time.Now()
		// Create an empty file using write mode.
		wf := C.hdfsOpenFile(fs, cPath, C.O_WRONLY|C.O_CREAT, 0, 0, 0)
		if wf == nil {
			glog.Fatalf("Failed to create file %s in HDFS", fullPath)
		}
		// Flush and close the file.
		if C.hdfsFlush(fs, wf) != 0 || C.hdfsHSync(fs, wf) != 0 {
			glog.Fatalf("Failed to flush new file %s in HDFS", fullPath)
		}
		C.hdfsCloseFile(fs, wf)
	}

	// For reading, only open a read handle if the file is not empty.
	var readFile C.hdfsFile = nil
	if !empty {
		readFile = C.hdfsOpenFile(fs, cPath, C.O_RDONLY, 0, 0, 0)
		if readFile == nil {
			glog.Fatalf("Failed to open file %s for reading in HDFS", fullPath)
		}
	}

	// Open file for writing.
	writeFile := C.hdfsOpenFile(fs, cPath, C.O_WRONLY, 0, 0, 0)
	if writeFile == nil {
		glog.Fatalf("Failed to open file %s for writing in HDFS", fullPath)
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
		empty:        empty,
	}
}

func (df *DiskFile) ReadAt(p []byte, off int64) (n int, err error) {
	// If the file is empty, simulate EOF.
	if df.empty {
		return 0, io.EOF
	}
	// Ensure we have a valid read handle.
	if df.readFile == nil {
		cPath := C.CString(df.fullFilePath)
		defer C.free(unsafe.Pointer(cPath))
		df.readFile = C.hdfsOpenFile(df.fs, cPath, C.O_RDONLY, 0, 0, 0)
		if df.readFile == nil {
			glog.Errorf("Failed to flush new file %s in HDFS", df.fullFilePath)
			return 0, fmt.Errorf("failed to open file %s for reading", df.fullFilePath)
		}
	}
	ret := C.hdfsSeek(df.fs, df.readFile, C.tOffset(off))
	if ret != 0 {
		glog.Errorf("hdfsSeek failed at offset %d in ReadAt", off)
		return 0, fmt.Errorf("hdfsSeek failed at offset %d", off)
	}
	nRead := C.hdfsRead(df.fs, df.readFile, unsafe.Pointer(&p[0]), C.tSize(len(p)))
	if nRead < 0 {
		glog.Errorf("hdfsRead failed")
		return int(nRead), fmt.Errorf("hdfsRead failed")
	}
	return int(nRead), nil
}

func (df *DiskFile) WriteAt(p []byte, off int64) (n int, err error) {
	if df.writeFile == nil {
		return 0, os.ErrClosed
	}
	// If we are writing at offset 0 on an empty file, we assume the file pointer is already there.
	if !(off == 0 && df.empty) {
		ret := C.hdfsSeek(df.fs, df.writeFile, C.tOffset(off))
		if ret != 0 {
			glog.Errorf("hdfsSeek failed at offset %d in WriteAt", off)
			return 0, fmt.Errorf("hdfsSeek failed at offset %d", off)
		}
	}
	nWritten := C.hdfsWrite(df.fs, df.writeFile, unsafe.Pointer(&p[0]), C.tSize(len(p)))
	if nWritten < 0 {
		glog.Errorf("hdfsWrite failed")
		return int(nWritten), fmt.Errorf("hdfsWrite failed")
	}
	waterMark := off + int64(nWritten)
	if waterMark > df.fileSize {
		df.fileSize = waterMark
		df.modTime = time.Now()
		// If we have just written data to an empty file, mark it as non-empty and open a read handle.
		if df.empty {
			df.empty = false
			cPath := C.CString(df.fullFilePath)
			defer C.free(unsafe.Pointer(cPath))
			df.readFile = C.hdfsOpenFile(df.fs, cPath, C.O_RDONLY, 0, 0, 0)
			if df.readFile == nil {
				glog.Errorf("failed to open file %s for reading after write", df.fullFilePath)
			}
		}
	}
	return int(nWritten), nil
}

func (df *DiskFile) Write(p []byte) (n int, err error) {
	return df.WriteAt(p, df.fileSize)
}

func (df *DiskFile) Truncate(off int64) error {
	if df.writeFile == nil {
		return os.ErrClosed
	}
	// Use existing HDFS interfaces to implement truncate.
	if off >= df.fileSize {
		// Extend the file by writing zeros.
		gap := off - df.fileSize
		if gap > 0 {
			buf := make([]byte, gap)
			n, err := df.WriteAt(buf, df.fileSize)
			if err != nil {
				glog.Errorf("failed to extend file: %v", err)
				return fmt.Errorf("failed to extend file: %v", err)
			}
			if int64(n) != gap {
				glog.Errorf("failed to extend file, wrote %d bytes instead of %d", n, gap)
				return fmt.Errorf("failed to extend file, wrote %d bytes instead of %d", n, gap)
			}
		}
		df.fileSize = off
		df.modTime = time.Now()
		return nil
	}

	// For shrinking the file, copy the first off bytes to a temporary file.
	tempPath := df.fullFilePath + ".truncating"
	cTempPath := C.CString(tempPath)
	defer C.free(unsafe.Pointer(cTempPath))

	// Ensure the original file is open for reading.
	if df.empty {
		// If the file is empty, nothing to do.
		return nil
	} else if df.readFile == nil {
		cOrigPath := C.CString(df.fullFilePath)
		defer C.free(unsafe.Pointer(cOrigPath))
		df.readFile = C.hdfsOpenFile(df.fs, cOrigPath, C.O_RDONLY, 0, 0, 0)
		if df.readFile == nil {
			glog.Errorf("failed to open original file for reading during truncate")
			return fmt.Errorf("failed to open original file for reading during truncate")
		}
	} else {
		// Seek to the beginning.
		if C.hdfsSeek(df.fs, df.readFile, C.tOffset(0)) != 0 {
			glog.Errorf("failed to seek to beginning during truncate")
			return fmt.Errorf("failed to seek to beginning during truncate")
		}
	}

	// Open temporary file for writing.
	tempFile := C.hdfsOpenFile(df.fs, cTempPath, C.O_WRONLY|C.O_CREAT, 0, 0, 0)
	if tempFile == nil {
		glog.Errorf("failed to open temporary file for writing during truncate")
		return fmt.Errorf("failed to open temporary file for writing during truncate")
	}

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
			glog.Errorf("failed to read from original file during truncate")
			return fmt.Errorf("failed to read from original file during truncate")
		}
		if nRead == 0 {
			break // reached end-of-file unexpectedly
		}
		nWritten := C.hdfsWrite(df.fs, tempFile, unsafe.Pointer(&buffer[0]), C.tSize(nRead))
		if nWritten < 0 || nWritten != nRead {
			C.hdfsCloseFile(df.fs, tempFile)
			glog.Errorf("failed to write to temporary file during truncate")
			return fmt.Errorf("failed to write to temporary file during truncate")
		}
		bytesCopied += int64(nWritten)
	}

	// Flush and close temporary file.
	if C.hdfsFlush(df.fs, tempFile) != 0 {
		glog.Errorf("failed to flush temporary file during truncate")
		return fmt.Errorf("failed to flush temporary file during truncate")
	}
	if C.hdfsHSync(df.fs, tempFile) != 0 {
		glog.Errorf("failed to hsync temporary file during truncate")
		return fmt.Errorf("failed to hsync temporary file during truncate")
	}
	if C.hdfsCloseFile(df.fs, tempFile) != 0 {
		glog.Errorf("failed to close temporary file during truncate")
		return fmt.Errorf("failed to close temporary file during truncate")
	}

	// Close original file handles.
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
		glog.Errorf("failed to delete original file during truncate")
		return fmt.Errorf("failed to delete original file during truncate")
	}

	// Rename temporary file to original name.
	if C.hdfsRename(df.fs, cTempPath, cOrigPath) != 0 {
		glog.Errorf("failed to rename temporary file to original file during truncate")
		return fmt.Errorf("failed to rename temporary file to original file during truncate")
	}

	// Reopen the file for reading and writing.
	cOrigPath2 := C.CString(df.fullFilePath)
	defer C.free(unsafe.Pointer(cOrigPath2))
	df.readFile = C.hdfsOpenFile(df.fs, cOrigPath2, C.O_RDONLY, 0, 0, 0)
	if df.readFile == nil {
		glog.Errorf("failed to reopen file for reading after truncate")
		return fmt.Errorf("failed to reopen file for reading after truncate")
	}
	df.writeFile = C.hdfsOpenFile(df.fs, cOrigPath2, C.O_WRONLY, 0, 0, 0)
	if df.writeFile == nil {
		glog.Errorf("failed to reopen file for writing after truncate")
		return fmt.Errorf("failed to reopen file for writing after truncate")
	}

	df.fileSize = off
	df.modTime = time.Now()
	// If truncated to zero, mark the file as empty.
	if off == 0 {
		df.empty = true
	}
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
			glog.Errorf("failed to close read file")
			err1 = fmt.Errorf("failed to close read file")
		}
		df.readFile = nil
	}
	if df.writeFile != nil {
		ret := C.hdfsCloseFile(df.fs, df.writeFile)
		if ret != 0 {
			glog.Errorf("failed to close write file")
			err1 = fmt.Errorf("failed to close write file")
		}
		df.writeFile = nil
	}
	ret := C.hdfsDisconnect(df.fs)
	if ret != 0 {
		if err1 == nil {
			glog.Errorf("failed to disconnect HDFS")
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
		err = os.ErrClosed
	}
	return df.fileSize, df.modTime, err
}

func (df *DiskFile) Name() string {
	return df.fullFilePath
}

func (df *DiskFile) Sync() error {
	if df.writeFile == nil {
		return os.ErrClosed
	}
	if isMac {
		return nil
	}
	return df.FileSync()
}

// FileSync wraps the hdfs flush operations.
func (df *DiskFile) FileSync() error {
	ret := C.hdfsFlush(df.fs, df.writeFile)
	if ret != 0 {
		glog.Errorf("hdfsFlush failed")
		return fmt.Errorf("hdfsFlush failed")
	}
	ret = C.hdfsHSync(df.fs, df.writeFile)
	if ret != 0 {
		glog.Errorf("hdfsHSync failed")
		return fmt.Errorf("hdfsHSync failed")
	}
	return nil
}
