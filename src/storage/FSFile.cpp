#include "storage/FSFile.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>

#include "utils/zerobuf.h"
using namespace std;

namespace taco {
string newPath= "";
off_t offset = 0;
int setflag=0;
int fd = -1;
static FSFile *file;
FSFile*
FSFile::Open(const std::string& path, bool o_trunc,
             bool o_direct, bool o_creat, mode_t mode) {

    // Hint: wse open(2) to obtain a file descriptor of the file for read/write.
    // The file should be opened with O_RDWR flag per specification.
    //    // Run "man 2 open" in the shell for details.

    //TODO implement it
    if(!file) file = new FSFile;
    int flag = O_RDWR;
    newPath = path;
	//if(!fileF) fileF = new FSFile;
    if (o_direct) flag =flag|O_DIRECT;

    if(o_creat) flag = flag|O_CREAT;

    if(o_trunc) flag = flag|O_TRUNC;
    setflag = flag;
    fd = open(newPath.c_str(), flag, mode);
    //return fd == -1?nullptr:file;
    	//FSFile* fileF = new FSFile();
    if (fd!= -1){
	    return file;}
    else {
	    return nullptr;}
}

FSFile::~FSFile() {
    //TODO implement it
    close(fd);
    fd = -1;
}

bool
FSFile::Reopen() {
    //TODO implement it
    if(fd == -1) return true;
    return false;
}

void
FSFile::Close() {
    // Hint: use close(2)
    //TODO implement it
    close(fd);
    fd = -1;
}

bool
FSFile::IsOpen() const {
    //TODO implement it
    return (fcntl(fd, F_GETFD) != -1 || errno != EBADF);
}

void
FSFile::Delete() const {
    // Hint: use unlink(2)
    //TODO implement it
    unlink(newPath.c_str());
    
}

void
FSFile::Read(void *buf, size_t count, off_t offset) {
    // Hint: use pread(2)
    //TODO implement it
    pread(fd, buf, ((sizeof(buf)-1)-offset), offset);
}

void
FSFile::Write(const void *buf, size_t count, off_t offset) {
    // Hint: use pwrite(2)
    //TODO implement it
    pwrite(fd, buf, count, offset);
    offset =offset + count;
}

void
FSFile::Allocate(size_t count) {
    // Hint: call fallocate_zerofill_fast() first to see if we can use
    // the faster fallocate(2) to extend the file.
    //
    // If it returns false and errno == EOPNOTSUPP (not supported by the file
    // system), fall back to writing `count' of zeros at the end of the file.
    // You may use g_zerobuf defined in utils/zerobuf.h as a large buffer that
    // is always all 0.
    //
    // If fallocate_zerofill_fast() returns false and errno is not either 0 or
    // EOPNOTSUPP, log a fatal error with strerror(errno) as a substring.

    //TODO implement it
    bool f = fallocate_zerofill_fast(fd,(off_t)Size(),(off_t)count);
    if (!f){fallocate(fd, FALLOC_FL_ZERO_RANGE,(off_t)Size(),count);}
    //if(fallocate_zerofill_fast(fd, offset, count) ==false){
	    //if(errno == EOPNOTSUPP) {
		    //char temp[count+1];
		    //for (int i=0;i<count;i++) temp[i] = '0';
		    //Write(temp, count, offset);
	    //}
	    //else if(errno!=0) cout<<strerror(errno)<<endl;
}    

size_t
FSFile::Size() const noexcept {
    // Hint: you may obtain the file size using stat(2)
    // FSFile::Size() is frequently called to determine the file size, so
    // you might want to cache the result of stat(2) in the file (but then
    // an FSFile::Allocate() call may extend it). You may assume no one may
    // extend or shrink the file externally when the database is running.

    //TODO implement it
    struct stat temp;
    stat(newPath.c_str(), &temp);
    return temp.st_size;
}

void
FSFile::Flush() {
    // Hint: use fsync(2) or fdatasync(2).
    //TODO implement it
    fsync(fd);
}

}   // namespace taco
