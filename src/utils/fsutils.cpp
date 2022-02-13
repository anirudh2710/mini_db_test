#include "utils/fsutils.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>

#include <absl/strings/str_cat.h>

namespace taco {

bool
file_exists(const char *path) {
    struct stat buf;
    if (stat(path, &buf)) {
        return false;
    }
    return true;
}

bool
regular_file_exists(const char *path) {
    struct stat buf;
    if (stat(path, &buf)) {
        return false;
    }
    return S_ISREG(buf.st_mode);
}

bool
dir_exists(const char *path) {
    struct stat buf;
    if (stat(path, &buf)) {
        return false;
    }
    return S_ISDIR(buf.st_mode);
}

bool
dir_empty(const char *path) {
    DIR *dir;

    if (!(dir = opendir(path))) {
        char *errstr = strerror(errno);
        LOG(kFatal, "cannot open directory %s: %s", path, errstr);
    }

    dirent *ent;
    errno = 0;
    bool is_empty = true;
    while ((ent = readdir(dir))) {
        if (errno != 0) {
            char *errstr = strerror(errno);
            LOG(kFatal, "cannot read directory %s: %s", path, errstr);
        }

        // "." or ".."
        if (ent->d_name[0] == '.') {
            if (ent->d_name[1] == '\0' ||
                (ent->d_name[1] == '.' && ent->d_name[2] == '\0')) {
                continue;
            }
        }

        // found some file in the directory
        is_empty = false;
        break;
    }

    closedir(dir);
    return is_empty;
}

void
remove_dir(const char *path) {
    DIR *dir;

    if (!(dir = opendir(path))) {
        char *errstr = strerror(errno);
        LOG(kFatal, "cannot open directory %s: %s", path, errstr);
    }

    int fd = dirfd(dir);
    dirent *ent;
    errno = 0;
    while ((ent = readdir(dir))) {
        if (errno != 0) {
            char *errstr = strerror(errno);
            LOG(kFatal, "cannot read directory %s: %s", path, errstr);
        }

        if (ent->d_name[0] == '.') {
            if (ent->d_name[1] == '\0' ||
                (ent->d_name[1] == '.' && ent->d_name[2] == '\0')) {
                continue;
            }
        }

        if (ent->d_type == DT_DIR) {
            std::string new_path = absl::StrCat(path, "/", ent->d_name);
            remove_dir(new_path.c_str());
        } else {
            if (unlinkat(fd, ent->d_name, 0)) {
                char *errstr = strerror(errno);
                LOG(kFatal, "cannot remove file %s/%s: %s",
                        path, ent->d_name, errstr);
            }
        }
    }
    closedir(dir);

    if (rmdir(path)) {
        char *errstr = strerror(errno);
        LOG(kFatal, "cannot remove directory %s: %s", path, errstr);
    }
}

std::string
mktempfile(std::string prefix) {
    prefix.resize(prefix.length() + 6, 'X');
    int fd = mkstemp(&prefix[0]);
    if (fd == -1) {
        char *errstr = strerror(errno);
        LOG(kFatal, "unable to make temporary file: %s", errstr);
    }
    close(fd);
    return prefix;
}

std::string
mktempdir(std::string prefix) {
    prefix.resize(prefix.length() + 6, 'X');
    char *dname = mkdtemp(&prefix[0]);
    if (!dname) {
        char *errstr = strerror(errno);
        LOG(kFatal, "unable to make temporary directory: %s", errstr);
    }
    return prefix;
}

}   // namespace taco
