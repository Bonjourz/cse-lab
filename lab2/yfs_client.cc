// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <string>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <fstream>
#include <time.h>
using namespace std;

#define UINT_MAX_LEN 10
#define INUM_MAX_LEN 20

yfs_client::yfs_client()
{
    ec = new extent_client();

}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string yfs_client::i2s(yfs_client::inum inum) {
    std::stringstream ss;
    ss << inum;
    std::string str = ss.str();
    while(str.length() != INUM_MAX_LEN) {
        str = "0" + str;
    }

    return str;
}

int yfs_client::s2n(std::string n) {
    std::istringstream ist(n);
    unsigned int num;
    ist >> num;
    return num;
}

std::string yfs_client::n2s(uint32_t num) {
    std::stringstream ss;
    ss << num;
    std::string str = ss.str();
    while (str.length() != UINT_MAX_LEN) {
        str = "0" + str;
    }

    return str;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}


bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } else if (a.type == extent_protocol::T_DIR) {
        printf("isfile: %lld is a dir\n", inum);
        return false;
    }
    else {
        printf("isfile: %lld is a symlink\n", inum);
        return false;
    }
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 *
 * */

bool
yfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return false;
    } else if (a.type == extent_protocol::T_DIR) {
        printf("isfile: %lld is a dir\n", inum);
        return true;
    }
    else {
        printf("isfile: %lld is a symlink\n", inum);
        return false;
    }
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getsym(inum inum, syminfo &fin)
{
    int r = OK;

    printf("getsymlink %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getsymlink %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;
    printf("Set attributes\n");
    /*
     * your lab2 code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    if (isdir(ino)) {
        printf("Error in setting the size of a directory\n");
        return IOERR;
    }

    std::string buf;
    if (ec->get(ino, buf) != extent_protocol::OK) {
        printf("Error in setting attributes of files\n");
        return IOERR;
    }

    buf.resize(size);

    if (ec->put(ino, buf) != extent_protocol::OK) {
        printf("Error in setting attributes of files\n");
        return IOERR;
    }
    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    printf("Create\n");
    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    if (!isdir(parent)) {
        printf("%s is not a directory\n", name);
        return IOERR;
    }

    bool found = false;
    inum ino;
    lookup(parent, name, found, ino);
    if (found) {
        printf("The file named %s has existed\n", name);
        return IOERR;
    }

    /* Alloc inode */
    if (ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK) {
        printf("Error in create\n");
        return IOERR;
    }

    /* Update the info of directory */
    std::string buf;
    if (ec->get(parent, buf) != extent_protocol::OK) {
        printf("Error in creating file\n");
        return IOERR;
    }

    std::string name_str = name;
    uint32_t len = name_str.length();
    std::string new_info = n2s(len) + name_str + i2s(ino_out);
    buf += new_info;

    if (ec->put(parent, buf) != extent_protocol::OK) {
        printf("Error in creating file\n");
        return IOERR;
    }

    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    if (!isdir(parent)) {
        printf("%s is not a directory\n", name);
        return IOERR;
    }

    /* Determined whether the name exitsts */
    bool found = false;
    inum ino;
    lookup(parent, name, found, ino);
    if (found) {
        printf("The file named %s has existed\n", name);
        return IOERR;
    }

    /* Alloc inode */
    if (ec->create(extent_protocol::T_DIR, ino_out) != extent_protocol::OK) {
        printf("Error in create\n");
        return IOERR;
    }

    /* Update the info of directory */
    std::string buf;
    if (ec->get(parent, buf) != extent_protocol::OK) {
        printf("Error in creating file\n");
        return IOERR;
    }

    std::string name_str = name;
    int len = name_str.length();
    std::string new_info = n2s(len) + name_str + i2s(ino_out);
    buf += new_info;

    if (ec->put(parent, buf) != extent_protocol::OK) {
        printf("Error in creating file\n");
        return IOERR;
    }

    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = NOENT;
    /*
     * your lab2 code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    found = false;

    std::list<dirent> list;
    readdir(parent, list);
    std::string target = name;

    for (std::list<dirent>::iterator it = list.begin(); it != list.end(); it++) {
        if (it->name == target) {
            ino_out = it->inum;
            found = true;
            return OK;
        }
    }

    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;
    /*
     * your lab2 code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    if (!isdir(dir))
        return IOERR;

    std::string buf;
    if (ec->get(dir, buf) != extent_protocol::OK) {
        printf("Error with readdir\n");
        return IOERR;
    }

    int size = buf.length();
    int i = 0;
    while (i < size) {
        uint32_t name_len = s2n(buf.substr(i, UINT_MAX_LEN));
        i += UINT_MAX_LEN;

        dirent ele;
        ele.name = buf.substr(i, name_len);
        i += name_len;
        ele.inum = n2i(buf.substr(i, INUM_MAX_LEN));
        i += INUM_MAX_LEN;
        list.push_back(ele);
    }

    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: read using ec->get().
     */

    std::string buf;
    if (ec->get(ino, buf) != extent_protocol::OK) {
        printf("Error in read\n");
        return IOERR;
    }

    if (buf.length() < off)
        data = "";

    else
        data = buf.substr(off, size);

    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    std::string buf;
    if (ec->get(ino, buf) != extent_protocol::OK) {
        printf("Error in write\n");
        return IOERR;
    }

    bytes_written = (off > buf.length()) ? size + off - buf.length() : size; 

    if (off + size > buf.length()) 
        buf.resize(off + size);

    for (int i = 0; i < size; i++)
        buf[i + off] = data[i];
    

    if (ec->put(ino, buf) != extent_protocol::OK) {
        printf("Error in write\n");
        return IOERR;
    }

    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    list<dirent> dir_list;
    readdir(parent, dir_list);
    std::string name_str = name;
    yfs_client::inum i_num = -1;
    std::string dir_buf = "";

    for (std::list<dirent>::iterator it = dir_list.begin(); it != dir_list.end(); it++) {
        if (it->name == name_str)
            i_num = it->inum;

        else {
            std::string ele_str = n2s(it->name.length()) + it->name + i2s(it->inum);
            dir_buf += ele_str;
        }
    }

    if (isdir(i_num)) {
        printf("Cannot unlink a directory!\n");
    }

    if (i_num == -1) {
        printf("Cannot find the link named %s\n", name);
        return IOERR;
    }

    if (ec->put(parent, dir_buf) != extent_protocol::OK) {
        printf("Error in unlinking\n");
        return IOERR;
    }

    ec->remove(i_num);


    return r;
}

int yfs_client::symlink(inum parent, const char *name, const char *link, inum& ino) {
    int r = OK;

    bool found;
    yfs_client::inum inum;
    if (lookup(parent, name, found, inum) != NOENT) {
        printf("Create symbolic link error: The name has existed\n");
        return EXIST;
    }

    /* Alloc inode for symbolic link */
    ec->create(extent_protocol::T_SYMLINK, ino);
    std::string buf;
    ec->get(parent, buf);

    /* Write info back to parent directory */
    std::string name_str = name;
    buf += (n2s(name_str.length()) + name_str + i2s(ino));
    ec->put(parent, buf);

    /* Write link info to link block */
    std::string link_buf = link;
    ec->put(ino, link_buf);

    return r;

}

int yfs_client::readlink(inum ino, std::string& name) {
    int r = OK;
    if (ec->get(ino, name) != extent_protocol::OK) {
        printf("Error in reading link.\n");
        return IOERR;
    }

    return r;
}

