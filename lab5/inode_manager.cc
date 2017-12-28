#include "inode_manager.h"
#include <iostream>
#include <fstream>
#include <pthread.h>
#include <string>
using namespace std;

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  /*
   *your lab1 code goes here.
   *if id is smaller than 0 or larger than BLOCK_NUM 
   *or buf is null, just return.
   *put the content of target block into buf.
   *hint: use memcpy
  */
  if (id < 0 || id >= BLOCK_NUM || buf == NULL) {
    printf("Read error,\n");
    return;
  }

  memcpy(buf, blocks[id], BLOCK_SIZE);  
}

void
disk::write_block(blockid_t id, const char *buf)
{
  /*
   *your lab1 code goes here.
   *hint: just like read_block
  */
  if (id < 0 || id >= BLOCK_NUM || buf == NULL) {
    printf("Write error.\n");
    return;
  }

  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your lab1 code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.

   *hint: use macro IBLOCK and BBLOCK.
          use bit operation.
          remind yourself of the layout of disk.
   */
  pthread_mutex_lock(&bitmap_lock);
  char buf[BLOCK_SIZE];
  blockid_t blockid = IBLOCK(sb.ninodes, sb.nblocks);
  while (1) {
    if (blockid >= sb.nblocks) {
      pthread_mutex_unlock(&bitmap_lock);
      return -1;
    }

    read_block(BBLOCK(blockid), buf);

    int off_block = blockid % BPB;    /* offset in a block */
    int pos = off_block / 32;     /* index of uint32 in a block */
    int off = off_block % 32;     /* offset of the uint32 */
    uint32_t *data = (uint32_t *)buf;
    data += pos;
    
    if (((*data) >> off & 1) == 0) {
      *data |= (1 << off); 
      write_block(BBLOCK(blockid), buf);
      using_blocks[blockid] = 1;
      pthread_mutex_unlock(&bitmap_lock);
      return blockid;
    }
    blockid ++;
  }
  
  pthread_mutex_unlock(&bitmap_lock);
  return -1;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your lab1 code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */

  if (id >= sb.nblocks)
    return;
  
  pthread_mutex_lock(&bitmap_lock);
  char* buf =(char *) malloc(BLOCK_SIZE);
  read_block(BBLOCK(id), buf);
  int off_block = id % BPB;
  int pos = off_block / 32;
  int off = off_block % 32;

  uint32_t *data = (uint32_t *)buf;
  data += pos;        /* Get the correct postion */
  uint32_t mask = (1 << off);
  mask = ((uint32_t)0xffffffff ^ mask);
  *data = ((uint32_t)*data & mask);
  write_block(BBLOCK(id), buf);
  free(buf);buf = NULL;
  std::map<uint32_t, int>::iterator iter;
  using_blocks[id] = 0;
  pthread_mutex_unlock(&bitmap_lock);
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
  pthread_mutex_init(&bitmap_lock, NULL);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
  pthread_mutex_init(&inode_lock, NULL);
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your lab1 code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
    
   * if you get some heap memory, do not forget to free it.
   */
  pthread_mutex_lock(&inode_lock); 
  uint32_t inode_id = 1;
  char buf[BLOCK_SIZE];
  while (inode_id <= bm->sb.ninodes) {
    bm->read_block(IBLOCK(inode_id, bm->sb.nblocks), buf);
    for (uint32_t i = 0; i < IPB && i <= bm->sb.ninodes; i++, inode_id++) { 
      inode_t *inode = (inode_t *)(buf + i * sizeof(inode_t));
      if (inode->type == 0) {
        inode->type = type;
        inode->size = 0;
        inode->atime = inode->mtime = inode->ctime = time(NULL);         
        bm->write_block(IBLOCK(inode_id, bm->sb.nblocks), buf);
        pthread_mutex_unlock(&inode_lock);
        return inode_id;
      }
    }
  } 

  pthread_mutex_unlock(&inode_lock);
  return -1;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your lab1 code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   * do not forget to free memory if necessary.
   */
    //printf("im: begin free inode: %d\n", inum);
  inode_t *inode_ptr = get_inode(inum);
    inode_t* ptr = inode_ptr;
    //printf("im: free inode: after get inode\n");
  if (inode_ptr == NULL)
    return;
  
  pthread_mutex_lock(&inode_lock);
  if (inode_ptr->type == 0) {
      //printf("im: before free inode type0\n");
      free(inode_ptr);
      inode_ptr = NULL;
      //printf("im: after free inode type0\n");
      pthread_mutex_unlock(&inode_lock);
    return;
  }
  inode_ptr->type = 0;
  int block_num = (inode_ptr->size == 0) ? 0 : (inode_ptr->size - 1) / BLOCK_SIZE + 1;
  //printf("im: free1 before\n");
  for (int i = 0; i < block_num && i < (int)NDIRECT; i++)
    bm->free_block(inode_ptr->blocks[i]);
  //printf("im: free5 before\n");
  if (block_num > NDIRECT) {
    //printf("im free9 before\n");
    char* buf_tmp = (char *)malloc(BLOCK_SIZE);
    //printf("im: free6 before\n");
    bm->read_block(inode_ptr->blocks[NDIRECT], buf_tmp);
    uint block_a[NINDIRECT];
    //printf("im: free7 before\n");
    memcpy(block_a, buf_tmp, sizeof(block_a));
    //printf("im: free 10 before\n");
    bm->free_block(inode_ptr->blocks[NINDIRECT]);
    //printf("im: free8 before\n");
      free(buf_tmp);buf_tmp = NULL;
    for (int i = 0; i < block_num - NDIRECT; i++)
      bm->free_block(block_a[i]);
  }
  //printf("free2: before\n");
  assert(ptr == inode_ptr);
  inode_ptr->ctime = time(NULL);
  inode_ptr->atime = time(NULL);
  inode_ptr->mtime = time(NULL);
  put_inode(inum, inode_ptr);
  //printf("free 3: before\n");
  free(inode_ptr);
  inode_ptr = NULL;
  //printf("free 3: after\n");
  pthread_mutex_unlock(&inode_lock);
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your lab1 code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  inode_t *inode_ptr = get_inode(inum);

  if (inode_ptr == NULL) {
    return;
  }

  int inode_size = inode_ptr->size;
  *size = inode_size;

  if (inode_size == 0)
    return;
  
  if ((*buf_out = (char *)malloc(inode_size)) == NULL)
    return;

  int size_left = inode_size;
  char *buf_ptr = *buf_out;
  int i = 0;

  while (size_left >= BLOCK_SIZE && i < NDIRECT) {
    bm->read_block(inode_ptr->blocks[i], buf_ptr);
    buf_ptr += BLOCK_SIZE;
    i++;
    size_left -= BLOCK_SIZE;
  }

  /* Done */
  if (BLOCK_SIZE == 0)
    goto end;
  
  char buf_tmp[BLOCK_SIZE];
  /* From the NDIRECT */
  if (i < NDIRECT) {
    bm->read_block(inode_ptr->blocks[i], buf_tmp);
    memcpy(buf_ptr, buf_tmp, size_left);
    goto end;
  }

  uint block_a[NINDIRECT];
  bm->read_block(inode_ptr->blocks[NDIRECT], buf_tmp);
  memcpy(block_a, buf_tmp, sizeof(block_a));
  i = 0;

  while (size_left >= BLOCK_SIZE) {
    bm->read_block(block_a[i], buf_ptr);
    buf_ptr += BLOCK_SIZE;
    i++;
    size_left -= BLOCK_SIZE;
  }

  if (size_left > 0) {
    bm->read_block(block_a[i], buf_tmp);
    memcpy(buf_ptr, buf_tmp, size_left);
  }

  end:
    inode_ptr->atime = time(NULL);
    put_inode(inum, inode_ptr);
    free(inode_ptr);
    inode_ptr = NULL;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your lab1 code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode.
   * you should free some blocks if necessary.
   */
  
  if ((unsigned int)size > MAXFILE * BLOCK_SIZE)
    return;

  inode_t *inode_ptr = get_inode(inum);
  int o_size = inode_ptr->size;
  int o_block_num = (o_size == 0) ? 0 : (o_size - 1) / BLOCK_SIZE + 1;
  int n_block_num = (size == 0) ? 0 : (size - 1) / BLOCK_SIZE + 1;
  int diff = o_block_num - n_block_num;

  /* Need to free */
  if (diff > 0) {
    if (o_block_num <= NDIRECT) 
      for (int i = n_block_num; i < o_block_num; i++) 
        bm->free_block(inode_ptr->blocks[i]);
    
    else if (o_block_num > NDIRECT) {
      uint block_a[NDIRECT];
      char buf_tmp[BLOCK_SIZE];
      bm->read_block(inode_ptr->blocks[NDIRECT], buf_tmp);
      memcpy(block_a, buf_tmp, sizeof(block_a));
      
      /* Only free from the unindirect blocks */
      if (n_block_num > NDIRECT)
        for (int i = n_block_num - NDIRECT; i < o_block_num - NDIRECT; i++)
          bm->free_block(block_a[i]);
      
      /* Both free unindirect blocks and corresponding indirect block */
      else {
        for (int i = 0; i < o_block_num - NDIRECT; i++)
          bm->free_block(block_a[i]);

        for (int i = n_block_num; i < NDIRECT; i++)
          bm->free_block(inode_ptr->blocks[i]);
      }
    }
  }

  // Need to alloc
  else if (diff < 0) {
    if (n_block_num <= NDIRECT) {
      for (int i = o_block_num; i < n_block_num; i++)
        if ((inode_ptr->blocks[i] = bm->alloc_block()) < 0) {
          printf("Alloc error.\n");
          return;
        }
    }

    else if (n_block_num > NDIRECT) {
      uint block_a[NINDIRECT];
      
      /* Alloc direct block and indirect block */
      if (o_block_num <= NDIRECT) {
        for (int i = o_block_num; i < NDIRECT + 1; i++)
          if ((inode_ptr->blocks[i] = bm->alloc_block()) < 0) {
            printf("Alloc error.\n");
            return;
          }
      }

      /* Get the indirect blocks from the disk */
      else if (o_block_num > NDIRECT) {
        char buf_tmp[BLOCK_SIZE];
        bm->read_block(inode_ptr->blocks[NDIRECT], buf_tmp);
        memcpy(block_a, buf_tmp, sizeof(block_a));
      }

      for (int i = 0; i < n_block_num - NDIRECT; i++) {
        if ((block_a[i] = bm->alloc_block()) < 0) {
          printf("Alloc error.\n");
          return;
        }
      } 

      bm->write_block(inode_ptr->blocks[NDIRECT], (char *)block_a);
    }
  }

  /* Write data to the inode */
  char *buf_ptr = (char *)buf;
  int l_size = size;
  int i = 0;
  while (l_size >= BLOCK_SIZE && i < NDIRECT) {
    bm->write_block(inode_ptr->blocks[i], buf_ptr);
    buf_ptr += BLOCK_SIZE;
    i++;
    l_size -= BLOCK_SIZE;
  }
  
  /* Done */
  if (l_size == 0)
    goto end;

  char buf_tmp[BLOCK_SIZE];
  bzero(buf_tmp, BLOCK_SIZE);
  
  /* From the NDIRECT */
  if (i < NDIRECT) {
    memcpy(buf_tmp, buf_ptr, l_size);
    bm->write_block(inode_ptr->blocks[i], buf_tmp);
    goto end;
  }

  /* Should write to the INDIRECT BLOCK */
  uint block_a[NINDIRECT];
  bm->read_block(inode_ptr->blocks[NDIRECT], (char *)block_a);
  
  i = 0;
  while (l_size >= BLOCK_SIZE) {
    bm->write_block(block_a[i], buf_ptr);
    i++;
    buf_ptr += BLOCK_SIZE;
    l_size -= BLOCK_SIZE;
  }

  if (l_size > 0) {
    memcpy(buf_tmp, buf_ptr, l_size);
    bm->write_block(block_a[i], buf_tmp);
  }
  
  end:
    inode_ptr->size = size;
    inode_ptr->atime = inode_ptr->ctime = inode_ptr->mtime = time(NULL);
    put_inode(inum, inode_ptr);
    free(inode_ptr);
    inode_ptr = NULL;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your lab1 code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode_t * inode_ptr = get_inode(inum);
  
  if (inode_ptr == NULL)
    return;
  
  a.type = inode_ptr->type;
  a.atime = inode_ptr->atime;
  a.mtime = inode_ptr->mtime;
  a.ctime = inode_ptr->ctime;
  a.size = inode_ptr->size;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your lab1 code goes here
   * note: you need to consider about both the data block and inode of the file
   * do not forget to free memory if necessary.
   */
  free_inode(inum);
}

void inode_manager::store(uint32_t version) {
  stringstream ss;
  ss << version; 
  std::string log_file = ss.str();
  log_file += ".log";
  std::ofstream ofs;
  ofs.open(log_file.c_str(), std::ios::binary | std::ios::out);
  
  uint32_t i;
  char buf[BLOCK_SIZE];
  for (i = 0; i < IBLOCK(bm->sb.ninodes, bm->sb.nblocks); i++) {
    bm->read_block(i, buf);
    ofs.write(buf, BLOCK_SIZE);
  }

  int size = bm->using_blocks.size();
  ofs.write((char *)&size, sizeof(size));
  std::map<uint32_t, int>::iterator iter = bm->using_blocks.begin();
  for (i = 0; i < size; i++) {
    if (iter->second == 0) continue;
      blockid_t block_id = iter->first;
    ofs.write((char *)&block_id, sizeof(blockid_t));
    bm->read_block(iter->first, buf);
    ofs.write(buf, BLOCK_SIZE);
    iter++;
  }

  ofs.close();
}

void inode_manager::restore(uint32_t version) {
  free(bm);
  bm = new block_manager();
    
    uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
    if (root_dir != 1) {
      printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
      exit(0);
    }

  stringstream ss;
  ss << version; 
  std::string log_file = ss.str();
  log_file += ".log";
  std::ifstream ifs;
  ifs.open(log_file.c_str(), std::ios::binary | std::ios::in);
  if (!ifs) {
    printf("The log_file of version %d doesn't exist\n", version);
    return;
  }

  ifs.seekg(0, ios::beg);
  uint32_t i;
  char buf[BLOCK_SIZE];
  for (i = 0; i < IBLOCK(bm->sb.ninodes, bm->sb.nblocks); i++) {
    ifs.read(buf, BLOCK_SIZE);
    bm->write_block(i, buf);
  }

  int size;
  ifs.read((char *)&size, sizeof(int));
  for (i = 0; i < size; i++) {
    blockid_t block_id;
    ifs.read((char *)&block_id, sizeof(blockid_t));
    bm->using_blocks[block_id] = 1;

    ifs.read(buf, BLOCK_SIZE);
    bm->write_block(block_id, buf);
  }

  ifs.close();
}
