// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>


lock_server::lock_server():
  nacquire (0)
{
  cond = PTHREAD_COND_INITIALIZER;
  mutex = PTHREAD_MUTEX_INITIALIZER;
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab4 code goes here
  printf("acquire request from clt %d\n", clt);
  r = nacquire;
  pthread_mutex_lock(&mutex);

  if (lock_map.find(lid) == lock_map.end())
    lock_map[lid] = lock_protocol::LOCKED;

  else {
    while (lock_map[lid] == lock_protocol::LOCKED)
      pthread_cond_wait(&cond, &mutex);

    lock_map[lid] = lock_protocol::LOCKED;
  }

  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab4 code goes here
  printf("acquire release from clt %d\n", clt);
  r = nacquire;
  pthread_mutex_lock(&mutex);
  lock_map[lid] = lock_protocol::UNLOCKED;
  pthread_mutex_unlock(&mutex);
  pthread_cond_broadcast(&cond);
  return ret;
}
