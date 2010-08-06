// There are a couple of symbols that need to be #defined before
// #including all the headers.

#ifndef _PARAMS_H_
#define _PARAMS_H_

// The FUSE API has been changed a number of times.  So, our code
// needs to define the version of the API that we assume.  As of this
// writing, the most current API version is 26
#define FUSE_USE_VERSION 26


#include <limits.h>
#include <stdio.h>
#include "transport.h"

static const int OBJECTID_SIZE = 44;

typedef struct FS_LIST {
  int fd;
  char objectid[44];
  void *next;
} fd_list;
struct atmos_state {
    FILE *logfile;
    char *rootdir;
    fd_list *head;
    credentials *c;
};

#define ATMOS_DATA ((struct atmos_state *) fuse_get_context()->private_data)

#endif
