//
// Created by lukemartinlogan on 10/17/22.
//

#include <fcntl.h>
#include <unistd.h>
#include <cstdio>

int main() {
  printf("HERE?\n");
  int fd = open("hello.txt", O_WRONLY);
  close(fd);
}
