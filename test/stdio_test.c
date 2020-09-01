#include <stdio.h>
#include <stdlib.h>

#include "gotcha_stdio.h"

int main() {
  FILE * pFile;
  char buffer[] = { 'x' , 'y' , 'z' };

  init_gotcha_stdio();

  pFile = fopen("myfile.bin", "wb");
  fwrite(buffer , sizeof(char), sizeof(buffer), pFile);
  fclose(pFile);
  return 0;
}
